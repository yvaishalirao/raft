#!/usr/bin/env bash
# Re-run this on the actual demo machine before every rehearsal: a single
# clean kill/restart cycle is not enough evidence the restart path holds up
# under repeated real-world use.
set -euo pipefail
cd "$(dirname "$0")"

COMPOSE="docker compose -f docker-compose.yml"
CYCLES=10
NODES=(node-0 node-1 node-2 node-3 node-4)

wait_for_leader() {
	local exclude="${1:-}"
	local bound="${2:-15}"
	local deadline=$((SECONDS + bound))
	while [ "$SECONDS" -lt "$deadline" ]; do
		local id
		id=$($COMPOSE logs 2>/dev/null | grep "role=Leader" | tail -1 | sed -n 's/.*node=\([a-zA-Z0-9-]*\) role=Leader.*/\1/p')
		if [ -n "$id" ] && [ "$id" != "$exclude" ]; then
			echo "$id"
			return 0
		fi
		sleep 1
	done
	return 1
}

all_running() {
	local running
	running=$($COMPOSE ps --status running --format '{{.Name}}' | wc -l)
	[ "$running" -eq 5 ]
}

echo "starting cluster..."
$COMPOSE up --build -d
sleep 5
wait_for_leader "" 15 >/dev/null || { echo "FAIL: no initial leader" >&2; exit 1; }

for i in $(seq 1 "$CYCLES"); do
	target_id="${NODES[$(((i - 1) % 5))]}"
	target="raft-$target_id"
	echo "=== cycle $i: killing $target ==="

	go run ../cmd/supervisor kill "$target"

	new_leader=$(wait_for_leader "$target_id" 15) || {
		echo "FAIL cycle $i: no new leader after killing $target" >&2
		exit 1
	}
	echo "cycle $i: new leader node=$new_leader"

	go run ../cmd/supervisor restart "$target"

	deadline=$((SECONDS + 15))
	rejoined=false
	while [ "$SECONDS" -lt "$deadline" ]; do
		status=$(docker inspect "$target" --format '{{.State.Status}}')
		if [ "$status" = "running" ]; then
			rejoined=true
			break
		fi
		sleep 1
	done
	if [ "$rejoined" != true ]; then
		echo "FAIL cycle $i: $target did not return to running" >&2
		exit 1
	fi

	deadline=$((SECONDS + 15))
	stable=false
	while [ "$SECONDS" -lt "$deadline" ]; do
		if all_running; then
			stable=true
			break
		fi
		sleep 1
	done
	if [ "$stable" != true ]; then
		echo "FAIL cycle $i: cluster did not return to 5/5 running" >&2
		exit 1
	fi
	echo "cycle $i: converged, all 5 containers running"
done

echo "all $CYCLES cycles converged"
$COMPOSE down
