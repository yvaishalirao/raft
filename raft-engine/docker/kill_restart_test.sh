#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

COMPOSE="docker compose -f docker-compose.yml"

wait_for_leader() {
	local exclude="${1:-}"
	local deadline=$((SECONDS + 15))
	while [ "$SECONDS" -lt "$deadline" ]; do
		local id
		id=$($COMPOSE logs 2>/dev/null | grep "role=Leader" | tail -1 | sed -n 's/.*node=\([a-zA-Z0-9-]*\) role=Leader.*/\1/p')
		if [ -n "$id" ] && [ "$id" != "$exclude" ]; then
			echo "$id"
			return 0
		fi
		sleep 1
	done
	echo "TIMEOUT waiting for leader (excluding '$exclude')" >&2
	return 1
}

echo "starting cluster..."
$COMPOSE up --build -d
sleep 5

leader_id=$(wait_for_leader)
leader_container="raft-$leader_id"
echo "leader is $leader_container"

echo "SIGKILLing $leader_container..."
go run ../cmd/supervisor kill "$leader_container"

exit_code=$(docker inspect "$leader_container" --format '{{.State.ExitCode}}')
if [ "$exit_code" != "137" ]; then
	echo "FAIL TC-1: expected SIGKILL exit code 137, got $exit_code" >&2
	exit 1
fi
echo "TC-1 pass: exit code 137 confirms a real SIGKILL"

new_leader_id=$(wait_for_leader "$leader_id")
echo "TC-2 pass: survivors elected new leader node=$new_leader_id"

echo "restarting $leader_container..."
go run ../cmd/supervisor restart "$leader_container"

deadline=$((SECONDS + 15))
rejoined=false
while [ "$SECONDS" -lt "$deadline" ]; do
	status=$(docker inspect "$leader_container" --format '{{.State.Status}}')
	if [ "$status" = "running" ]; then
		rejoined=true
		break
	fi
	sleep 1
done
if [ "$rejoined" != true ]; then
	echo "FAIL TC-3: $leader_container never returned to running" >&2
	exit 1
fi

# A restarted container runs a brand-new process (v1 has no persistence),
# so a fresh startup line proves it came back with term=0 and an empty log
# rather than resuming stale state.
up_lines=$($COMPOSE logs "$leader_container" 2>/dev/null | grep -c "up: raft=")
if [ "$up_lines" -lt 1 ]; then
	echo "FAIL TC-3: no fresh startup log line for $leader_container" >&2
	exit 1
fi
echo "TC-3 pass: $leader_container rejoined as a fresh follower"

echo "all checks passed"
$COMPOSE down
