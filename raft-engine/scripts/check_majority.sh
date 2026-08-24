#!/usr/bin/env bash
set -e
if grep -rn 'len(reachable\|len(responded\|len(alive' raft/; then
  echo "BANNED PATTERN: majority must never be computed from a count of reachable/responded/alive peers"
  exit 1
fi
echo "majority computation OK"
