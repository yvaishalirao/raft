#!/usr/bin/env bash
set -e
DEPS=$(go list -deps ./raft/...)
if echo "$DEPS" | grep -E 'raft-engine/(rpc|kv|viz)'; then
  echo "ISOLATION VIOLATION: raft/ depends on transport/kv/viz"
  exit 1
fi
echo "raft/ isolation OK"
