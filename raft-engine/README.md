# raft-engine

A from-scratch implementation of the Raft consensus algorithm in Go, wrapped in a
toy distributed key-value store, with a real-time web visualizer showing leader
election, log replication, and fault injection (node crashes, network partitions)
as they happen live. The centerpiece demo: kill the leader mid-write and watch a
new leader get elected while the write eventually succeeds.

## Prerequisites

- Go 1.22+
- `protoc` (Protocol Buffers compiler)
- Docker (for the multi-process demo via Docker Compose)

## Quickstart

```
make build
make test
make demo
```

## Scope

This is a v1 educational implementation. It deliberately does not implement log
compaction/snapshotting, dynamic cluster membership changes, pre-vote optimization,
or disk persistence across restarts (a restart comes back as a fresh, empty node).
See [ARCHITECTURE.md](../docs/ARCHITECTURE.md) Section 1 for the full list of
explicit v1 exclusions and the rationale behind them.
