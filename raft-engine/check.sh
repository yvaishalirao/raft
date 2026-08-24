#!/usr/bin/env bash
set -e
echo "=== vet ===" && go vet ./...
echo "=== isolation ===" && bash scripts/check_isolation.sh
echo "=== majority ===" && bash scripts/check_majority.sh
echo "=== build ===" && go build ./...
echo "=== fmt ===" && test -z "$(gofmt -l .)"
echo "=== tests ===" && go test ./... -count=1
echo "=== ALL CHECKS PASSED ==="
