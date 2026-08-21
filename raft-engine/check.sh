#!/usr/bin/env bash
set -e
echo "=== vet ===" && go vet ./...
echo "=== build ===" && go build ./...
echo "=== fmt ===" && test -z "$(gofmt -l .)"
echo "=== tests ===" && go test ./... -count=1
echo "=== ALL CHECKS PASSED ==="
