#!/usr/bin/env bash
set -e

# Configuration
LOCAL_KIND_CLUSTER="${LOCAL_KIND_CLUSTER:-unstructured-data-controller-local}"

echo "Cleaning up local development environment..."

# Stop Docling if it was started by setup script
if [ -f /tmp/docling.pid ]; then
    DOCLING_PID=$(cat /tmp/docling.pid)
    if ps -p "$DOCLING_PID" > /dev/null 2>&1; then
        kill "$DOCLING_PID" 2>/dev/null || true
    fi
    rm -f /tmp/docling.pid /tmp/docling.log
fi

# Delete Kind cluster
kind delete cluster --name "${LOCAL_KIND_CLUSTER}" 2>/dev/null || true
echo "✓ Local development environment removed"
