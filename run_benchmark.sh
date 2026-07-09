#!/bin/bash

# Benchmark runner script for the distributed computing system
# This script helps set up and run throughput benchmarks

set -e

echo "🚀 Distributed Computing System - Benchmark Runner"
echo "=================================================="
echo ""

# Check if WebSocket server is running
if ! lsof -Pi :3000 -sTCP:LISTEN -t >/dev/null 2>&1 ; then
    echo "⚠️  Warning: WebSocket server doesn't appear to be running on port 3000"
    echo "   Please start it with: node server/websocket-server.js (from the WASMHive-WebApp repo)"
    echo ""
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Check if workers are available
echo "📋 Before running benchmarks:"
echo "   1. Make sure the WebSocket server is running (port 3000)"
echo "   2. Open worker nodes in browser tabs:"
echo "      - Open worker/index.html from the WASMHive-WebApp repo in one or more browser tabs"
echo "      - Each tab = one worker node"
echo ""
read -p "Are workers ready? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Please set up workers and try again."
    exit 1
fi

# Run benchmark with provided arguments or defaults
echo ""
echo "🏃 Running benchmark..."
echo ""

if [ $# -eq 0 ]; then
    cargo run --bin benchmark
else
    cargo run --bin benchmark -- "$@"
fi

echo ""
echo "✅ Benchmark complete!"
echo "   Check benchmark_results.json for detailed results"

