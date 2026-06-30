#!/bin/bash
# Stop the DST scheduler.

cd "$(dirname "$0")"

if [ ! -f scheduler.pid ]; then
    echo "No scheduler.pid found — scheduler may not be running"
    exit 0
fi

PID=$(cat scheduler.pid)
if kill -0 "$PID" 2>/dev/null; then
    kill "$PID"
    rm scheduler.pid
    echo "Scheduler stopped (PID $PID)"
else
    rm scheduler.pid
    echo "Scheduler was not running (stale PID $PID removed)"
fi
