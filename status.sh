#!/bin/bash
# Check if the DST scheduler is running.

cd "$(dirname "$0")"

if [ -f scheduler.pid ] && kill -0 "$(cat scheduler.pid)" 2>/dev/null; then
    echo "Scheduler: RUNNING (PID $(cat scheduler.pid))"
    echo ""
    echo "Last 10 log lines:"
    tail -10 logs/scheduler_bg.log 2>/dev/null
else
    echo "Scheduler: NOT RUNNING"
    echo "Start it with: bash start.sh"
fi
