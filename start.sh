#!/bin/bash
# Start the DST scheduler as a background process.
# Run from a JupyterHub terminal: bash start.sh

cd "$(dirname "$0")"
mkdir -p logs

if [ -f scheduler.pid ] && kill -0 "$(cat scheduler.pid)" 2>/dev/null; then
    echo "Scheduler is already running (PID $(cat scheduler.pid))"
    exit 0
fi

nohup python scheduler.py > logs/scheduler_bg.log 2>&1 &
echo $! > scheduler.pid
echo "Scheduler started (PID $!)"
echo "Tail logs: tail -f logs/scheduler_bg.log"
