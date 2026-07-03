#!/bin/bash
# Start the DST scheduler as a background process.
# Run from a JupyterHub terminal: bash start.sh

cd "$(dirname "$0")"
mkdir -p logs

# Refuse to start a duplicate — check for ANY running scheduler.py process,
# not just the saved PID (a stale/missing pidfile must not allow a second copy).
if pgrep -f "scheduler.py" >/dev/null 2>&1; then
    echo "Scheduler already running (PID(s): $(pgrep -f scheduler.py | tr '\n' ' '))"
    echo "Run 'bash stop.sh' first for a clean restart."
    exit 0
fi

nohup python scheduler.py > logs/scheduler_bg.log 2>&1 &
echo $! > scheduler.pid
echo "Scheduler started (PID $!)"
echo "Tail logs: tail -f logs/scheduler_bg.log"
