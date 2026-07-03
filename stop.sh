#!/bin/bash
# Stop the DST scheduler — kills the watchdog AND the spawned `--run` child.
# The watchdog (scheduler.py) forks a child (scheduler.py --run); killing only
# the saved PID would orphan the child, so we kill every scheduler.py process.

cd "$(dirname "$0")"

if pgrep -f "scheduler.py" >/dev/null 2>&1; then
    pkill -f "scheduler.py"
    sleep 1
    # force-kill any stragglers that ignored SIGTERM
    pkill -9 -f "scheduler.py" 2>/dev/null
    rm -f scheduler.pid
    echo "Scheduler stopped (all scheduler.py processes)"
else
    rm -f scheduler.pid
    echo "No scheduler processes running"
fi
