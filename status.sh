#!/bin/bash
# Check if the DST scheduler is running (detects ALL scheduler.py processes).

cd "$(dirname "$0")"

PIDS=$(pgrep -f "scheduler.py" | tr '\n' ' ')
if [ -n "$PIDS" ]; then
    echo "Scheduler: RUNNING (PID(s): $PIDS)"
    COUNT=$(pgrep -f "scheduler.py" | wc -l)
    if [ "$COUNT" -gt 2 ]; then
        echo "WARNING: $COUNT scheduler.py processes — expected 2 (watchdog + child)."
        echo "Run 'bash stop.sh' then 'bash start.sh' for a clean single instance."
    fi
    echo ""
    echo "Last 10 log lines:"
    tail -10 logs/scheduler_bg.log 2>/dev/null
else
    echo "Scheduler: NOT RUNNING"
    echo "Start it with: bash start.sh"
fi
