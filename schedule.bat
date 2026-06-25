@echo off
cd /d D:\DST\automation
:loop
python scheduler.py >> logs\scheduler_bat.log 2>&1
echo Scheduler exited at %date% %time% — restarting in 30s >> logs\scheduler_bat.log
timeout /t 30 /nobreak > nul
goto loop
