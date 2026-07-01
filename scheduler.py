"""
scheduler.py — runs the full pipeline on a schedule read from the Google Sheet.

Each active campaign row can have a `report_times` column with comma-separated
24-hour times, e.g.  11:00,14:00,17:00,20:00

Usage (terminal — watchdog mode, auto-restarts on crash):
    python scheduler.py

Usage (Jupyter cell — runs in background, survives kernel busy):
    import scheduler
    scheduler.launch_background()

Usage (Jupyter cell — check if running):
    scheduler.status()

Usage (Jupyter cell — stop):
    scheduler.stop()

The schedule is reloaded from the sheet every midnight, so you can update times
in the sheet without restarting.
"""
import subprocess
import sys
import os


def _ensure_deps():
    try:
        import dotenv, gspread, openpyxl, anthropic, requests, pandas, docx, matplotlib, schedule
    except ImportError:
        print("[bootstrap] Installing missing dependencies ...")
        req = os.path.join(os.path.dirname(__file__), "requirements.txt")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", req, "-q"])
        print("[bootstrap] Done.")

_ensure_deps()

import logging
import time
from datetime import datetime

import schedule
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

# ── logging ────────────────────────────────────────────────────────────────────
_LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

def _make_log_handler():
    return logging.FileHandler(
        os.path.join(_LOG_DIR, f"scheduler_{datetime.now().strftime('%Y-%m-%d')}.log"),
        encoding="utf-8",
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), _make_log_handler()],
)
log = logging.getLogger("scheduler")


# ── pipeline ───────────────────────────────────────────────────────────────────

import threading as _threading
_campaign_locks = {}   # state_name -> Lock — prevents overlapping runs for same campaign


def _run_campaign_thread(raw_row):
    """Wrapper that runs _run_campaign in a background thread so parallel schedules don't block."""
    t = _threading.Thread(target=_run_campaign, args=(raw_row,), daemon=True,
                          name=f"dst-{raw_row.get('state_name','?')}")
    t.start()


def _run_campaign(raw_row):
    from pipeline import config, analyze, cdd_sync, report, notify

    state = raw_row.get("state_name", "?")

    # Per-campaign lock — skip this run if the previous one is still in progress
    lock = _campaign_locks.setdefault(state, _threading.Lock())
    if not lock.acquire(blocking=False):
        log.warning(f"[{state}] previous run still in progress — skipping this trigger")
        return

    try:
        log.info(f"[{state}] pipeline triggered at {datetime.now().strftime('%H:%M')}")
        # Re-fetch row from sheet so any config changes are picked up without restart
        try:
            fresh_rows = config.get_active_rows()
            match = next(
                (r for r in fresh_rows
                 if r.get("state_name", "").strip().lower() == state.strip().lower()),
                None,
            )
            if match:
                raw_row = match
                log.info(f"[{state}] config refreshed from sheet")
            else:
                log.warning(f"[{state}] not found in sheet — using cached config")
        except Exception as e:
            log.warning(f"[{state}] sheet refresh failed (using cached): {e}")

        try:
            cfg = config.build(raw_row)
        except Exception as e:
            log.error(f"[{state}] config build FAILED: {e}", exc_info=True)
            return

        if not cfg["active"]:
            log.info(f"[{state}] inactive — skip"); return
        if not cfg["in_campaign_window"]:
            log.info(f"[{state}] outside campaign window — skip"); return

        analyze.run(cfg)

        try:
            cdd_sync.run(cfg)
        except Exception as e:
            log.error(f"[{state}] cdd_sync FAILED (non-fatal — continuing to report): {e}", exc_info=True)

        docx, partner_docx, slack_text = report.run(cfg)
        notify.run(cfg, docx, slack_text, partner_docx_path=partner_docx)
        log.info(f"[{state}] pipeline complete")
    except Exception as e:
        log.error(f"[{state}] FAILED: {e}", exc_info=True)
    finally:
        lock.release()


# ── schedule builder ───────────────────────────────────────────────────────────

def _reload_schedule():
    from pipeline import config as cfg_module

    try:
        rows = cfg_module.get_active_rows()
    except Exception as e:
        log.error(f"Failed to read Google Sheet — keeping existing schedule: {e}")
        # ensure the hourly reload job survives even if this call came from start()
        if not any(j.job_func.func == _reload_schedule for j in schedule.get_jobs()):
            schedule.every(1).hours.do(_reload_schedule)
        return

    # Only clear and rebuild after a successful Sheet read
    schedule.clear()
    schedule.every(1).hours.do(_reload_schedule)

    job_count = 0
    for row in rows:
        if row.get("active", "").strip().upper() not in ("TRUE", "YES", "1", "Y"):
            continue
        times = [t.strip() for t in str(row.get("report_times", "")).split(",") if t.strip()]
        state = row.get("state_name", "?")
        if not times:
            log.warning(f"[{state}] report_times not set — no jobs scheduled"); continue
        for t in times:
            try:
                schedule.every().day.at(t).do(_run_campaign_thread, raw_row=row)
                log.info(f"  scheduled [{state}] at {t}")
                job_count += 1
            except Exception as e:
                log.warning(f"  [{state}] invalid time '{t}': {e}")

    log.info(f"Schedule loaded: {job_count} job(s) across {len(rows)} campaign(s)")


# ── main loop ──────────────────────────────────────────────────────────────────

def start():
    """Blocking scheduler loop."""
    log.info("=== DST Campaign Scheduler starting ===")
    _reload_schedule()

    next_info_at = 0
    while True:
        schedule.run_pending()
        now = time.time()
        if now >= next_info_at:
            jobs = schedule.get_jobs()
            upcoming = [str(j.next_run) for j in sorted(jobs, key=lambda j: j.next_run)[:3]]
            log.info(f"Scheduler alive — {len(jobs)} job(s). Next: {upcoming}")
            next_info_at = now + 3600
        time.sleep(30)


# ── Jupyter helpers ────────────────────────────────────────────────────────────

_bg_thread = None

def launch_background():
    """Start scheduler in a background thread. Safe to call multiple times."""
    import threading
    global _bg_thread
    if _bg_thread and _bg_thread.is_alive():
        print("Scheduler already running — call scheduler.status() to check.")
        return
    _bg_thread = threading.Thread(target=start, name="dst-scheduler", daemon=True)
    _bg_thread.start()
    print("Scheduler started in background.")

def status():
    """Print current scheduler status."""
    if _bg_thread and _bg_thread.is_alive():
        jobs = schedule.get_jobs()
        print(f"Scheduler: RUNNING  |  {len(jobs)} job(s)")
        for j in sorted(jobs, key=lambda j: j.next_run)[:6]:
            print(f"  next: {j.next_run}")
    else:
        print("Scheduler: NOT RUNNING — call scheduler.launch_background()")

def stop():
    """Clear all scheduled jobs."""
    schedule.clear()
    print("Scheduler stopped (jobs cleared).")


# ── watchdog (terminal / cron on hosted Jupyter server) ───────────────────────

def _watchdog():
    """
    Spawn scheduler as a subprocess and restart it automatically if it crashes.
    Run this from a Jupyter terminal or cron so it survives kernel restarts.
    """
    import subprocess
    script = os.path.abspath(__file__)
    python = sys.executable
    log.info("=== Watchdog started ===")
    while True:
        log.info("Launching scheduler subprocess ...")
        proc = subprocess.Popen([python, script, "--run"])
        proc.wait()
        if proc.returncode == 0:
            log.info("Scheduler exited cleanly — watchdog stopping.")
            break
        log.warning(f"Scheduler crashed (exit {proc.returncode}) — restarting in 30s ...")
        time.sleep(30)


# ── entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if "--run" in sys.argv:
        # actual scheduler loop — spawned by watchdog
        try:
            start()
        except KeyboardInterrupt:
            log.info("Scheduler stopped.")
    else:
        # default: watchdog mode — auto-restarts on crash
        try:
            _watchdog()
        except KeyboardInterrupt:
            log.info("Watchdog stopped.")
