"""
run.py — Daily campaign report orchestrator
Usage: python run.py
Reads all active rows from the Google Sheet, runs the full pipeline per row.
"""
import logging
import os
import sys
from datetime import date

from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

# ── logging ────────────────────────────────────────────────────────────────────
_log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(_log_dir, exist_ok=True)
_log_file = os.path.join(_log_dir, f"{date.today().isoformat()}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(_log_file, encoding="utf-8"),
        logging.StreamHandler(open(sys.stdout.fileno(), mode="w", encoding="utf-8", closefd=False)),
    ],
)
log = logging.getLogger(__name__)

import config
import analyze
import cdd_sync
import report
import notify


def _update_sheet_status(cfg, status, step_failed="", error_msg="", drive_link=""):
    """Append a row to the Run Log tab in the config Google Sheet."""
    from datetime import datetime
    import gspread
    from google.oauth2.service_account import Credentials

    try:
        creds_path = os.getenv("GOOGLE_CREDENTIALS_PATH")
        sheet_id   = os.getenv("GOOGLE_SHEET_ID")
        if not creds_path or not sheet_id:
            return
        creds = Credentials.from_service_account_file(
            creds_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"],
        )
        ws  = gspread.Client(auth=creds).open_by_key(sheet_id).worksheet("Run Log")
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        row = [
            now,
            cfg.get("state_name", ""),
            cfg.get("campaign_name", ""),
            cfg.get("DAY", ""),
            datetime.now().strftime("%H:%M"),
            status,
            step_failed,
            str(error_msg)[:300] if error_msg else "",
            drive_link or "",
        ]
        ws.append_row(row, value_input_option="USER_ENTERED")
        log.info(f"[{cfg.get('state_name')}] Run Log updated: {status}")
    except Exception as e:
        log.warning(f"Run Log update failed (non-fatal): {e}")


def _slack_error(cfg_or_channel, state, step, error):
    """Post a failure alert to Slack so the team knows immediately."""
    import requests as _req
    token   = os.getenv("SLACK_TOKEN")
    channel = (cfg_or_channel.get("slack_channel") if isinstance(cfg_or_channel, dict)
               else cfg_or_channel) or os.getenv("SLACK_CHANNEL", "")
    if not token or not channel:
        return
    try:
        _req.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}"},
            json={"channel": channel,
                  "text": f"PIPELINE FAILURE [{state}] step={step}\n{type(error).__name__}: {error}"},
            timeout=10,
        )
    except Exception:
        pass   # don't let notification failure mask the original error


def run_campaign(row):
    cfg = config.build(row)
    state = cfg["state_name"]

    if not cfg["active"]:
        log.info(f"[{state}] active=FALSE — skipped")
        return

    if not cfg["in_campaign_window"]:
        log.info(
            f"[{state}] outside campaign window "
            f"({cfg['campaign_start']} to {cfg['campaign_end']}) — skipped"
        )
        return

    log.info(f"[{state}] ── Day {cfg['DAY']} / {cfg['campaign_days']} ─────────────")

    try:
        analyze.run(cfg)
    except Exception as e:
        log.error(f"[{state}] analyze FAILED: {e}", exc_info=True)
        _slack_error(cfg, state, "analyze", e)
        _update_sheet_status(cfg, "FAILED", "analyze", str(e))
        return

    try:
        cdd_sync.run(cfg)
    except Exception as e:
        log.error(f"[{state}] cdd_sync FAILED: {e}", exc_info=True)
        _slack_error(cfg, state, "cdd_sync", e)
        # non-fatal — continue to report with no sync data

    try:
        docx_path, slack_text = report.run(cfg)
    except Exception as e:
        log.error(f"[{state}] report FAILED: {e}", exc_info=True)
        _slack_error(cfg, state, "report", e)
        _update_sheet_status(cfg, "FAILED", "report", str(e))
        return

    drive_link = ""
    try:
        drive_link = notify.run(cfg, docx_path, slack_text) or ""
    except Exception as e:
        log.error(f"[{state}] notify FAILED: {e}", exc_info=True)
        _slack_error(cfg, state, "notify", e)
        _update_sheet_status(cfg, "FAILED", "notify", str(e))
        return

    _update_sheet_status(cfg, "SUCCESS", drive_link=drive_link)
    log.info(f"[{state}] DONE")


def main():
    log.info("=" * 60)
    log.info(f"DST Daily Report Run  —  {date.today().isoformat()}")
    log.info("=" * 60)

    try:
        rows = config.get_active_rows()
    except Exception as e:
        log.error(f"Failed to read Google Sheet: {e}", exc_info=True)
        sys.exit(1)

    if not rows:
        log.warning("No rows returned from Google Sheet — nothing to do")
        return

    for row in rows:
        state = str(row.get("state_name", "unknown")).strip()
        try:
            run_campaign(row)
        except Exception as e:
            log.error(f"[{state}] unexpected error: {e}", exc_info=True)

    log.info("=" * 60)
    log.info("Run complete")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
