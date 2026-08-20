"""The "Run Log" tab of the config Google Sheet — the run history in sheet mode.

One row per report attempt, written by the Airflow finalize task (and by
run.py's cumulative path in the identical format), and read back by the
Airflow scheduler's retime guard. There is no database anywhere: this tab is the
system of record in sheet mode (mdms mode publishes Kafka events instead).
All writes are non-fatal — a Sheets hiccup must never mask a run's outcome.
"""
import logging
import os
from datetime import datetime

log = logging.getLogger(__name__)

_HEADER = ["Timestamp", "State", "Campaign", "Day", "Time",
           "Status", "Step Failed", "Error", "Drive Link", "Mode"]


def _open_runlog_worksheet():
    """Open (creating if absent) the Run Log tab. Returns None when the sheet
    is not configured or unreachable — callers degrade, never crash."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        from pipeline.config import _resolve_creds_path

        sheet_id = os.getenv("GOOGLE_SHEET_ID")
        if not sheet_id:
            log.warning("[run-log] GOOGLE_SHEET_ID not set — Run Log unavailable")
            return None
        creds = Credentials.from_service_account_file(
            _resolve_creds_path(),
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"])
        spreadsheet = gspread.Client(auth=creds).open_by_key(sheet_id)
        tab_name = os.getenv("GOOGLE_RUNLOG_TAB", "Run Log")
        try:
            ws = spreadsheet.worksheet(tab_name)
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(tab_name, rows=1000, cols=12)
            ws.append_row(_HEADER)
        return ws
    except Exception as e:
        log.warning(f"[run-log] worksheet unavailable: {e}")
        return None


def append_run_log(state_name, campaign_name, day, status,
                   step_failed="", error="", drive_link="", mode=""):
    """Append one outcome row. Returns True on success, False otherwise."""
    ws = _open_runlog_worksheet()
    if ws is None:
        return False
    try:
        now = datetime.now()
        ws.append_row(
            [now.strftime("%Y-%m-%d %H:%M"), state_name, campaign_name, str(day),
             now.strftime("%H:%M"), status, step_failed,
             str(error)[:300] if error else "", drive_link or "", mode],
            value_input_option="USER_ENTERED")
        log.info(f"[run-log] appended: {state_name} Day {day} -> {status}")
        return True
    except Exception as e:
        log.warning(f"[run-log] append failed (non-fatal): {e}")
        return False


def fetch_today_runs():
    """Return today's Run Log rows as dicts: {state, status, time "HH:MM", mode}.

    Rows written before the Mode column existed default to mode "both" — the
    safe direction for the retime guard (they cover every slot mode). Returns
    an empty list when the sheet is unavailable; callers accept that as
    "history unknown" (a rare duplicate fire is preferred over never firing).
    """
    ws = _open_runlog_worksheet()
    if ws is None:
        return []
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        runs = []
        for row in ws.get_all_values()[1:]:
            if not row or not str(row[0]).startswith(today):
                continue
            runs.append({
                "state":  row[1] if len(row) > 1 else "",
                "status": (row[5] if len(row) > 5 else "").strip().upper(),
                "time":   str(row[0])[11:16],
                "mode":   (row[9] if len(row) > 9 else "").strip() or "both",
            })
        return runs
    except Exception as e:
        log.warning(f"[run-log] fetch failed: {e}")
        return []
