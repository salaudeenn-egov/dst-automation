"""Appends run outcomes to the "Run Log" tab of the config Google Sheet.

The human-visible run history the team checks — one row per report attempt.
Shared by run.py and the Airflow finalize task so both orchestrators write
the identical format. Non-fatal by design: a Sheets hiccup must never mask
the run's real outcome (the Postgres audit table remains the source of truth
on the Airflow side).
"""
import logging
import os
from datetime import datetime

log = logging.getLogger(__name__)

_HEADER = ["Timestamp", "State", "Campaign", "Day", "Time",
           "Status", "Step Failed", "Error", "Drive Link"]


def append_run_log(state_name, campaign_name, day, status,
                   step_failed="", error="", drive_link=""):
    """Append one outcome row. Returns True on success, False otherwise."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        from pipeline.config import _resolve_creds_path

        sheet_id = os.getenv("GOOGLE_SHEET_ID")
        if not sheet_id:
            log.warning("[run-log] GOOGLE_SHEET_ID not set — skipping Run Log append")
            return False

        creds = Credentials.from_service_account_file(
            _resolve_creds_path(),
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"])
        spreadsheet = gspread.Client(auth=creds).open_by_key(sheet_id)
        tab_name = os.getenv("GOOGLE_RUNLOG_TAB", "Run Log")
        try:
            ws = spreadsheet.worksheet(tab_name)
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(tab_name, rows=1000, cols=10)
            ws.append_row(_HEADER)

        now = datetime.now()
        ws.append_row(
            [now.strftime("%Y-%m-%d %H:%M"), state_name, campaign_name, str(day),
             now.strftime("%H:%M"), status, step_failed,
             str(error)[:300] if error else "", drive_link or ""],
            value_input_option="USER_ENTERED")
        log.info(f"[run-log] appended: {state_name} Day {day} -> {status}")
        return True
    except Exception as e:
        log.warning(f"[run-log] append failed (non-fatal): {e}")
        return False
