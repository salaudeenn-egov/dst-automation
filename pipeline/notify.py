"""
notify.py — Google Drive upload + Google Sheets write + Slack post

Authentication: single service account (credential.json) for both Drive and Sheets.
No OAuth token required. Grant the service account Editor access to:
  - The campaign config Google Sheet
  - The Drive folder where reports are uploaded
"""
import logging
import os

import gspread
import openpyxl
import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

log = logging.getLogger(__name__)

_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _creds():
    path = os.getenv("GOOGLE_CREDENTIALS_PATH")
    if not path or not os.path.exists(path):
        raise FileNotFoundError(f"credential.json not found: {path}")
    return Credentials.from_service_account_file(path, scopes=_SCOPES)


def _drive_creds():
    """Drive credentials — same service account, Shared Drive bypasses quota."""
    return _creds()


# ── Google Drive ───────────────────────────────────────────────────────────────

def _upload_to_drive(file_path, title):
    """Upload file to Drive (converts docx→Google Doc, xlsx→Google Sheet). Returns shareable URL."""
    service   = build("drive", "v3", credentials=_drive_creds())
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")

    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".xlsx":
        upload_mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        target_mime = "application/vnd.google-apps.spreadsheet"
        url_tmpl    = "https://docs.google.com/spreadsheets/d/{}/edit"
    else:
        upload_mime = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        target_mime = "application/vnd.google-apps.document"
        url_tmpl    = "https://docs.google.com/document/d/{}/edit"

    metadata = {"name": title, "mimeType": target_mime}
    if folder_id:
        metadata["parents"] = [folder_id]

    file = service.files().create(
        body=metadata,
        media_body=MediaFileUpload(file_path, mimetype=upload_mime, resumable=True),
        fields="id,name",
        supportsAllDrives=True,
    ).execute()

    file_id = file["id"]

    # Make readable by anyone with the link
    service.permissions().create(
        fileId=file_id,
        body={"type": "anyone", "role": "reader"},
        supportsAllDrives=True,
    ).execute()

    url = url_tmpl.format(file_id)
    log.info(f"[notify] Drive upload done: {file['name']} -> {url}")
    return url


# ── Google Sheets ──────────────────────────────────────────────────────────────

def _write_to_sheets(cfg, perf_xlsx):
    """Write performance rows to Google Sheet tab '{state_name} Day{N}'."""
    sheet_id = cfg.get("google_sheet_id", "")
    if not sheet_id:
        log.info("[notify] google_sheet_id not set — skipping Sheets write")
        return
    try:
        client      = gspread.Client(auth=_creds())
        spreadsheet = client.open_by_key(sheet_id)
        tab_name    = f"{cfg['state_name']} Day{cfg['DAY']}"

        try:
            ws = spreadsheet.worksheet(tab_name)
            ws.clear()
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=tab_name, rows=2000, cols=30)

        wb   = openpyxl.load_workbook(perf_xlsx, read_only=True)
        rows = list(wb["ALL FACILITIES"].iter_rows(min_row=2, values_only=True))
        wb.close()

        data = [[str(c) if c is not None else "" for c in row] for row in rows]
        if data:
            ws.update("A1", data)
        log.info(f"[notify] Sheets updated: '{tab_name}'")
    except Exception as e:
        log.warning(f"[notify] Sheets write failed (non-fatal): {e}")


# ── Slack ──────────────────────────────────────────────────────────────────────

def _slack_post(channel, text, token):
    r = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {token}"},
        json={"channel": channel, "text": text},
        timeout=30,
    )
    r.raise_for_status()
    resp = r.json()
    if not resp.get("ok"):
        raise RuntimeError(f"Slack postMessage failed: {resp.get('error')}")
    return resp


# ── shared helper (called by report.py for raw Excel uploads) ──────────────────

def upload_file(path, title):
    """Upload any file to Drive. Returns shareable link or empty string on failure."""
    try:
        return _upload_to_drive(path, title)
    except Exception as e:
        log.warning(f"[notify] upload_file failed (non-fatal): {e}")
        return ""


# ── public entry point ─────────────────────────────────────────────────────────

def run(cfg, docx_path, slack_text):
    token   = os.getenv("SLACK_TOKEN")
    channel = cfg.get("slack_channel", "")

    # Upload report to Drive
    drive_link = None
    if docx_path and os.path.exists(docx_path):
        try:
            from datetime import datetime as _dt
            title      = f"{cfg['state_name']} Day {cfg['DAY']} Report — {cfg['DATE_LABEL']} {_dt.now().strftime('%H:%M')}"
            drive_link = _upload_to_drive(docx_path, title)
        except Exception as e:
            log.warning(f"[notify] Drive upload failed (non-fatal): {e}")

    # Post to Slack
    if not token:
        log.warning("[notify] SLACK_TOKEN not set — skipping Slack")
        return
    if not channel:
        log.warning("[notify] slack_channel not set — skipping Slack")
        return

    try:
        from datetime import datetime, timezone
        message = slack_text
        if drive_link:
            message = f"{slack_text}\n\nFull report: {drive_link}"
        message += f"\n\nData extracted at {datetime.now(timezone.utc).strftime('%H:%M')} UTC on {cfg['DATE_LABEL']}."
        message += "\n[Beta] This report is auto-generated. Please verify before acting on the data."
        _slack_post(channel, message, token)
        log.info(f"[notify] Slack post done → {channel}")
    except Exception as e:
        log.error(f"[notify] Slack failed: {e}")
        raise

    return drive_link
