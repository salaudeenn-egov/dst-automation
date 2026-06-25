"""
notify.py — Google Drive upload + Google Sheets write + Slack post
Flow: .docx → Google Drive (converts to Google Doc) → shareable link → Slack
"""
import logging
import os

import gspread
import openpyxl
import requests
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from oauth2client.service_account import ServiceAccountCredentials

log = logging.getLogger(__name__)

_SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

_DRIVE_TOKEN = os.path.join(os.path.dirname(__file__), "drive_token.json")


# ── Google credentials ─────────────────────────────────────────────────────────

def _creds():
    creds_path = os.getenv("GOOGLE_CREDENTIALS_PATH")
    if not creds_path or not os.path.exists(creds_path):
        raise FileNotFoundError(f"credential.json not found: {creds_path}")
    return ServiceAccountCredentials.from_json_keyfile_name(creds_path, _SCOPE)


def _drive_creds():
    """Use personal Gmail OAuth token if available, else fall back to service account."""
    if os.path.exists(_DRIVE_TOKEN):
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        creds = Credentials.from_authorized_user_file(
            _DRIVE_TOKEN, ["https://www.googleapis.com/auth/drive"]
        )
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(_DRIVE_TOKEN, "w") as f:
                f.write(creds.to_json())
        log.info("[notify] Using Gmail OAuth for Drive upload")
        return creds
    return _creds()


# ── Google Drive ───────────────────────────────────────────────────────────────

def _upload_to_drive(docx_path, title):
    """
    Upload .docx to Google Drive and convert to Google Docs format.
    Returns a shareable 'anyone with link can view' URL.
    """
    service   = build("drive", "v3", credentials=_drive_creds())
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")

    # Detect file type to set correct MIME — xlsx stays as Excel, docx converts to Google Doc
    ext = os.path.splitext(docx_path)[1].lower()
    if ext == ".xlsx":
        upload_mime  = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        target_mime  = "application/vnd.google-apps.spreadsheet"
    else:
        upload_mime  = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        target_mime  = "application/vnd.google-apps.document"

    metadata = {
        "name":     title,
        "mimeType": target_mime,
    }
    if folder_id:
        metadata["parents"] = [folder_id]

    media = MediaFileUpload(
        docx_path,
        mimetype=upload_mime,
        resumable=True,
    )

    file = service.files().create(
        body=metadata,
        media_body=media,
        fields="id,name",
    ).execute()

    file_id = file["id"]

    # transfer ownership to real user so files count against their quota, not service account's
    owner_email = os.getenv("GOOGLE_OWNER_EMAIL", "")
    if owner_email:
        try:
            service.permissions().create(
                fileId=file_id,
                transferOwnership=True,
                body={"type": "user", "role": "owner", "emailAddress": owner_email},
            ).execute()
            log.info(f"[notify] Drive ownership transferred to {owner_email}")
        except Exception as e:
            log.warning(f"[notify] Ownership transfer failed (non-fatal): {e}")

    # anyone with the link can view
    service.permissions().create(
        fileId=file_id,
        body={"type": "anyone", "role": "reader"},
    ).execute()

    if ext == ".xlsx":
        url = f"https://docs.google.com/spreadsheets/d/{file_id}/edit"
    else:
        url = f"https://docs.google.com/document/d/{file_id}/edit"
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
        client      = gspread.authorize(_creds())
        spreadsheet = client.open_by_key(sheet_id)
        tab_name    = f"{cfg['state_name']} Day{cfg['DAY']}"

        try:
            ws = spreadsheet.worksheet(tab_name)
            ws.clear()
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=tab_name, rows=2000, cols=30)

        wb = openpyxl.load_workbook(perf_xlsx, read_only=True)
        sheet = wb["ALL FACILITIES"]
        rows  = list(sheet.iter_rows(min_row=2, values_only=True))
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


# ── shared helper (called by report.py to upload raw Excel files) ──────────────

def upload_file(path, title):
    """
    Upload any file to Google Drive and return a shareable link.
    Non-fatal: returns empty string on any error.
    """
    try:
        return _upload_to_drive(path, title)
    except Exception as e:
        log.warning(f"[notify] upload_file failed (non-fatal): {e}")
        return ""


# ── public entry point ─────────────────────────────────────────────────────────

def run(cfg, docx_path, slack_text):
    token   = os.getenv("SLACK_TOKEN")
    channel = cfg.get("slack_channel", "")

    # Step 1: upload .docx to Google Drive → get shareable link
    drive_link = None
    if docx_path and os.path.exists(docx_path):
        try:
            from datetime import datetime as _dt
            title      = f"{cfg['state_name']} Day {cfg['DAY']} Report — {cfg['DATE_LABEL']} {_dt.now().strftime('%H:%M')}"
            drive_link = _upload_to_drive(docx_path, title)
        except Exception as e:
            log.warning(f"[notify] Drive upload failed (non-fatal): {e}")

    # Step 3: post Claude summary + Drive link to Slack
    if not drive_link:
        log.warning("[notify] No Drive link — Slack message will have no report link")
    if not token:
        log.warning("[notify] SLACK_TOKEN not set — skipping Slack")
        return
    if not channel:
        log.warning("[notify] slack_channel not set — skipping Slack")
        return

    try:
        from datetime import datetime
        extract_time = datetime.now().strftime("%H:%M")
        message = slack_text
        if drive_link:
            message = f"{slack_text}\n\nFull report: {drive_link}"
        message += f"\n\nData extracted at {extract_time} on {cfg['DATE_LABEL']}."
        message += "\n[Beta] This report is auto-generated. Please verify before acting on the data."
        _slack_post(channel, message, token)
        log.info(f"[notify] Slack post done → {channel}")
    except Exception as e:
        log.error(f"[notify] Slack failed: {e}")
        raise

    return drive_link
