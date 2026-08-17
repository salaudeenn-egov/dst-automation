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
    from pipeline.config import _resolve_creds_path
    return Credentials.from_service_account_file(_resolve_creds_path(), scopes=_SCOPES)


def _drive_creds():
    """Drive credentials — same service account, Shared Drive bypasses quota."""
    return _creds()


# ── Drive folder organisation ────────────────────────────────────────────────
# Reports are filed under the root GOOGLE_DRIVE_FOLDER_ID in a per-campaign tree,
# all derived from existing config (no extra sheet column / env var):
#     <Instance>/<State>/<Campaign>/<Day N | Cumulative (Days 1-N)>
#   Instance = GOOGLE_SHEET_TAB (the per-deployment tab; naming the tab names the folder)
#   State    = state_name column
#   Campaign = campaign_name column (falls back to drug_type)

def _find_or_create_folder(service, name, parent_id):
    """Return the id of sub-folder `name` under `parent_id`, creating it if absent."""
    safe = str(name).strip().replace("/", "-") or "Unnamed"
    esc  = safe.replace("\\", "\\\\").replace("'", "\\'")
    q = (f"name = '{esc}' and mimeType = 'application/vnd.google-apps.folder' "
         f"and '{parent_id}' in parents and trashed = false")
    resp = service.files().list(
        q=q, spaces="drive", fields="files(id,name)",
        supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute()
    hits = resp.get("files", [])
    if hits:
        return hits[0]["id"]
    meta = {"name": safe, "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id]}
    folder = service.files().create(body=meta, fields="id", supportsAllDrives=True).execute()
    log.info(f"[notify] Drive folder created: {safe}")
    return folder["id"]


def campaign_folder_id(cfg):
    """
    Resolve (creating on demand) the Drive folder this run's files belong in, and
    cache it on cfg. Returns "" if no root folder is configured — callers then fall
    back to the old flat upload. Path: <Instance>/<State>/<Campaign>/<leaf>.
    """
    if cfg.get("_drive_folder_id"):
        return cfg["_drive_folder_id"]
    root = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "").strip()
    if not root:
        return ""
    try:
        service  = build("drive", "v3", credentials=_drive_creds())
        tab      = (os.getenv("GOOGLE_SHEET_TAB", "Sheet1") or "Sheet1").strip()
        instance = tab.title() if tab.islower() else tab
        state    = cfg.get("state_name") or cfg.get("tenant") or "Unknown"
        campaign = cfg.get("campaign_name") or cfg.get("drug_type") or "Campaign"
        leaf = (f"Cumulative (Days 1-{cfg.get('DAY', '')})" if cfg.get("cumulative")
                else f"Day {cfg.get('DAY', '')}")
        fid = root
        for part in (instance, state, campaign, leaf):
            fid = _find_or_create_folder(service, part, fid)
        cfg["_drive_folder_id"] = fid
        log.info(f"[notify] Drive target: {instance}/{state}/{campaign}/{leaf}")
        return fid
    except Exception as e:
        log.warning(f"[notify] could not resolve campaign Drive folder (using root): {e}")
        return ""


# ── Google Drive ───────────────────────────────────────────────────────────────

def _upload_to_drive(file_path, title, folder_id=None):
    """Upload file to Drive (converts docx→Google Doc, xlsx→Google Sheet). Returns shareable URL."""
    service   = build("drive", "v3", credentials=_drive_creds())
    folder_id = folder_id or os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")

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

def upload_file(path, title, folder_id=None):
    """Upload any file to Drive. Returns shareable link or empty string on failure."""
    try:
        return _upload_to_drive(path, title, folder_id=folder_id)
    except Exception as e:
        log.warning(f"[notify] upload_file failed (non-fatal): {e}")
        return ""


# ── campaign temp files (checkpoints) ──────────────────────────────────────────

def temp_folder_id(cfg):
    """Resolve (creating on demand) <Instance>/<State>/<Campaign>/<Day N>/temp."""
    fid = campaign_folder_id(cfg)
    if not fid:
        return ""
    from pipeline.core import drive
    return drive.find_or_create_folder("temp", fid)


def upload_checkpoints(cfg, stages=("analyze", "cdd_sync")):
    """Push this run's checkpoint JSONs to the campaign's Drive temp folder.

    Raw upload (no conversion, no public link — checkpoints carry beneficiary
    names), overwriting any previous copy of the same run. Non-fatal throughout.
    """
    from pipeline.core import drive
    from pipeline.core.checkpoint import checkpoint_path
    uploaded = {}
    try:
        folder = temp_folder_id(cfg)
        if not folder:
            log.info("[notify] no Drive root configured — skipping checkpoint upload")
            return uploaded
        for stage in stages:
            path = checkpoint_path(cfg, stage)
            if os.path.exists(path):
                uploaded[stage] = drive.upload_raw(path, os.path.basename(path), folder)
    except Exception as e:
        log.warning(f"[notify] checkpoint upload failed (non-fatal): {e}")
    return uploaded


def download_checkpoint(cfg, stage):
    """Fetch a stage checkpoint from the campaign's Drive temp folder into the
    local checkpoints directory, enabling rerun_from_checkpoint on any machine.
    Returns the local path, or None when the checkpoint is not on Drive."""
    from pipeline.core import drive
    from pipeline.core.checkpoint import checkpoint_path
    folder = temp_folder_id(cfg)
    if not folder:
        return None
    path = checkpoint_path(cfg, stage)
    return drive.download_raw(os.path.basename(path), folder, path)


# ── public entry point ─────────────────────────────────────────────────────────

def run(cfg, docx_path, slack_text, partner_docx_path=None, mode="both"):
    """
    mode:
      "both"     — post internal (main channel) and partner report (default)
      "internal" — post only the internal report to the main channel
      "partner"  — post only the partner report to the partner channel
    """
    token   = os.getenv("SLACK_TOKEN")
    channel = cfg.get("slack_channel", "")

    do_internal = mode in ("both", "internal")
    do_partner  = mode in ("both", "partner")

    # Per-campaign Drive folder (auto-created); "" falls back to the flat root folder
    fid = campaign_folder_id(cfg)

    # Upload main report to Drive (only when posting internally)
    drive_link = None
    if do_internal and docx_path and os.path.exists(docx_path):
        try:
            from datetime import datetime as _dt
            title      = f"{cfg['state_name']} Day {cfg['DAY']} Report — {cfg['DATE_LABEL']} {_dt.now().strftime('%H:%M')}"
            drive_link = _upload_to_drive(docx_path, title, folder_id=fid)
        except Exception as e:
            log.warning(f"[notify] Drive upload failed (non-fatal): {e}")

    # Post to Slack
    if not token:
        log.warning("[notify] SLACK_TOKEN not set — skipping Slack")
        return

    # Main channel — full report (only if configured; a failure must not block the partner post)
    if do_internal and channel:
        try:
            message = slack_text
            if drive_link:
                message = f"{slack_text}\n\nFull report: {drive_link}"
            _slack_post(channel, message, token)
            log.info(f"[notify] Slack post done -> {channel}")
        except Exception as e:
            log.error(f"[notify] Slack failed (non-fatal): {e}")
    elif do_internal:
        log.warning("[notify] slack_channel not set — skipping main post (partner post still runs)")

    # Partner channel — report without DQ sections (if configured)
    partner_channel = cfg.get("slack_channel_partners", "")
    if do_partner and partner_channel and partner_docx_path and os.path.exists(partner_docx_path):
        # Upload and post in separate try blocks so Slack post fires even if Drive fails
        partner_link = ""
        try:
            from datetime import datetime as _dt2
            partner_title = (f"{cfg['state_name']} Day {cfg['DAY']} Report — "
                             f"{cfg['DATE_LABEL']} {_dt2.now().strftime('%H:%M')}")
            partner_link = _upload_to_drive(partner_docx_path, partner_title, folder_id=fid)
        except Exception as e:
            log.warning(f"[notify] Partner Drive upload failed (non-fatal): {e}")

        try:
            partner_msg = slack_text
            if partner_link:
                partner_msg = f"{slack_text}\n\nFull report: {partner_link}"
            _slack_post(partner_channel, partner_msg, token)
            log.info(f"[notify] Partner Slack post done -> {partner_channel}")
        except Exception as e:
            log.warning(f"[notify] Partner channel post failed (non-fatal): {e}")

    upload_checkpoints(cfg)

    return drive_link
