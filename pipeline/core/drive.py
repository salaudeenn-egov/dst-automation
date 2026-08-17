"""Google Drive helpers for machine artifacts (checkpoints, staged temp files).

Unlike the report uploads in notify.py, these transfers never convert files
(bytes round-trip exactly), never grant anyone-with-link access (checkpoints
carry beneficiary names), and overwrite by files.update because the service
account cannot delete. All calls target a Shared Drive (supportsAllDrives).
"""
import io
import logging
import mimetypes
import os

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

log = logging.getLogger(__name__)

_SCOPES = ["https://www.googleapis.com/auth/drive"]


def _service():
    from pipeline.config import _resolve_creds_path
    creds = Credentials.from_service_account_file(_resolve_creds_path(), scopes=_SCOPES)
    return build("drive", "v3", credentials=creds)


def _escape(name):
    return str(name).replace("\\", "\\\\").replace("'", "\\'")


def find_file(name, folder_id, service=None):
    """Return the file id of `name` inside `folder_id`, or None."""
    service = service or _service()
    q = (f"name = '{_escape(name)}' and '{folder_id}' in parents "
         f"and trashed = false")
    resp = service.files().list(
        q=q, spaces="drive", fields="files(id,name)",
        supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute()
    hits = resp.get("files", [])
    return hits[0]["id"] if hits else None


def find_or_create_folder(name, parent_id, service=None):
    """Return the id of sub-folder `name` under `parent_id`, creating it if absent."""
    service = service or _service()
    q = (f"name = '{_escape(name)}' and mimeType = 'application/vnd.google-apps.folder' "
         f"and '{parent_id}' in parents and trashed = false")
    resp = service.files().list(
        q=q, spaces="drive", fields="files(id,name)",
        supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute()
    hits = resp.get("files", [])
    if hits:
        return hits[0]["id"]
    meta = {"name": str(name), "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id]}
    folder = service.files().create(body=meta, fields="id", supportsAllDrives=True).execute()
    log.info(f"[drive] folder created: {name}")
    return folder["id"]


def upload_raw(path, name, folder_id):
    """Upload `path` as `name` into `folder_id` without conversion.

    Overwrites an existing file of the same name in place. Returns the file id.
    """
    service = _service()
    mime = mimetypes.guess_type(path)[0] or "application/octet-stream"
    media = MediaFileUpload(path, mimetype=mime, resumable=True)

    existing = find_file(name, folder_id, service)
    if existing:
        service.files().update(
            fileId=existing, media_body=media, supportsAllDrives=True,
        ).execute()
        log.info(f"[drive] updated: {name}")
        return existing

    file = service.files().create(
        body={"name": name, "parents": [folder_id]},
        media_body=media, fields="id", supportsAllDrives=True,
    ).execute()
    log.info(f"[drive] uploaded: {name}")
    return file["id"]


def download_raw(name, folder_id, dest_path):
    """Download `name` from `folder_id` to `dest_path`. Returns dest_path, or None if absent."""
    service = _service()
    file_id = find_file(name, folder_id, service)
    if not file_id:
        log.info(f"[drive] not found: {name}")
        return None
    os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    with open(dest_path, "wb") as f:
        f.write(buf.getvalue())
    log.info(f"[drive] downloaded: {name} -> {dest_path}")
    return dest_path
