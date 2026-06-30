"""
config.py — Google Sheet reader + config builder
Reads one row per active campaign and returns a fully resolved config dict.
"""
import os
import logging
from datetime import date, timedelta, datetime

import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

log = logging.getLogger(__name__)

_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_MONTH_MAP = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May",     6: "June",     7: "July",  8: "August",
    9: "September", 10: "October", 11: "November", 12: "December",
}


def _resolve_creds_path():
    """Return credential.json path — falls back to file in project root if env path is wrong OS."""
    configured = os.getenv("GOOGLE_CREDENTIALS_PATH", "")
    if configured and os.path.exists(configured):
        return configured
    # Fall back: credential.json next to run.py (project root, one level above pipeline/)
    fallback = os.path.join(os.path.dirname(__file__), "..", "credential.json")
    fallback = os.path.abspath(fallback)
    if os.path.exists(fallback):
        log.info(f"[config] GOOGLE_CREDENTIALS_PATH not found; using fallback: {fallback}")
        return fallback
    raise FileNotFoundError(
        f"credential.json not found. Tried:\n  {configured}\n  {fallback}\n"
        f"Set GOOGLE_CREDENTIALS_PATH in .env to the correct path."
    )


def _gs_client():
    creds = Credentials.from_service_account_file(_resolve_creds_path(), scopes=_SCOPES)
    return gspread.Client(auth=creds)


def _parse_date(val):
    if not val:
        return None
    s = str(val).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H-%M-%S"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _bool(val):
    return str(val).strip().upper() in ("TRUE", "YES", "1", "Y")


def _date_label(d):
    return f"{d.day} {_MONTH_MAP[d.month]} {d.year}"


def get_active_rows():
    """Return all rows from the DST config Google Sheet."""
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise ValueError("GOOGLE_SHEET_ID not set in .env")
    client  = _gs_client()
    sheet   = client.open_by_key(sheet_id).worksheet("Sheet1")
    rows    = sheet.get_all_records()
    log.info(f"Google Sheet: {len(rows)} rows loaded")
    return rows


def build(row):
    """
    Build a fully resolved config dict from a Google Sheet row.
    Auto-computes DAY, GTE, LTE, DATE_LABEL, CAMPAIGN_DATES, ES index names.
    """
    tenant       = str(row.get("tenant", "")).strip().lower()
    campaign_start = _parse_date(row.get("campaign_start", ""))
    campaign_end   = _parse_date(row.get("campaign_end", ""))

    extract_date = date.today()
    # Local test override — set TEST_EXTRACT_DATE=YYYY-MM-DD in .env
    _test_date = os.getenv("TEST_EXTRACT_DATE", "").strip()
    if _test_date:
        try:
            extract_date = date.fromisoformat(_test_date)
            log.info(f"[config] TEST_EXTRACT_DATE override active: {extract_date}")
        except ValueError:
            log.warning(f"[config] Invalid TEST_EXTRACT_DATE '{_test_date}' — using today")

    if not campaign_start or not campaign_end:
        raise ValueError(
            f"[{row.get('state_name')}] campaign_start / campaign_end missing or unparseable"
        )

    today = extract_date
    in_window = campaign_start <= extract_date <= campaign_end

    campaign_days_cfg = int(row.get("campaign_days", 4) or 4)
    day = (today - campaign_start).days + 1
    day = max(1, min(day, campaign_days_cfg))

    campaign_dates = [
        (campaign_start + timedelta(days=i)).isoformat() for i in range(day)
    ]

    start_label = _date_label(campaign_start)
    end_label   = _date_label(campaign_end)
    date_label  = _date_label(today)

    gte = f"{today.isoformat()}T00:00:00.000Z"
    lte = f"{today.isoformat()}T23:59:59.999Z"

    out_dir = str(row.get("out_dir", "")).strip() or os.path.join(
        os.path.dirname(__file__), "output", tenant
    )
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(os.path.join(out_dir, "logs"), exist_ok=True)

    return {
        # identity
        "active":          _bool(row.get("active", "TRUE")),
        "in_campaign_window": in_window,
        "campaign_name":   str(row.get("campaign_name", "")).strip(),
        "state_name":      str(row.get("state_name", "")).strip(),
        "tenant":          tenant,
        "drug_type":       str(row.get("drug_type", "SPAQ")).strip().upper(),

        # dates
        "campaign_start":  campaign_start,
        "campaign_end":    campaign_end,
        "extract_date":    today,   # always today — no sheet override
        "campaign_days":   campaign_days_cfg,
        "DAY":             day,
        "GTE":             gte,
        "LTE":             lte,
        "DATE_LABEL":      date_label,
        "START_LABEL":     start_label,
        "END_LABEL":       end_label,
        "CAMPAIGN_DATES":  campaign_dates,

        # ES credentials from .env
        "es_url":  os.getenv("ES_URL"),
        "es_auth": (os.getenv("ES_USER"), os.getenv("ES_PASS")) if os.getenv("ES_USER") else None,
        "ES_INDEX_TASK":      f"{tenant}-project-task-index-v1",
        "ES_INDEX_STAFF":     f"{tenant}-project-staff-index-v1",
        "ES_INDEX_SYNC":      f"{tenant}-user-sync-index-v1",
        "ES_INDEX_IND":       f"{tenant}-individual-index-v1",
        "ES_INDEX_HH_MEMBER": f"{tenant}-household-member-index-v1",

        # Campaign identifier — drives ES filter in analyze.py and cdd_sync.py
        # is_admin_console=TRUE  → filter by campaignNumber (Nigeria, Chad admin)
        # is_admin_console=FALSE → filter by projectTypeId (Togo) OR projectType+cycleIndex (AZM Nigeria/Congo)
        "is_admin_console":  _bool(row.get("is_admin_console", "TRUE")),
        "campaign_number":   str(row.get("campaign_number", "")).strip(),
        "project_type_id":   str(row.get("project_type_id", "")).strip(),
        "project_type":      str(row.get("project_type", "")).strip(),
        "cycle_index":       str(row.get("cycle_index", "")).strip(),

        # ES date range field: "taskDates" (default) or "@timestamp"
        "task_date_field":   str(row.get("task_date_field", "taskDates")).strip() or "taskDates",

        # Whether analyze.py adds doseIndex=1 to treatment query
        # FALSE for all Nigeria SMC states (extraction scripts confirm doseIndex not used)
        # FALSE for AZM. TRUE only if your task docs require it.
        "dose_index_filter": _bool(row.get("dose_index_filter", "FALSE")),

        # Whether analyze.py adds campaign filter (campaignNumber/projectTypeId/projectType)
        # to the task index query. FALSE for all Nigeria SMC states — date range alone
        # isolates the campaign. TRUE only for AZM/non-admin where multiple project types
        # share the same tenant and date range.
        "task_campaign_filter": _bool(row.get("task_campaign_filter", "FALSE")),

        # targets / counts
        "target_csv":      str(row.get("target_csv", "")).strip(),
        "hfs_total":       int(row.get("hfs_total", 0) or 0),
        "flws_total":      int(row.get("flws_total", 0) or 0),
        "lgas_total":      int(row.get("lgas_total", 0) or 0),

        # output
        "out_dir":         out_dir,
        "google_sheet_id": str(row.get("google_sheet_id", "")).strip(),
        "slack_channel":   str(row.get("slack_channel", "")).strip(),

        # scheduler — comma-separated 24h times e.g. "11:00,14:00,17:00,20:00"
        "report_times": [
            t.strip() for t in str(row.get("report_times", "")).split(",")
            if t.strip()
        ],

        # derived filenames
        "perf_xlsx":  os.path.join(out_dir, f"performance_day{day}.xlsx"),
        "sync_xlsx":  os.path.join(out_dir, f"cdd_sync_day{day}.xlsx"),
        "docx_path":  os.path.join(out_dir,
                                   f"{str(row.get('state_name','')).strip().replace(' ','_')}"
                                   f"_Day{day}_Report_{datetime.now().strftime('%H%M')}.docx"),
    }
