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


# ── in-code feature defaults ───────────────────────────────────────────────────
# ITN duplicate-distribution matrix (analyze_itn._classify_duplicates): the
# code-side switch, so no Google Sheet column is needed. Flip to "TRUE" to
# enable it for every ITN/LLIN row this deployment runs; SMC/AZM rows never
# read it. A dup_matrix column on the sheet, if one is ever added, overrides
# this per row (TRUE/FALSE cell beats the default; empty cell falls back here).
DUP_MATRIX_DEFAULT = "FALSE"


def _date_label(d):
    return f"{d.day} {_MONTH_MAP[d.month]} {d.year}"


def _safe_int(val, default):
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def _sanitize_tab(name):
    """openpyxl sheet name: strip invalid chars []:*?/\\ and cap at 31."""
    safe = str(name).translate(str.maketrans("", "", "[]:*?/\\")).strip()
    return safe[:31] or "SECONDARY"


def _parse_secondary_products(row):
    """
    Parse the secondary product(s) counted alongside the primary drug.

    Read from a single sheet column — `secondary_products` if present, else the
    existing `secondary_product` column. The value drives one of two formats:

    1. LIST format (contains '|' or ';') — one or more entries, each pipe-delimited
       as  productName|label|ageMin|ageMax  (label/bands optional), ';'-separated:
           Red VAS|Red VAS; Blue VAS|Blue VAS
           VITAMIN_A_RED|Red VAS|12|59; VITAMIN_A_BLUE|Blue VAS|6|11
       Omit the age band to count the product across ALL ages (the productName
       already encodes the band; age-filtering would undercount vs dashboard).

    2. LEGACY single value (no delimiter) — a bare product name, counted age 3-59,
       tab "ORS-ZINC" (keeps Kebbi/Sokoto ORS-Zinc working unchanged).

    Returns a list of {name, label, age_min, age_max, tab} dicts (possibly empty).
    """
    raw = (str(row.get("secondary_products", "")).strip()
           or str(row.get("secondary_product", "")).strip())
    if not raw:
        return []

    # Legacy single product (no list delimiters) — bare name, age 3-59.
    if "|" not in raw and ";" not in raw:
        return [{"name": raw, "label": raw,
                 "age_min": 3, "age_max": 59, "tab": "ORS-ZINC"}]

    # List format.
    specs = []
    for chunk in raw.split(";"):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts = [p.strip() for p in chunk.split("|")]
        name  = parts[0] if parts else ""
        if not name:
            continue
        label = parts[1] if len(parts) > 1 and parts[1] else name
        amin  = _safe_int(parts[2], None) if len(parts) > 2 and parts[2] != "" else None
        amax  = _safe_int(parts[3], None) if len(parts) > 3 and parts[3] != "" else None
        specs.append({"name": name, "label": label,
                      "age_min": amin, "age_max": amax,
                      "tab": _sanitize_tab(label)})
    return specs


def get_active_rows():
    """Return all rows from the DST config Google Sheet.

    The worksheet tab is configurable per deployment via GOOGLE_SHEET_TAB
    (defaults to "Sheet1"). This lets each environment/cluster read its own
    tab from the same shared sheet — e.g. GOOGLE_SHEET_TAB=taraba on the
    Taraba Jupyter, GOOGLE_SHEET_TAB=togo on the Togo one.
    """
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise ValueError("GOOGLE_SHEET_ID not set in .env")
    tab    = os.getenv("GOOGLE_SHEET_TAB", "Sheet1").strip() or "Sheet1"
    client = _gs_client()
    try:
        sheet = client.open_by_key(sheet_id).worksheet(tab)
    except gspread.WorksheetNotFound:
        raise ValueError(
            f"Worksheet tab '{tab}' not found in sheet {sheet_id}. "
            f"Check GOOGLE_SHEET_TAB in .env, or create the tab with the "
            f"standard column headers."
        )
    rows = sheet.get_all_records(numericise_ignore=["all"])
    log.info(f"Google Sheet tab '{tab}': {len(rows)} rows loaded")
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

    if not os.getenv("ES_URL"):
        raise ValueError("ES_URL not set in .env — cannot connect to Elasticsearch")

    today = extract_date
    in_window = campaign_start <= extract_date <= campaign_end

    try:
        campaign_days_cfg = int(float(row.get("campaign_days", 4) or 4))
    except (ValueError, TypeError):
        campaign_days_cfg = 4
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

    # ES index prefix. Nigeria central instances are tenant-prefixed (e.g.
    # "ba-project-task-index-v1"). Togo's dedicated cluster uses UN-prefixed
    # indices ("project-task-index-v1") with tenantId carried inside each doc.
    # Control per deployment via ES_INDEX_PREFIX in .env:
    #   unset      -> "{tenant}-"  (default, Nigeria central)
    #   ES_INDEX_PREFIX=   (empty) -> no prefix (Togo dedicated cluster)
    #   ES_INDEX_PREFIX=xx-        -> custom prefix
    _prefix_env = os.getenv("ES_INDEX_PREFIX")
    idx_prefix  = f"{tenant}-" if _prefix_env is None else _prefix_env

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
        "ES_INDEX_TASK":      f"{idx_prefix}project-task-index-v1",
        "ES_INDEX_STAFF":     f"{idx_prefix}project-staff-index-v1",
        "ES_INDEX_SYNC":      f"{idx_prefix}user-sync-index-v1",
        "ES_INDEX_IND":       f"{idx_prefix}individual-index-v1",
        "ES_INDEX_PB":        f"{idx_prefix}project-beneficiary-index-v1",
        "ES_INDEX_HH_MEMBER": f"{idx_prefix}household-member-index-v1",

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

        # ITN only: duplicate-distribution matrix (same/different user x same/different
        # day per household). Off keeps every existing number, query, Word section and
        # Slack post unchanged (the performance Excel only gains six empty trailing
        # columns). Default lives IN CODE (DUP_MATRIX_DEFAULT above — no sheet column
        # required); a non-empty dup_matrix sheet cell overrides it per row.
        "dup_matrix": _bool(str(row.get("dup_matrix", "")).strip() or DUP_MATRIX_DEFAULT),

        # secondary product(s) counted alongside the primary drug — empty = disabled.
        # Legacy single string (age 3-59) OR a spec list (see _parse_secondary_products).
        "secondary_product":  str(row.get("secondary_product", "")).strip(),
        "secondary_products": _parse_secondary_products(row),

        # targets / counts
        "target_csv":      str(row.get("target_csv", "")).strip(),
        "hfs_total":       int(float(row.get("hfs_total", 0) or 0)),
        "flws_total":      int(float(row.get("flws_total", 0) or 0)),
        "lgas_total":      int(float(row.get("lgas_total", 0) or 0)),

        # output
        "out_dir":         out_dir,
        "google_sheet_id": str(row.get("google_sheet_id", "")).strip(),
        "slack_channel":          str(row.get("slack_channel", "")).strip(),
        "slack_channel_partners": str(row.get("slack_channel_partners", "")).strip(),

        # scheduler — comma-separated 24h times e.g. "11:00,14:00,17:00,20:00"
        "report_times": [
            t.strip() for t in str(row.get("report_times", "")).split(",")
            if t.strip()
        ],

        # partner report schedule — separate times for the partner-channel post.
        # If empty, the partner report goes out together with the internal report
        # at report_times (backward-compatible). If set, report_times becomes
        # internal-only and these times drive the partner-only post.
        "partner_report_times": [
            t.strip() for t in str(row.get("partner_report_times", "")).split(",")
            if t.strip()
        ],

        # derived filenames
        "perf_xlsx":  os.path.join(out_dir, f"performance_day{day}.xlsx"),
        "sync_xlsx":  os.path.join(out_dir, f"cdd_sync_day{day}.xlsx"),
        "docx_path":  os.path.join(out_dir,
                                   f"{str(row.get('state_name','')).strip().replace(' ','_')}"
                                   f"_Day{day}_Report_{datetime.now().strftime('%H%M')}.docx"),
        "partner_docx_path": os.path.join(out_dir,
                                          f"{str(row.get('state_name','')).strip().replace(' ','_')}"
                                          f"_Day{day}_PartnerReport_{datetime.now().strftime('%H%M')}.docx"),
    }
