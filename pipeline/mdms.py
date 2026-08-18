"""MDMS as the machine-read campaign-config store, mirrored one-way from the
Google Sheet.

The sheet stays the only human surface (tabs = deployment groups, rows =
tenant campaigns); the dst_config_sync DAG mirrors each tab into MDMS, and the
scheduler reads MDMS instead of the sheet (DST_CONFIG_SOURCE=mdms). Same read
pattern as the platform's hcm_campaign_scheduler, including internal calls
with a dummy authToken (auth is enforced at the gateway, which in-cluster
service-to-service traffic never crosses).

Tenancy mapping: one MDMS root tenant per environment; every entry carries
deploymentGroup (= sheet tab) and its campaign tenant inside data.row, so one
search serves all of a group's tenants — exactly how a sheet tab works.

Sync design: plan_sync() is PURE (sheet rows + existing entries -> actions);
apply_sync() does the HTTP. Every edge case lives in the plan and is
unit-testable without a network.

Entry data shape (top-level fields drive schema uniqueness):
    {"rowIdentity": "<tenant>::<campaign key>",
     "deploymentGroup": "<sheet tab / group name>",
     "row": {...the sheet row verbatim, all values as strings...}}

Env: MDMS_URL (in-cluster base URL, required to enable),
MDMS_SEARCH_ENDPOINT (/mdms-v2/v2/_search), MDMS_TENANT_ID, MDMS_AUTH_TOKEN
(default "" — internal calls need none), DST_MDMS_SCHEMA_CODE, MDMS_LIMIT.
"""
import logging
import os

import requests

log = logging.getLogger(__name__)

DEFAULT_SCHEMA_CODE = "airflow-configs.dst-campaign-report-config"


def _base_url():
    return os.getenv("MDMS_URL", "").strip().rstrip("/")


def _schema_code():
    return os.getenv("DST_MDMS_SCHEMA_CODE", DEFAULT_SCHEMA_CODE)


def _mdms_tenant(group=None):
    """One MDMS root tenant per environment; a group may override it."""
    if group and str(group.get("mdms_tenant", "")).strip():
        return str(group["mdms_tenant"]).strip()
    return os.getenv("MDMS_TENANT_ID", os.getenv("TENANT_ID", "dev"))


def _request_info():
    return {"apiId": "dst-automation", "msgId": "dst-config-sync",
            "authToken": os.getenv("MDMS_AUTH_TOKEN", ""),
            "userInfo": {"id": 1}}


def row_identity(row):
    """Stable identity of a campaign across edits: tenant + campaign key.

    Editing times/dates/channels keeps the identity (-> update); a genuinely
    new campaign (new campaign_number) is a new identity (-> create). Falls
    back to project_type_id for non-admin tenants, then state_name."""
    tenant = str(row.get("tenant", "")).strip().lower()
    campaign_key = (str(row.get("campaign_number", "")).strip()
                    or str(row.get("project_type_id", "")).strip()
                    or str(row.get("state_name", "")).strip().lower())
    return f"{tenant}::{campaign_key}"


def normalize_row(row):
    return {str(k).strip(): str(v).strip() for k, v in row.items()}


def validate_row(row):
    """Pre-flight checks a bad sheet edit must not get past. Returns a list
    of problems (empty = valid). MDMS schema validation is the second net."""
    from pipeline.config import _parse_date
    from pipeline.schedule_utils import parse_report_times

    problems = []
    if not str(row.get("tenant", "")).strip():
        problems.append("tenant is empty")
    if not str(row.get("state_name", "")).strip():
        problems.append("state_name is empty")
    for field in ("campaign_start", "campaign_end"):
        if not _parse_date(row.get(field, "")):
            problems.append(f"{field} unparseable: {row.get(field)!r}")
    if not (parse_report_times(row.get("report_times", ""))
            or parse_report_times(row.get("partner_report_times", ""))):
        problems.append(
            f"no valid time in report_times {row.get('report_times')!r} "
            f"or partner_report_times {row.get('partner_report_times')!r}")
    return problems


def plan_sync(sheet_rows, existing_entries, group_name):
    """Pure diff: what must change in MDMS to mirror the sheet tab.

    Returns {"create": [data], "update": [(entry, data)], "deactivate": [entry],
             "unchanged": int, "rejected": [(identity, problems)],
             "skip_deactivation": bool}

    Edge cases encoded here:
      - duplicate identity in the sheet -> first row wins, later ones rejected
      - invalid row -> rejected; an EXISTING entry for that identity is kept
        as last-known-good (a typo must not kill a running campaign's config)
      - identity vanished from the sheet -> deactivate its entry
      - empty sheet read -> deactivation SKIPPED entirely (a transient empty
        read must not wipe the mirror), flagged for alerting
      - previously deactivated identity returns -> update (reactivates)
      - active=FALSE rows still sync: that column is campaign data the
        scheduler interprets; MDMS isActive only means "row exists on sheet"
    """
    by_identity = {}
    for entry in existing_entries:
        data = entry.get("data") or {}
        if data.get("deploymentGroup") == group_name and data.get("rowIdentity"):
            by_identity[data["rowIdentity"]] = entry

    plan = {"create": [], "update": [], "deactivate": [],
            "unchanged": 0, "rejected": [], "skip_deactivation": False}
    seen = set()

    for raw in sheet_rows:
        row = normalize_row(raw)
        identity = row_identity(row)
        if identity in seen:
            plan["rejected"].append((identity, ["duplicate identity in sheet"]))
            continue
        seen.add(identity)

        problems = validate_row(row)
        if problems:
            plan["rejected"].append((identity, problems))
            continue

        data = {"rowIdentity": identity, "deploymentGroup": group_name, "row": row}
        existing = by_identity.get(identity)
        if existing is None:
            plan["create"].append(data)
        elif ((existing.get("data") or {}).get("row") != row
              or not existing.get("isActive", True)):
            plan["update"].append((existing, data))
        else:
            plan["unchanged"] += 1

    if not sheet_rows:
        plan["skip_deactivation"] = True
    else:
        for identity, entry in by_identity.items():
            if identity not in seen and entry.get("isActive", True):
                plan["deactivate"].append(entry)
    return plan


def search_entries(group=None):
    """All entries of our schema for the group's MDMS tenant (paginated),
    optionally filtered to the group. Same call shape as the platform's
    fetch_campaigns_from_mdms."""
    url = _base_url() + os.getenv("MDMS_SEARCH_ENDPOINT", "/mdms-v2/v2/_search")
    limit = int(os.getenv("MDMS_LIMIT", "500"))
    entries, offset = [], 0
    while True:
        r = requests.post(url, json={
            "RequestInfo": _request_info(),
            "MdmsCriteria": {"tenantId": _mdms_tenant(group),
                             "schemaCode": _schema_code(),
                             "limit": limit, "offset": offset},
        }, timeout=60)
        r.raise_for_status()
        page = r.json().get("mdms", [])
        entries.extend(page)
        if len(page) < limit:
            break
        offset += limit
    if group is not None:
        entries = [e for e in entries
                   if (e.get("data") or {}).get("deploymentGroup") == group.get("name")]
    return entries


def _write(action, body_mdms):
    url = f"{_base_url()}/mdms-v2/v2/_{action}/{_schema_code()}"
    r = requests.post(url, json={"RequestInfo": _request_info(),
                                 "Mdms": body_mdms}, timeout=60)
    r.raise_for_status()


def apply_sync(plan, group=None):
    """Execute a plan against MDMS. Continues past per-entry failures and
    raises one summary error at the end so the sync task alerts + retries
    (idempotent: a retry re-plans against the new mirror state)."""
    counts = {"created": 0, "updated": 0, "deactivated": 0,
              "unchanged": plan["unchanged"], "rejected": len(plan["rejected"])}
    failures = []

    for data in plan["create"]:
        try:
            _write("create", {"tenantId": _mdms_tenant(group),
                              "schemaCode": _schema_code(),
                              "data": data, "isActive": True})
            counts["created"] += 1
        except Exception as e:
            failures.append(f"create {data['rowIdentity']}: {e}")

    for existing, data in plan["update"]:
        try:
            _write("update", {**existing, "data": data, "isActive": True})
            counts["updated"] += 1
        except Exception as e:
            failures.append(f"update {data['rowIdentity']}: {e}")

    for entry in plan["deactivate"]:
        identity = (entry.get("data") or {}).get("rowIdentity", "?")
        try:
            _write("update", {**entry, "isActive": False})
            counts["deactivated"] += 1
        except Exception as e:
            failures.append(f"deactivate {identity}: {e}")

    for identity, problems in plan["rejected"]:
        log.warning(f"[mdms-sync] REJECTED {identity}: {'; '.join(problems)}")
    if plan["skip_deactivation"]:
        log.warning("[mdms-sync] sheet returned 0 rows — deactivation skipped "
                    "(a transient empty read must not wipe the mirror)")
    if failures:
        raise RuntimeError(f"MDMS sync had {len(failures)} failure(s): "
                           + " | ".join(failures[:5]))
    return counts


def sync_rows_to_mdms(group, sheet_rows):
    """One sync pass for one group (tab): read mirror, plan, apply."""
    if not _base_url():
        log.info("[mdms-sync] MDMS_URL not set — sync skipped")
        return None
    existing = search_entries(group)
    plan = plan_sync(sheet_rows, existing, group.get("name"))
    counts = apply_sync(plan, group)
    counts["rejected_details"] = [f"{identity}: {'; '.join(problems)}"
                                  for identity, problems in plan["rejected"]]
    log.info(f"[mdms-sync] {group.get('name')}: {counts}")
    return counts


def get_active_rows_from_mdms(group):
    """The scheduler's MDMS read path (DST_CONFIG_SOURCE=mdms): returns row
    dicts identical in shape to config.get_active_rows()."""
    if not _base_url():
        raise ValueError("DST_CONFIG_SOURCE=mdms but MDMS_URL is not set")
    rows = [(e.get("data") or {}).get("row", {})
            for e in search_entries(group) if e.get("isActive", True)]
    log.info(f"MDMS group '{group.get('name')}': {len(rows)} row(s) loaded")
    return rows
