"""cohort.py — OPTIONAL cohort report stage (all cycles per child).

One CSV row per child (individualId) with every cycle's administration packed
into ordered list columns, plus household linkage and status flags. Built for
end-of-campaign / cross-cycle analysis, so the window is ALWAYS
campaign_start -> extract time and NO cycleIndex filter is applied — the
whole point is every cycle side by side.

Same insert-only contract as pipeline/stock.py: run.py / scheduler.py call
cohort.run(cfg) guarded and non-fatally; with the flag off, run() returns
None immediately and nothing changes anywhere.

Feature switch (stock.py's DUP_MATRIX pattern):
  1. COHORT_REPORT_DEFAULT below           (per checkout)
  2. the DST_COHORT_REPORT environment key (per deployment, overrides 1)

Field facts this stage relies on:
  - age/gender sit top-level on NG task docs and under additionalDetails on
    Chad-like ones — both locations are read.
  - child name comes from the individual index (the task's additionalDetails
    name is not reliable across deployments; see the Name Resolution Rule).
  - beneficiary ID comes from the TASK doc (additionalDetails.beneficiaryId,
    backfilled across the child's docs), falling back to the individual
    index UNIQUE_BENEFICIARY_ID identifier where the task field is absent.
    Exported as stored — no decryption service call.
  - EVERY task is kept per cycle (revisits/redoses are separate rows in the
    lists), sorted by (cycle, date of administration).
  - boundary columns are built dynamically from the keys that actually appear
    (NG: country/state/lga/...; Chad-like: province/district/...).

Output: CSV parts of 500k rows (a cohort exceeds Excel's row limit and a
single DataFrame exhausts memory), written to cfg["out_dir"] and uploaded to
the campaign Drive folder. run(cfg) returns the list of paths, or None on
the no-op (flag off, or zero matching tasks).
"""
import csv
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
import urllib3

urllib3.disable_warnings()
log = logging.getLogger(__name__)

# ── feature switch ────────────────────────────────────────────────────────────
COHORT_REPORT_DEFAULT = "FALSE"          # flip to "TRUE" to enable per checkout


def _flag_on(cfg):
    val = (os.getenv("DST_COHORT_REPORT", "").strip() or COHORT_REPORT_DEFAULT)
    return (val.strip().upper() in ("TRUE", "YES", "1", "Y", "ON")
            or bool(cfg.get("cohort_report")))


# ── tuning ────────────────────────────────────────────────────────────────────
MAX_WORKERS = 10
BATCH_SIZE = 2000
CHUNK_SIZE = 1_000_000          # rows per CSV part
SCROLL_SIZE = 10000
_TIMEOUT = 120

# Known boundary levels in hierarchy order — the report emits, in this
# order, only the keys that actually appear in this campaign's data.
BOUNDARY_LEVEL_ORDER = [
    "country", "state", "pays", "province", "region", "administrativeProvince",
    "district", "lga", "county", "ward", "locality", "healthFacility",
    "community", "village",
]

STATUS_FLAGS = ["BENEFICIARY_INELIGIBLE", "BENEFICIARY_REFERRED",
                "BENEFICIARY_REFUSED", "CLOSED_HOUSEHOLD"]

# column labels for boundary keys (reference script's exact NG headers)
_BOUNDARY_LABELS = {
    "country": "Country", "state": "State", "pays": "Pays",
    "province": "Province", "region": "Region",
    "administrativeProvince": "Administrative Province",
    "district": "District", "lga": "LGA", "county": "County",
    "ward": "Ward", "locality": "Locality",
    "healthFacility": "Health Facility", "community": "Community",
    "village": "Village",
}

LIST_COLUMNS = ["Cycle Index List", "Age List", "Quantity Administered List",
                "Redose Quantity Administered List",
                "Date of Administration List"]


# ── campaign scope (ALL cycles — deliberately NO cycleIndex clause) ───────────

def _campaign_filters(cfg):
    filters = []
    if cfg.get("is_admin_console") and cfg.get("campaign_number"):
        filters.append({"term": {"Data.campaignNumber.keyword": cfg["campaign_number"]}})
    elif cfg.get("project_type_id"):
        filters.append({"term": {"Data.projectTypeId.keyword": cfg["project_type_id"]}})
    elif cfg.get("project_type"):
        filters.append({"term": {"Data.projectType.keyword": cfg["project_type"]}})
    return filters


def _task_must(cfg):
    field = cfg.get("task_date_field", "taskDates")
    gte = str(cfg["campaign_start"].isoformat())[:10]
    lte = cfg["LTE"]
    if field == "taskDates":
        bounds = {"gte": gte, "lte": lte[:10]}
    else:                       # @timestamp: ISO strings work directly
        bounds = {"gte": f"{gte}T00:00:00.000Z", "lte": lte}
    return _campaign_filters(cfg) + [{"range": {f"Data.{field}": bounds}}]


# ── ES access (self-contained, mirrors stock.py) ──────────────────────────────

def _post(cfg, url_path, body, label):
    r = requests.post(f"{cfg['es_url']}/{url_path}",
                      json=body, auth=cfg["es_auth"], verify=False,
                      timeout=_TIMEOUT)
    r.raise_for_status()
    return r.json()


def _scroll_pages(cfg, index, query, label):
    """Yield hit pages for a scrolled search."""
    data = _post(cfg, f"{index}/_search?scroll=10m", query, label)
    while True:
        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break
        yield hits
        scroll_id = data.get("_scroll_id")
        if not scroll_id:
            break
        data = _post(cfg, "_search/scroll",
                     {"scroll": "10m", "scroll_id": scroll_id}, label)


def _run_batches(cfg, items, worker, label):
    batches = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(worker, cfg, b) for b in batches]
        for n, future in enumerate(as_completed(futures), 1):
            results.append(future.result())
            if n % 25 == 0 or n == len(batches):
                log.info(f"  [cohort] {label}: {n}/{len(batches)} batches")
    return results


# ── step 1: project tasks ─────────────────────────────────────────────────────

def _convert_ts_to_date(ts):
    try:
        return datetime.fromtimestamp(
            int(ts) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:                                            # noqa: BLE001
        return ""


def _fetch_project_tasks(cfg, info_map, seen_boundary_keys):
    query = {
        "size": SCROLL_SIZE,
        "query": {"bool": {"must": _task_must(cfg) + [
            {"terms": {"Data.administrationStatus.keyword":
                       ["ADMINISTRATION_SUCCESS"] + STATUS_FLAGS}},
        ]}},
        "_source": [
            "Data.boundaryHierarchy", "Data.age", "Data.gender",
            "Data.additionalDetails.age", "Data.additionalDetails.gender",
            "Data.individualId", "Data.userName", "Data.quantity",
            "Data.administrationStatus", "Data.additionalDetails.reAdministered",
            "Data.taskDates", "Data.additionalDetails.dateOfAdministration",
            "Data.productName", "Data.additionalDetails.cycleIndex",
            "Data.additionalDetails.beneficiaryId",
        ],
    }
    total = 0
    for hits in _scroll_pages(cfg, cfg["ES_INDEX_TASK"], query, "cohort tasks"):
        for doc in hits:
            data = doc["_source"]["Data"]
            indv_id = data.get("individualId")
            if not indv_id:
                continue
            ad = data.get("additionalDetails") or {}
            boundary = data.get("boundaryHierarchy") or {}
            seen_boundary_keys.update(k for k, v in boundary.items() if v)

            if indv_id not in info_map:
                info = {
                    "_boundary": boundary,
                    "Username": data.get("userName", ""),
                    "Child Name": "",
                    "Gender": data.get("gender") or ad.get("gender") or "",
                    "Beneficiary ID (Child)": ad.get("beneficiaryId", ""),
                    "Individual ID": indv_id,
                    "Household Client Reference ID": "",
                    "Household Head Name": "",
                    "Product Name": data.get("productName", ""),
                    "Cycle Index List": [],
                    "Age List": [],
                    "Quantity Administered List": [],
                    "Redose Quantity Administered List": [],
                    "Date of Administration List": [],
                    "Date of Registration": data.get("taskDates", ""),
                }
                for flag in STATUS_FLAGS:
                    info[flag] = "no"
                info_map[indv_id] = info

            info = info_map[indv_id]
            # backfill the beneficiary id if an earlier doc lacked it
            if not info["Beneficiary ID (Child)"] and ad.get("beneficiaryId"):
                info["Beneficiary ID (Child)"] = ad["beneficiaryId"]
            cycle_index = ad.get("cycleIndex")
            if cycle_index is not None:
                quantity = data.get("quantity", 0) or 0
                re_administered = str(ad.get("reAdministered", "")).lower()
                info["Cycle Index List"].append(str(cycle_index).zfill(2))
                info["Age List"].append(
                    data.get("age") if data.get("age") not in (None, "")
                    else ad.get("age", ""))
                info["Date of Administration List"].append(
                    _convert_ts_to_date(ad.get("dateOfAdministration")))
                info["Quantity Administered List"].append(
                    quantity if data.get("administrationStatus") == "ADMINISTRATION_SUCCESS" else 0)
                info["Redose Quantity Administered List"].append(
                    quantity if re_administered == "true" else 0)

            status = data.get("administrationStatus", "")
            if status in STATUS_FLAGS:
                info[status] = "yes"

        total += len(hits)
        if total % 100000 < SCROLL_SIZE:
            log.info(f"  [cohort] tasks fetched: {total:,}")
    log.info(f"  [cohort] {total:,} tasks -> {len(info_map):,} individuals")
    return total


# ── step 2/3: enrichment lookups ──────────────────────────────────────────────

def _fetch_individuals_batch(cfg, batch):
    body = {"size": len(batch),
            "query": {"terms": {"clientReferenceId.keyword": batch}},
            "_source": ["clientReferenceId", "name", "identifiers"]}
    data = _post(cfg, f"{cfg['ES_INDEX_IND']}/_search", body, "individuals")
    return data.get("hits", {}).get("hits", [])


def _fetch_household_links_batch(cfg, batch):
    body = {"size": len(batch),
            "query": {"terms": {
                "Data.householdMember.individualClientReferenceId.keyword": batch}},
            "_source": ["Data.householdMember.householdClientReferenceId",
                        "Data.householdMember.individualClientReferenceId"]}
    data = _post(cfg, f"{cfg['ES_INDEX_HH_MEMBER']}/_search", body, "hh links")
    out = {}
    for doc in data.get("hits", {}).get("hits", []):
        src = doc["_source"]["Data"]["householdMember"]
        out[src["individualClientReferenceId"]] = src["householdClientReferenceId"]
    return out


def _fetch_household_heads_batch(cfg, batch):
    body = {"size": len(batch),
            "query": {"bool": {"must": [
                {"terms": {"Data.householdMember.householdClientReferenceId.keyword": batch}},
                {"term": {"Data.householdMember.isHeadOfHousehold": True}},
            ]}},
            "_source": ["Data.householdMember.householdClientReferenceId",
                        "Data.householdMember.individualClientReferenceId"]}
    data = _post(cfg, f"{cfg['ES_INDEX_HH_MEMBER']}/_search", body, "hh heads")
    out = {}
    for doc in data.get("hits", {}).get("hits", []):
        src = doc["_source"]["Data"]["householdMember"]
        out[src["householdClientReferenceId"]] = src["individualClientReferenceId"]
    return out


def _full_name(name_obj):
    return f"{name_obj.get('givenName', '')} {name_obj.get('familyName', '')}" \
        .replace("None", "").strip()


# ── step 4: sort + flatten + stream to CSV parts ─────────────────────────────

def _sort_cycle_data(info):
    cycles = info["Cycle Index List"]
    if not cycles:
        return
    combined = sorted(
        zip(cycles, info["Age List"], info["Quantity Administered List"],
            info["Redose Quantity Administered List"],
            info["Date of Administration List"]),
        key=lambda x: (int(x[0]), x[4] or ""))
    (info["Cycle Index List"], info["Age List"],
     info["Quantity Administered List"],
     info["Redose Quantity Administered List"],
     info["Date of Administration List"]) = map(list, zip(*combined))


def _write_parts(cfg, info_map, boundary_cols):
    final_columns = (
        [_BOUNDARY_LABELS[c] for c in boundary_cols]
        + ["Username", "Child Name", "Gender",
           "Beneficiary ID (Child)", "Individual ID",
           "Household Client Reference ID", "Household Head Name",
           "Product Name"]
        + LIST_COLUMNS
        + STATUS_FLAGS
        + ["Date of Registration"])

    paths = []
    file_index, row_count = 1, 0

    def open_part(idx):
        path = os.path.join(cfg["out_dir"], f"cohort_report_part{idx}.csv")
        f = open(path, "w", newline="", encoding="utf-8")
        w = csv.DictWriter(f, fieldnames=final_columns, extrasaction="ignore")
        w.writeheader()
        paths.append(path)
        return f, w

    current_file, writer = open_part(file_index)
    # export sorted by child name (reference script's ordering)
    ordered = sorted(info_map, key=lambda i: info_map[i]["Child Name"] or "")
    for indv_id in ordered:
        info = info_map[indv_id]
        _sort_cycle_data(info)
        row = dict(info)
        boundary = row.pop("_boundary", {}) or {}
        for k in boundary_cols:
            row[_BOUNDARY_LABELS[k]] = boundary.get(k, "")
        for col in LIST_COLUMNS:
            row[col] = " | ".join(str(v) for v in info[col])
        writer.writerow({c: row.get(c, "") for c in final_columns})
        row_count += 1
        if row_count == CHUNK_SIZE:
            current_file.close()
            log.info(f"  [cohort] part {file_index} complete ({row_count:,} rows)")
            file_index += 1
            row_count = 0
            current_file, writer = open_part(file_index)
    current_file.close()
    log.info(f"  [cohort] part {file_index} complete ({row_count:,} rows)")
    return paths


def _publish(cfg, paths):
    """Upload the CSV parts to the campaign's Drive folder. Non-fatal."""
    if cfg.get("no_upload"):
        return
    try:
        from pipeline import notify
        fid = notify.campaign_folder_id(cfg)
        period = (f"Cumulative Days 1-{cfg['DAY']}" if cfg.get("cumulative")
                  else f"Day {cfg['DAY']}")
        stamp = datetime.now().strftime("%H:%M")
        for i, path in enumerate(paths, 1):
            suffix = f" part {i}" if len(paths) > 1 else ""
            notify.upload_file(
                path,
                f"{cfg['state_name']} {period} Cohort Report{suffix} — "
                f"{cfg['DATE_LABEL']} {stamp}",
                folder_id=fid)
    except Exception as e:                                       # noqa: BLE001
        log.warning(f"[cohort] Drive upload failed (non-fatal): {e}")


# ── stage entry point ─────────────────────────────────────────────────────────

def run(cfg):
    """Build the all-cycles cohort CSV parts and upload them.

    Returns the list of written paths on success, None on the no-op (flag
    off, or zero matching tasks). Callers wrap it non-fatally."""
    if not _flag_on(cfg):
        log.info("[cohort] cohort report disabled (DST_COHORT_REPORT / "
                 "COHORT_REPORT_DEFAULT) — skipped")
        return None
    log.info(f"[cohort] {cfg['state_name']} cohort report "
             f"(all cycles, {cfg['campaign_start']} -> {cfg['LTE'][:10]}) ...")

    info_map = {}
    seen_boundary_keys = set()
    _fetch_project_tasks(cfg, info_map, seen_boundary_keys)
    if not info_map:
        log.error("[cohort] zero matching tasks — no cohort report this run")
        return None

    all_individuals = list(info_map.keys())

    log.info(f"  [cohort] enriching {len(all_individuals):,} children ...")
    for result in _run_batches(cfg, all_individuals, _fetch_individuals_batch,
                               "child info"):
        for doc in result:
            src = doc["_source"]
            info = info_map.get(src["clientReferenceId"])
            if not info:
                continue
            info["Child Name"] = _full_name(src.get("name", {}))
            # fallback only: the task-doc beneficiaryId is primary
            if not info["Beneficiary ID (Child)"]:
                for ident in src.get("identifiers", []):
                    if ident.get("identifierType") == "UNIQUE_BENEFICIARY_ID":
                        info["Beneficiary ID (Child)"] = ident.get("identifierId", "")
                        break

    child_to_household = {}
    for result in _run_batches(cfg, all_individuals,
                               _fetch_household_links_batch, "hh links"):
        child_to_household.update(result)

    hh_to_head = {}
    for result in _run_batches(cfg, list(set(child_to_household.values())),
                               _fetch_household_heads_batch, "hh heads"):
        hh_to_head.update(result)

    head_names = {}
    for result in _run_batches(cfg, list(set(hh_to_head.values())),
                               _fetch_individuals_batch, "head names"):
        for doc in result:
            src = doc["_source"]
            head_names[src["clientReferenceId"]] = _full_name(src.get("name", {}))

    for child_id in all_individuals:
        info = info_map[child_id]
        hh_id = child_to_household.get(child_id, "")
        info["Household Client Reference ID"] = hh_id
        head_ref = hh_to_head.get(hh_id, "")
        if head_ref:
            info["Household Head Name"] = head_names.get(head_ref, "")

    boundary_cols = [k for k in BOUNDARY_LEVEL_ORDER if k in seen_boundary_keys]
    paths = _write_parts(cfg, info_map, boundary_cols)
    _publish(cfg, paths)
    log.info(f"[cohort] done — {len(info_map):,} children, "
             f"{len(paths)} CSV part(s)")
    return paths
