"""stock.py — OPTIONAL stock / supply-chain stage (self-contained add-on).

Ported from the Airflow tree's pipeline/stock.py for this pre-Airflow copy:
no pipeline.core package here, so the ES scroll, the openpyxl styles and the
index-name derivation are inlined, and the stage uploads its own workbook.
NOTHING in the existing modules is required to change: run.py calls
stock.run(cfg) after cdd_sync (guarded, non-fatal), and report.py /
report_itn.py call build_stock_section(doc, cfg, ...) right before their
Conclusion when cfg["stock_data"] exists. Everything else is internal.

Feature switch (in-code default, same pattern as DUP_MATRIX_DEFAULT):
  1. STOCK_REPORT_DEFAULT below            (per checkout)
  2. the DST_STOCK_REPORT environment key  (per deployment, overrides 1)
Off = stock.run returns None immediately and no report output changes at all.

What it reports — the fleet has FOUR distinct stock data models and this
module detects which one the campaign's own documents follow:

1. SMC, Nigeria convention (auto-detected: additionalDetails.stockEntryType
   ISSUED/RETURNED x status ACCEPTED/REJECTED/IN_TRANSIT, both sides of every
   transfer recorded) — per-hop Sent / Accepted / Rejected triples at
   LGA x Health Facility x product grain, the exact shape of the fleet's
   existing NA/PL STOCK reports, plus balances.
2. SMC, Chad-like convention (eventType/reason, sender-side records only,
   upstream may be "Central Facility") — receipts at HF, HF dispatches to
   CDDs, CDD returns, damage/loss from BOTH the reason enum and Chad's
   additionalDetails.status. The "issued" leg auto-picks the side that
   actually carries the data (HF dispatches vs CDD receipts).
3. AZM (Kebbi convention) — same legs, but stock is BOTTLES of 30 doses:
   consumption = (SUCCESS + VISITED doses) / 30 and redose is not
   double-subtracted; bottle returns (unused/partial/wasted/empty) come from
   additionalDetails.
4. ITN/LLIN (Chad ITN model) — boundary-grain stock (bales, scans,
   received/returned/issued/wasted) merged with distribution from the task
   index (DISTRIBUTOR vs DISTRIBUTOR_REGISTRAR quantities, scanned/manual
   codes, duplicate bednet codes via scripted metrics).

Balances (canonical agg_stock_summary formulas):
    Stock at HF  = received-in + CDD-returns - issued-out - returned-upstream
                   - damaged - lost
    Stock at CDD = issued - used - returns  (never shown as a minus figure in
                   the doc: a negative means handovers were not recorded, and
                   the section says exactly that in plain words)

The Word section is written for three readers: the campaign manager (Supply
at a Glance), on-ground supervisors (Facilities to Restock First; Facilities
Not Recording Handovers), and the audit — always LAST — Stock Check per CDD.

Field facts this module relies on (do not "fix" them):
  - Data.facility* is always "me" and Data.transactingFacility* the
    counterparty — the transformer flips sender/receiver by direction.
  - Data.physicalCount is always a positive magnitude; direction comes only
    from eventType/reason.
  - On STOCK docs projectTypeId lives at Data.additionalDetails.projectTypeId
    (top-level on task docs). campaignNumber is top-level on both.
  - The stock date field is a per-deployment choice (DST_STOCK_DATE_FIELD):
    createdTime/dateOfEntry are epoch ms, @timestamp is ISO, taskDates is
    YYYY-MM-DD — the range clause must match the value format.

The date window is CUMULATIVE-TO-DATE, not the report day: a balance computed
from one day's movements is meaningless. With a campaign identifier the query
has no lower bound (pre-campaign pre-positioning belongs in the balance);
without one it falls back to campaign_start so a shared tenant cannot bleed a
previous campaign's stock into this report.

run(cfg) returns the workbook path on success and None on the no-op (flag
off, or zero stock documents matched).
"""
import logging
import os
from datetime import datetime, timezone

import requests
import urllib3
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

urllib3.disable_warnings()
log = logging.getLogger(__name__)

# ── feature switches (in-code defaults, env overrides) ────────────────────────
STOCK_REPORT_DEFAULT = "FALSE"          # flip to "TRUE" to enable per checkout
STOCK_DATE_FIELD_DEFAULT = "createdTime"


def _flag_on():
    val = (os.getenv("DST_STOCK_REPORT", "").strip() or STOCK_REPORT_DEFAULT)
    return val.strip().upper() in ("TRUE", "YES", "1", "Y", "ON")


def _date_field(cfg):
    return (str(cfg.get("stock_date_field", "")).strip()
            or os.getenv("DST_STOCK_DATE_FIELD", "").strip()
            or STOCK_DATE_FIELD_DEFAULT)


ITN_DRUG_TYPES = {"ITN", "LLIN"}
_DEFAULT_BOUNDARY = {
    "smc": ["state", "lga", "ward", "healthFacility"],
    "itn": ["pays", "province", "district"],
}
# boundaryHierarchy keys differ per deployment (NG: state/lga/ward/
# healthFacility; Chad: country/province/district/HEALTHFACILITY). When no
# explicit override is configured, the levels are DETECTED from a probe doc
# and ordered by this list.
_LEVEL_ORDER = ["country", "state", "pays", "province", "region", "district",
                "lga", "county", "ward", "village", "healthFacility",
                "HEALTHFACILITY"]
_DAMAGED_REASONS = ["DAMAGED_IN_STORAGE", "DAMAGED_IN_TRANSIT"]
_LOST_REASONS = ["LOST_IN_STORAGE", "LOST_IN_TRANSIT"]
_TERMS_SIZE = 2000          # explicit on every terms agg — ES caps at 10 otherwise
_TIMEOUT = 120
AZM_DOSES_PER_BOTTLE = 30   # the Kebbi AZM reports' bottle conversion
_ADDL = "Data.additionalDetails"

_TASK_SUFFIX = "project-task-index-v1"


def _stock_index(cfg):
    """Derive the stock index name from the task index key, preserving the
    deployment's prefix convention without touching config.py."""
    task_index = cfg["ES_INDEX_TASK"]
    if not task_index.endswith(_TASK_SUFFIX):
        raise ValueError(f"unexpected task index name {task_index!r}")
    prefix = task_index[:-len(_TASK_SUFFIX)]
    return f"{prefix}stock-index-v1"


def _variant(cfg):
    return "itn" if cfg.get("drug_type") in ITN_DRUG_TYPES else "smc"


def _boundary_levels(cfg, probe_index=None):
    raw = (str(cfg.get("stock_boundary_levels", "")).strip()
           or os.getenv("DST_STOCK_BOUNDARY_LEVELS", "").strip())
    if raw:
        return [b.strip() for b in raw.split(",") if b.strip()]
    if probe_index:
        detected = _detect_boundary_levels(cfg, probe_index)
        if detected:
            log.info(f"  [stock] boundary levels detected from data: {detected}")
            return detected
    return _DEFAULT_BOUNDARY[_variant(cfg)]


def _detect_boundary_levels(cfg, index):
    """Read one matching doc's boundaryHierarchy and order its keys by the
    known hierarchy order. Returns None when nothing matched."""
    try:
        body = {"size": 1, "_source": ["Data.boundaryHierarchy"],
                "query": {"bool": {"must": _stock_must(cfg)}}}
        data = _search(cfg, index, body, "boundary probe")
        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            return None
        keys = set((hits[0]["_source"].get("Data", {})
                    .get("boundaryHierarchy") or {}).keys())
        return [k for k in _LEVEL_ORDER if k in keys] or None
    except Exception as e:                                       # noqa: BLE001
        log.warning(f"  [stock] boundary probe failed (using defaults): {e}")
        return None


def _stock_xlsx(cfg):
    if cfg.get("stock_xlsx"):
        return cfg["stock_xlsx"]
    name = ("stock_cumulative.xlsx" if cfg.get("cumulative")
            else f"stock_day{cfg['DAY']}.xlsx")
    return os.path.join(cfg["out_dir"], name)


def _epoch_ms(iso_ts):
    return int(datetime.strptime(iso_ts, "%Y-%m-%dT%H:%M:%S.%fZ")
               .replace(tzinfo=timezone.utc).timestamp() * 1000)


# ── query building ────────────────────────────────────────────────────────────

def _campaign_filters(cfg):
    """Campaign-scope clauses for the STOCK indices. Unlike the task index
    (where the daily date range isolates the campaign), stock movements span
    the whole campaign and often precede it, so the identifier is applied
    whenever the sheet provides one. projectTypeId sits under
    additionalDetails on stock docs — top-level would silently match nothing."""
    filters = []
    if cfg.get("is_admin_console") and cfg.get("campaign_number"):
        filters.append({"term": {"Data.campaignNumber.keyword": cfg["campaign_number"]}})
    elif cfg.get("project_type_id"):
        # projectTypeId sits TOP-LEVEL on some deployments' stock docs (Chad)
        # and under additionalDetails on others — match either.
        pid = cfg["project_type_id"]
        filters.append({"bool": {"minimum_should_match": 1, "should": [
            {"term": {"Data.projectTypeId.keyword": pid}},
            {"term": {"Data.additionalDetails.projectTypeId.keyword": pid}},
        ]}})
    if cfg.get("cycle_index"):
        filters.append({"term": {
            "Data.additionalDetails.cycleIndex.keyword": cfg["cycle_index"]}})
    return filters


def _task_campaign_filters(cfg):
    """Campaign-scope clauses for the TASK index (top-level fields there)."""
    filters = []
    if cfg.get("is_admin_console") and cfg.get("campaign_number"):
        filters.append({"term": {"Data.campaignNumber.keyword": cfg["campaign_number"]}})
    elif cfg.get("project_type_id"):
        filters.append({"term": {"Data.projectTypeId.keyword": cfg["project_type_id"]}})
    elif cfg.get("project_type"):
        filters.append({"term": {"Data.projectType.keyword": cfg["project_type"]}})
    if cfg.get("cycle_index"):
        filters.append({"term": {
            "Data.additionalDetails.cycleIndex.keyword": cfg["cycle_index"]}})
    return filters


def _range_clause(field, gte_iso, lte_iso):
    """Range clause whose value format matches the field's convention."""
    bounds = {}
    if field == "taskDates":
        if gte_iso:
            bounds["gte"] = gte_iso[:10]
        bounds["lte"] = lte_iso[:10]
    elif field == "@timestamp":
        if gte_iso:
            bounds["gte"] = gte_iso
        bounds["lte"] = lte_iso
    else:                                   # createdTime / dateOfEntry: epoch ms
        if gte_iso:
            bounds["gte"] = _epoch_ms(gte_iso)
        bounds["lte"] = _epoch_ms(lte_iso)
    return {"range": {f"Data.{field}": bounds}}


def _stock_must(cfg):
    filters = _campaign_filters(cfg)
    gte = None if filters else f"{cfg['campaign_start'].isoformat()}T00:00:00.000Z"
    return filters + [_range_clause(_date_field(cfg), gte, cfg["LTE"])]


def _task_must(cfg):
    gte = f"{cfg['campaign_start'].isoformat()}T00:00:00.000Z"
    return (_task_campaign_filters(cfg)
            + [_range_clause(cfg.get("task_date_field", "taskDates"),
                             gte, cfg["LTE"])])


# ── ES access (inlined — this tree has no pipeline.core) ──────────────────────

def _search(cfg, index, body, label):
    r = requests.post(f"{cfg['es_url']}/{index}/_search",
                      json=body, auth=cfg["es_auth"], verify=False,
                      timeout=_TIMEOUT)
    r.raise_for_status()
    log.info(f"  [stock] {label}: query ok")
    return r.json()


def _composite(cfg, index, must, sources, sub_aggs, label):
    """Paginated composite agg WITH sub-aggregations. missing_bucket keeps
    docs with an empty boundaryHierarchy — enrichment failures are silent and
    dropping those docs would silently understate every total."""
    buckets, after = [], None
    while True:
        comp = {"size": 1000, "sources": sources}
        if after:
            comp["after"] = after
        body = {"size": 0,
                "query": {"bool": {"must": must}},
                "aggs": {"combo": {"composite": comp, "aggs": sub_aggs}}}
        data = _search(cfg, index, body, label)
        page = data["aggregations"]["combo"]["buckets"]
        buckets.extend(page)
        after = data["aggregations"]["combo"].get("after_key")
        if not after:
            break
    log.info(f"  [stock] {label}: {len(buckets)} bucket(s)")
    return buckets


def _bsources(levels, extra=None):
    src = [{lvl: {"terms": {"field": f"Data.boundaryHierarchy.{lvl}.keyword",
                            "missing_bucket": True}}}
           for lvl in levels]
    for name, field in (extra or []):
        src.append({name: {"terms": {"field": field, "missing_bucket": True}}})
    return src


def _bkey(bucket_key, levels):
    return tuple(bucket_key.get(lvl) or "" for lvl in levels)


def _sum_agg(field="Data.physicalCount"):
    return {"qty": {"sum": {"field": field}}}


# ── SMC / AZM ─────────────────────────────────────────────────────────────────

def _smc_leg_composite(cfg, index, extra_must, levels, label):
    buckets = _composite(
        cfg, index, _stock_must(cfg) + extra_must,
        _bsources(levels, extra=[("product", "Data.productName.keyword")]),
        _sum_agg(), label)
    out = {}
    for b in buckets:
        key = _bkey(b["key"], levels) + (b["key"].get("product") or "",)
        out[key] = out.get(key, 0) + (b["qty"]["value"] or 0)
    return out


_ENTRY = f"{_ADDL}.stockEntryType.keyword"
_STATUS = f"{_ADDL}.status.keyword"


def _uses_entry_status(cfg, v1):
    """Nigeria encodes movements as additionalDetails.stockEntryType
    (ISSUED/RETURNED) x status (ACCEPTED/REJECTED/IN_TRANSIT) with BOTH sides
    of each transfer recorded; Chad uses eventType/reason with sender-side
    records only. Chad also carries stockEntryType but a different status
    vocabulary (IN_TRANSIT/RECEIVED/DAMAGED/LOST), so the probe requires
    ACCEPTED/REJECTED docs — and a meaningful SHARE of them, so a handful of
    stray docs from a mixed app version cannot flip the whole ledger."""
    body = {"size": 0, "track_total_hits": True,
            "query": {"bool": {"must": _stock_must(cfg)}},
            "aggs": {"acc_rej": {"filter": {"bool": {"must": [
                {"term": {_ENTRY: "ISSUED"}},
                {"terms": {_STATUS: ["ACCEPTED", "REJECTED"]}},
            ]}}}}}
    data = _search(cfg, v1, body, "convention probe")
    total = data.get("hits", {}).get("total", {}).get("value", 0)
    hits = data.get("aggregations", {}).get("acc_rej", {}).get("doc_count", 0)
    return hits >= 3 and total and (hits / total) >= 0.01


def _entry_triple(entry, prefix):
    """sent / accepted / rejected / in-transit sums for one stockEntryType.
    In-transit is READ from the docs' own status IN_TRANSIT, never derived
    as sent - accepted - rejected."""
    def filt(extra):
        return {"filter": {"bool": {"must": [{"term": {_ENTRY: entry}}] + extra}},
                "aggs": _sum_agg()}
    return {
        f"{prefix}_sent": filt([]),
        f"{prefix}_acc":  filt([{"term": {_STATUS: "ACCEPTED"}}]),
        f"{prefix}_rej":  filt([{"term": {_STATUS: "REJECTED"}}]),
        f"{prefix}_trans": filt([{"term": {_STATUS: "IN_TRANSIT"}}]),
    }


def _branch(cfg, v1, ftypes, name_field, sub, label):
    """One composite over a facilityType branch, keyed on the HEALTH FACILITY
    NAME (the NG reports' grain: state/staff branches carry the HF in
    transactingFacilityName, the HF branch in facilityName)."""
    sources = [
        {"hf": {"terms": {"field": f"Data.{name_field}.keyword",
                          "missing_bucket": True}}},
        {"product": {"terms": {"field": "Data.productName.keyword",
                               "missing_bucket": True}}},
    ]
    must = _stock_must(cfg) + [{"terms": {"Data.facilityType.keyword": ftypes}}]
    out = {}
    for b in _composite(cfg, v1, must, sources, sub, label):
        key = (b["key"].get("hf") or "", b["key"].get("product") or "")
        vals = out.setdefault(key, {})
        for name in sub:
            vals[name] = vals.get(name, 0) + (b[name]["qty"]["value"] or 0)
    return out


_NG_METRICS = ["state_sent", "state_acc", "state_rej", "state_trans",
               "iss_sent", "iss_acc", "iss_rej", "iss_trans",
               "sret_sent", "sret_acc", "sret_rej",
               "hret_sent", "hret_acc", "hret_rej"]


def _collect_smc_ng(cfg, v1, task):
    """Nigeria-convention ledger: per hop Sent / Accepted / Rejected triples,
    the exact shape of the fleet's existing STOCK reports (NA/PL)."""
    branch_state = _branch(cfg, v1, ["State Facility"],
                           "transactingFacilityName",
                           _entry_triple("ISSUED", "state"), "State->HF (NG)")
    branch_staff = _branch(cfg, v1, ["STAFF"], "transactingFacilityName",
                           _entry_triple("RETURNED", "sret"), "Staff->HF (NG)")
    hf_sub = {}
    hf_sub.update(_entry_triple("ISSUED", "iss"))
    hf_sub.update(_entry_triple("RETURNED", "hret"))
    branch_hf = _branch(cfg, v1, ["Health Facility", "WAREHOUSE", "Warehouse"],
                        "facilityName", hf_sub, "HF branch (NG)")

    # consumption / redose / LGA lookup from the task index at the same grain
    tsources = [
        {"lga": {"terms": {"field": "Data.boundaryHierarchy.lga.keyword",
                           "missing_bucket": True}}},
        {"hf": {"terms": {
            "field": "Data.boundaryHierarchy.healthFacility.keyword",
            "missing_bucket": True}}},
        {"product": {"terms": {"field": "Data.productName.keyword",
                               "missing_bucket": True}}},
    ]
    consumed, redose, lga_map = {}, {}, {}
    for status, agg, sink in (
            ("ADMINISTRATION_SUCCESS", _sum_agg("Data.quantity"), consumed),
            ("VISITED", {}, redose)):
        for b in _composite(
                cfg, task,
                _task_must(cfg) + [{"term": {
                    "Data.administrationStatus.keyword": status}}],
                tsources, agg, f"task {status} (NG)"):
            key = (b["key"].get("hf") or "", b["key"].get("product") or "")
            val = (b["qty"]["value"] or 0) if "qty" in b else b["doc_count"]
            sink[key] = sink.get(key, 0) + val
            if b["key"].get("lga"):
                lga_map[key[0]] = b["key"]["lga"]

    all_keys = (set(branch_state) | set(branch_staff) | set(branch_hf)
                | set(consumed) | set(redose))
    if not (branch_state or branch_staff or branch_hf):
        return None

    def m(source, key, name):
        return (source.get(key) or {}).get(name, 0)

    rows = []
    # Gross handover volume is no longer a printed column (it restarts the
    # "sent more than received" debate with every reviewer) but the report
    # metrics (6.1 give-back rate etc.) still need the total.
    gross_issued = 0
    for key in sorted(all_keys):
        hf, product = key
        vals = {}
        for name in _NG_METRICS:
            src = (branch_state if name.startswith("state")
                   else branch_staff if name.startswith("sret") else branch_hf)
            vals[name] = m(src, key, name)
        con = consumed.get(key, 0)
        red = redose.get(key, 0)
        # Custody rule (user, 2026-09-24 v3): the RECEIVER is accountable
        # for stock in transit, on every leg —
        #  - HF->CDD: sent stock counts with the CDDs from dispatch;
        #  - CDD->HF returns: count at the HF from dispatch;
        #  - HF->state returns: leave the HF book at dispatch;
        #  - a REJECTION bounces accountability back to the sender
        #    (CDD-rejected handovers -> HF; HF-rejected returns -> CDD;
        #    state-rejected returns -> HF).
        # Exception: state->HF in-transit sits on no facility balance (the
        # HF has not confirmed it; it shows only in its own column).
        # The Chad layout already works this way (sender-side records).
        ret_in = vals["sret_sent"] - vals["sret_rej"]    # returns on HF book
        ret_up = vals["hret_sent"] - vals["hret_rej"]    # returns off HF book
        net_given = (vals["iss_sent"] - vals["iss_rej"] - ret_in)
        balance_hf = (vals["state_acc"] + ret_in
                      - vals["iss_sent"] + vals["iss_rej"]
                      - ret_up)
        # Stock Left with CDDs is built on CONFIRMED receipts only (user,
        # 2026-09-24): in-transit stock sits in its own column — a
        # goods-in-transit bucket, counted in neither pocket until the CDD
        # confirms. Conservation: used + redose + Left-with-CDDs + Left-at-HF
        # + In-Transit(HF->CDD) + confirmed upstream returns = Received.
        balance_cdd = (net_given - vals["iss_trans"]) - (con + red)
        # Strict stock-journey order: state -> HF -> CDDs -> used -> returns,
        # computed outcomes (net + balances) last. In Transit is the docs'
        # OWN status IN_TRANSIT (this convention records it), not a formula.
        gross_issued += vals["iss_sent"]
        # "Received by CDD" as REAL DOSES, each counted once (the raw
        # accepted-handover counter exceeds Received on re-issued give-backs
        # and a percentage was rejected by the user): confirmed custody =
        # Stock Given minus what is still on the way. The row self-checks:
        # Stock Given = Received by CDD + In Transit.
        cdd_received = net_given - vals["iss_trans"]
        # Every printed column counts REAL doses and stays <= Received (the
        # conservation rule reviewers expect). The raw handover counters
        # (which exceed Received because returned stock goes out again) are
        # NOT printed — removed on feedback 2026-09-21 — but their gross sum
        # still feeds the report metrics via totals["issued"].
        # Column order rule (user, 2026-09-25): each balance CLOSES its own
        # block, so the row reads left-to-right with no cross-references —
        # CDD block: Given - rejected - in transit = Received by CDD, minus
        # used/redose = Stock Left with CDDs. THEN the return legs and the
        # facility balance. Returns must never sit between the CDD numbers
        # and the CDD balance (readers subtract them a second time).
        rows.append([lga_map.get(hf, ""), hf, product,
                     vals["state_sent"], vals["state_acc"], vals["state_rej"],
                     vals["state_trans"],                    # In Transit s->HF
                     net_given,                              # real doses out
                     vals["iss_rej"], vals["iss_trans"],
                     cdd_received,                           # confirmed doses
                     con, red,
                     balance_cdd,
                     vals["sret_sent"], vals["sret_acc"], vals["sret_rej"],
                     vals["hret_sent"], vals["hret_acc"], vals["hret_rej"],
                     balance_hf])

    headers = ["LGA", "Health Facility", "Product",
               "Sent by State to HF", "Received by HF from State",
               "Rejected by HF",
               "In Transit from State to HF (sent, not yet received)",
               "Stock Given to CDDs (each dose counted once)",
               "Rejected by CDD",
               "In Transit from HF to CDD (sent, not yet received)",
               "Received by CDD (each dose counted once)",
               "Used by CDD (administered)",
               "Redose (repeat dose after the first was spat out/vomited)",
               "Stock Left with CDDs",
               "Returned by CDD to HF", "Return Received by HF",
               "Return Rejected by HF",
               "Returned by HF to State", "Return Received by State",
               "Return Rejected by State",
               "Stock Left at HF"]
    totals = {
        "received":  sum(r[4] for r in rows),
        "issued":    gross_issued,
        # receiver-accountable: returns count from DISPATCH minus rejections
        # (sent - rejected on each leg), matching net_given/balance_hf so the
        # 6.1 reconciliation closes exactly
        "returned":  sum(r[14] - r[16] for r in rows),
        "returned_upstream": sum(r[17] - r[19] for r in rows),
        "rejected_in":  sum(r[5] for r in rows),
        "rejected_out": sum(r[8] for r in rows),
        "consumed":  sum(r[11] for r in rows),
        "redose":    sum(r[12] for r in rows),
        "damaged": 0, "lost": 0,
        "in_transit_out": sum(r[9] for r in rows),
        "balance_hf":  sum(r[20] for r in rows),
        "balance_cdd": sum(r[13] for r in rows),
    }
    rows.sort(key=lambda r: (r[0], r[1], r[2]))
    ix = {"lga": 0, "hf": 1, "product": 2,
          "received": 4, "issued": 7,
          "consumed": 11, "damaged": None, "lost": None,
          "bal_hf": 20, "bal_cdd": 13}
    return {"variant": "smc", "ng": True, "levels": ["LGA", "Health Facility"],
            "headers": headers, "rows": rows, "totals": totals, "ix": ix,
            "cdd_rows": _collect_cdd_accountability_ng(cfg, v1, task)}


def _collect_cdd_accountability_ng(cfg, v1, task):
    """NG accountability: 'received' = HF-branch ISSUED+ACCEPTED grouped by
    the staff name (transactingFacilityName = the CDD username per the
    transformer's STAFF rule); returns from the STAFF branch."""
    sources = [{"user": {"terms": {"field": "Data.transactingFacilityName.keyword"}}},
               {"product": {"terms": {"field": "Data.productName.keyword",
                                      "missing_bucket": True}}}]
    received = {}
    for b in _composite(
            cfg, v1,
            _stock_must(cfg) + [
                {"terms": {"Data.facilityType.keyword":
                           ["Health Facility", "WAREHOUSE", "Warehouse"]}},
                {"term": {_ENTRY: "ISSUED"}},
                {"term": {_STATUS: "ACCEPTED"}}],
            sources, _sum_agg(), "NG accountability received"):
        received[(b["key"]["user"], b["key"].get("product") or "")] = \
            b["qty"]["value"] or 0

    rsources = [{"user": {"terms": {"field": "Data.facilityName.keyword"}}},
                {"product": {"terms": {"field": "Data.productName.keyword",
                                       "missing_bucket": True}}}]
    sub = {"acc": {"filter": {"term": {_STATUS: "ACCEPTED"}},
                   "aggs": _sum_agg()},
           "unused":  {"sum": {"field": f"{_ADDL}.unused_quantity"}},
           "partial": {"sum": {"field": f"{_ADDL}.partial_quantity"}},
           "wasted":  {"sum": {"field": f"{_ADDL}.wastedBlistersReturned"}},
           "empty":   {"sum": {"field": f"{_ADDL}.emptyBottlesReturned"}}}
    returned, extras = {}, {}
    for b in _composite(
            cfg, v1,
            _stock_must(cfg) + [{"term": {"Data.facilityType.keyword": "STAFF"}},
                                {"term": {_ENTRY: "RETURNED"}}],
            rsources, sub, "NG accountability returns"):
        key = (b["key"]["user"], b["key"].get("product") or "")
        returned[key] = b["acc"]["qty"]["value"] or 0
        extras[key] = [b["unused"]["value"] or 0, b["partial"]["value"] or 0,
                       b["wasted"]["value"] or 0, b["empty"]["value"] or 0]

    consumed = {}
    tsources = [{"user": {"terms": {"field": "Data.userName.keyword"}}},
                {"product": {"terms": {"field": "Data.productName.keyword",
                                       "missing_bucket": True}}}]
    for b in _composite(
            cfg, task,
            _task_must(cfg) + [{"term": {
                "Data.administrationStatus.keyword": "ADMINISTRATION_SUCCESS"}}],
            tsources, _sum_agg("Data.quantity"), "NG accountability consumption"):
        consumed[(b["key"]["user"], b["key"].get("product") or "")] = \
            b["qty"]["value"] or 0

    rows = []
    for key in set(received) | set(returned) | set(consumed):
        rec = received.get(key, 0)
        con = consumed.get(key, 0)
        ret = returned.get(key, 0)
        ex = extras.get(key, [0, 0, 0, 0])
        rows.append([key[0], key[1], rec, con, ret, rec - con - ret] + ex
                    + [_high_negative_flag(rec, con, ret)])
    rows.sort(key=lambda r: abs(r[5]), reverse=True)
    return rows


def _high_negative_flag(received, consumed, returned):
    """Highlight CDDs whose difference is a LARGE negative — they used well
    beyond the stock recorded to their login. Same noise floor as the report
    tables: at least 20 units and 5% of the larger of got/used."""
    diff = received - consumed - returned
    return ("CHECK: high negative difference"
            if diff <= -20 and abs(diff) >= 0.05 * max(received, consumed, 1)
            else "")


def _collect_smc(cfg):
    v1 = _stock_index(cfg)
    task = cfg["ES_INDEX_TASK"]
    if _uses_entry_status(cfg, v1):
        log.info("  [stock] stockEntryType/status ACCEPTED convention detected "
                 "— using the NG sent/accepted/rejected leg set")
        return _collect_smc_ng(cfg, v1, task)
    levels = _boundary_levels(cfg, probe_index=v1)

    # Legs are defined by the reporting facility's OWN records, so they hold
    # across deployments (NG records both sides of a transfer; Chad records
    # only the sender side and its upstream is "Central Facility" rather than
    # "State Facility"):
    #   received  - receipts AT the HF (RECEIVED+RECEIVED), any upstream
    #               counterparty except STAFF
    #   issued    - the HF's own dispatch to a CDD (DISPATCHED, no reason,
    #               counterparty STAFF)
    #   returned  - the CDD's return record (facilityType STAFF, RETURNED)
    #   returned_upstream - the HF's return record (facilityType HF, RETURNED,
    #               counterparty not STAFF)
    _not_staff_ttype = {"bool": {"must_not": [
        {"term": {"Data.transactingFacilityType.keyword": "STAFF"}}]}}

    state_to_hf = _smc_leg_composite(cfg, v1, [
        {"term": {"Data.eventType.keyword": "RECEIVED"}},
        {"term": {"Data.reason.keyword": "RECEIVED"}},
        {"term": {"Data.facilityType.keyword": "Health Facility"}},
        _not_staff_ttype,
    ], levels, "Received at HF")

    # Issued to CDDs — deployments record this on DIFFERENT sides:
    #   Chad: only the HF's dispatch (DISPATCHED, no reason, counterparty STAFF)
    #   Kebbi AZM-style: only the CDD's receipt (facilityType STAFF, RECEIVED)
    # Query both sides and use whichever carries the volume; when both sides
    # of the same movement are recorded, using ONE side avoids double counting.
    issued_hf_side = _smc_leg_composite(cfg, v1, [
        {"term": {"Data.eventType.keyword": "DISPATCHED"}},
        {"term": {"Data.facilityType.keyword": "Health Facility"}},
        {"term": {"Data.transactingFacilityType.keyword": "STAFF"}},
        {"bool": {"must_not": [{"exists": {"field": "Data.reason"}}]}},
    ], levels, "Issued to CDD (HF dispatches)")
    issued_cdd_side = _smc_leg_composite(cfg, v1, [
        {"term": {"Data.eventType.keyword": "RECEIVED"}},
        {"term": {"Data.reason.keyword": "RECEIVED"}},
        {"term": {"Data.facilityType.keyword": "STAFF"}},
    ], levels, "Issued to CDD (CDD receipts)")
    if sum(issued_cdd_side.values()) > sum(issued_hf_side.values()):
        hf_to_cdd = issued_cdd_side
        log.info("  [stock] issued leg counted from CDD-side receipts")
    else:
        hf_to_cdd = issued_hf_side
        log.info("  [stock] issued leg counted from HF-side dispatches")

    hf_to_state = _smc_leg_composite(cfg, v1, [
        {"term": {"Data.facilityType.keyword": "Health Facility"}},
        {"term": {"Data.reason.keyword": "RETURNED"}},
        _not_staff_ttype,
    ], levels, "Returned upstream")

    cdd_to_hf = _smc_leg_composite(cfg, v1, [
        {"term": {"Data.facilityType.keyword": "STAFF"}},
        {"term": {"Data.transactingFacilityType.keyword": "Health Facility"}},
        {"term": {"Data.reason.keyword": "RETURNED"}},
    ], levels, "Returned by CDD")

    # Damage/loss lives in TWO places depending on deployment: the reason
    # enum (DAMAGED_IN_*/LOST_IN_*) or Chad's additionalDetails.status
    # (DAMAGED/LOST). Count both — the conventions are mutually exclusive
    # in practice.
    def _merged(a, b):
        out = dict(a)
        for k, qty in b.items():
            out[k] = out.get(k, 0) + qty
        return out

    damaged = _merged(
        _smc_leg_composite(cfg, v1, [
            {"terms": {"Data.reason.keyword": _DAMAGED_REASONS}}],
            levels, "damaged (reason)"),
        _smc_leg_composite(cfg, v1, [
            {"term": {_STATUS: "DAMAGED"}}], levels, "damaged (status)"))
    lost = _merged(
        _smc_leg_composite(cfg, v1, [
            {"terms": {"Data.reason.keyword": _LOST_REASONS}}],
            levels, "lost (reason)"),
        _smc_leg_composite(cfg, v1, [
            {"term": {_STATUS: "LOST"}}], levels, "lost (status)"))

    def _task_leg(statuses, agg, label):
        buckets = _composite(
            cfg, task,
            _task_must(cfg) + [{"terms": {
                "Data.administrationStatus.keyword": statuses}}],
            _bsources(levels, extra=[("product", "Data.productName.keyword")]),
            agg, label)
        out = {}
        for b in buckets:
            key = _bkey(b["key"], levels) + (b["key"].get("product") or "",)
            val = (b["qty"]["value"] or 0) if "qty" in b else b["doc_count"]
            out[key] = out.get(key, 0) + val
        return out

    # AZM stock is BOTTLES (30 doses each) and a redose pours from the same
    # bottle, so consumption = (SUCCESS + VISITED doses) / 30 — the Kebbi AZM
    # report's convention. SPAQ/others: consumption = SUCCESS doses; redose is
    # a separate count subtracted from the CDD balance.
    is_azm = cfg.get("drug_type") == "AZM"
    consumed_statuses = (["ADMINISTRATION_SUCCESS", "VISITED"] if is_azm
                         else ["ADMINISTRATION_SUCCESS"])
    cdd_to_bnf = _task_leg(consumed_statuses, _sum_agg("Data.quantity"),
                           "CDD->BNF")
    if is_azm:
        cdd_to_bnf = {k: round(qty / AZM_DOSES_PER_BOTTLE, 2)
                      for k, qty in cdd_to_bnf.items()}
    redose = _task_leg(["VISITED"], {}, "redose")

    all_keys = (set(state_to_hf) | set(hf_to_state) | set(hf_to_cdd)
                | set(cdd_to_hf) | set(damaged) | set(lost)
                | set(cdd_to_bnf) | set(redose))
    if not (state_to_hf or hf_to_cdd or hf_to_state or cdd_to_hf):
        return None      # zero stock movement documents matched — the no-op

    rows = []
    gross_issued = 0
    for key in sorted(all_keys):
        s2h = state_to_hf.get(key, 0); h2s = hf_to_state.get(key, 0)
        h2c = hf_to_cdd.get(key, 0);   c2h = cdd_to_hf.get(key, 0)
        c2b = cdd_to_bnf.get(key, 0);  red = redose.get(key, 0)
        dam = damaged.get(key, 0);     los = lost.get(key, 0)
        # AZM: redose doses are already inside consumption (same bottle), so
        # only SPAQ-style products subtract redose from the CDD balance.
        cdd_bal = (h2c - (c2b + c2h) if is_azm
                   else h2c - (c2b + red + c2h))
        # Strict stock-journey order: received -> given -> used -> returns ->
        # shrinkage, computed outcomes (net + balances) last.
        gross_issued += h2c
        # Every printed column counts REAL doses (see the NG layout comment);
        # the gross handover counter feeds totals["issued"] only.
        # block-closing order (see NG comment): CDD block then facility block
        rows.append(list(key) + [
            s2h,                                    # Received
            h2c - c2h,                              # real doses out
            c2b, red,
            cdd_bal,                                # Stock Left with CDDs
            c2h,                                    # Returned by CDDs
            h2s,                                    # Returned to state
            dam, los,
            # canonical balance (agg_stock_summary): damage/loss is real
            # shrinkage, not stock in hand
            (s2h + c2h) - (h2s + h2c) - dam - los,  # Stock at HF
        ])

    unit = "bottles" if is_azm else "doses"
    _redose_txt = "" if is_azm else " - redose"
    headers = (list(levels) + ["Product",
               "Received by HF",
               "Stock Given to CDDs (each dose counted once)",
               f"Used by CDDs ({unit})", "Redose",
               "Stock Left with CDDs",
               "Returned by CDDs to HF", "Returned by HF to State",
               "Damaged", "Lost",
               "Stock Left at HF"])
    n = len(levels) + 1
    totals = {
        "received":  sum(r[n] for r in rows),
        "issued":    gross_issued,
        "consumed":  sum(r[n + 2] for r in rows),
        "redose":    sum(r[n + 3] for r in rows),
        "returned":  sum(r[n + 5] for r in rows),
        "returned_upstream": sum(r[n + 6] for r in rows),
        "damaged":   sum(r[n + 7] for r in rows),
        "lost":      sum(r[n + 8] for r in rows),
        "balance_hf":  sum(r[n + 9] for r in rows),
        "balance_cdd": sum(r[n + 4] for r in rows),
    }
    # column index map for the audience-oriented report tables
    ix = {"lga": next((i for i, lvl in enumerate(levels)
                       if lvl.lower() in ("lga", "district")), None),
          "hf": len(levels) - 1, "product": len(levels),
          "received": n, "issued": n + 1,
          "consumed": n + 2, "damaged": n + 7, "lost": n + 8,
          "bal_hf": n + 9, "bal_cdd": n + 4}
    return {"variant": "smc", "levels": levels, "headers": headers,
            "rows": rows, "totals": totals, "ix": ix,
            "cdd_rows": _collect_cdd_accountability(cfg, v1, task)}


def _collect_cdd_accountability(cfg, v1_index, task_index):
    """Per user x product: received vs consumed vs returned (plus the
    unused/partial/wasted blister and empty-bottle quantities the SMC and AZM
    apps record under additionalDetails)."""
    sources = [{"user": {"terms": {"field": "Data.userName.keyword"}}},
               {"product": {"terms": {"field": "Data.productName.keyword",
                                      "missing_bucket": True}}}]
    sub = {
        "received": {"filter": {"bool": {"must": [
            {"term": {"Data.eventType.keyword": "RECEIVED"}},
            {"term": {"Data.reason.keyword": "RECEIVED"}}]}},
            "aggs": _sum_agg()},
        "returned": {"filter": {"term": {"Data.reason.keyword": "RETURNED"}},
                     "aggs": _sum_agg()},
        "unused":  {"sum": {"field": f"{_ADDL}.unused_quantity"}},
        "partial": {"sum": {"field": f"{_ADDL}.partial_quantity"}},
        "wasted":  {"sum": {"field": f"{_ADDL}.wastedBlistersReturned"}},
        "empty":   {"sum": {"field": f"{_ADDL}.emptyBottlesReturned"}},
    }
    buckets = _composite(
        cfg, v1_index,
        _stock_must(cfg) + [{"term": {"Data.facilityType.keyword": "STAFF"}}],
        sources, sub, "CDD accountability")

    is_azm = cfg.get("drug_type") == "AZM"
    consumed_statuses = (["ADMINISTRATION_SUCCESS", "VISITED"] if is_azm
                         else ["ADMINISTRATION_SUCCESS"])
    consumed = {}
    tsources = [{"user": {"terms": {"field": "Data.userName.keyword"}}},
                {"product": {"terms": {"field": "Data.productName.keyword",
                                       "missing_bucket": True}}}]
    for b in _composite(
            cfg, task_index,
            _task_must(cfg) + [{"terms": {
                "Data.administrationStatus.keyword": consumed_statuses}}],
            tsources, _sum_agg("Data.quantity"), "CDD consumption"):
        qty = b["qty"]["value"] or 0
        if is_azm:
            qty = round(qty / AZM_DOSES_PER_BOTTLE, 2)
        consumed[(b["key"]["user"], b["key"].get("product") or "")] = qty

    # Union with consumed-only users: a CDD who administered but has NO stock
    # documents at all must still appear (that is exactly the two-login case).
    stock_side = {}
    for b in buckets:
        key = (b["key"]["user"], b["key"].get("product") or "")
        stock_side[key] = [b["received"]["qty"]["value"] or 0,
                           b["returned"]["qty"]["value"] or 0,
                           b["unused"]["value"] or 0,
                           b["partial"]["value"] or 0,
                           b["wasted"]["value"] or 0,
                           b["empty"]["value"] or 0]

    rows = []
    for key in set(stock_side) | set(consumed):
        rec, ret, unused, partial, wasted, empty = stock_side.get(
            key, [0, 0, 0, 0, 0, 0])
        con = consumed.get(key, 0)
        rows.append([key[0], key[1], rec, con, ret, rec - con - ret,
                     unused, partial, wasted, empty,
                     _high_negative_flag(rec, con, ret)])
    rows.sort(key=lambda r: abs(r[5]), reverse=True)
    return rows


# ── ITN / LLIN ────────────────────────────────────────────────────────────────

_DUP_REDUCE = """
    Map m = new HashMap();
    for (s in states) {
        for (c in s) {
            m.put(c, m.getOrDefault(c, 0) + 1);
        }
    }
    int dup = 0;
    for (v in m.values()) {
        if (v > 1) dup += (v - 1);
    }
    return dup;
"""


def _dup_metric(lists):
    """Duplicate bednet-code counter over one or two additionalDetails list
    fields — copied from the proven Chad ITN report."""
    reads = "\n".join(
        f"""if (d.{fld} != null) {{ for (c in d.{fld}) {{ state.codes.add(c); }} }}"""
        for fld in lists)
    return {"scripted_metric": {
        "init_script": "state.codes = []",
        "map_script": f"""
            if (params._source.Data?.additionalDetails != null) {{
                def d = params._source.Data.additionalDetails;
                {reads}
            }}
        """,
        "combine_script": "return state.codes",
        "reduce_script": _DUP_REDUCE,
    }}


def _nested_boundary_aggs(levels, leaf_aggs):
    aggs = leaf_aggs
    for lvl in reversed(levels):
        aggs = {f"by_{lvl}": {
            "terms": {"field": f"Data.boundaryHierarchy.{lvl}.keyword",
                      "size": _TERMS_SIZE, "missing": "—"},
            "aggs": aggs}}
    return aggs


def _walk_buckets(agg_result, levels):
    def _rec(node, depth, prefix):
        lvl = levels[depth]
        for b in node[f"by_{lvl}"]["buckets"]:
            key = prefix + (b["key"],)
            if depth + 1 == len(levels):
                yield key, b
            else:
                yield from _rec(b, depth + 1, key)
    yield from _rec(agg_result["aggregations"], 0, ())


def _collect_itn(cfg):
    v1 = _stock_index(cfg)
    levels = _boundary_levels(cfg, probe_index=v1)

    stock_leaf = {
        "bales_quantity":    {"sum": {"field": f"{_ADDL}.balesQuantity"}},
        "actual_bale_scans": {"sum": {"field": f"{_ADDL}.actualBaleScans"}},
        "manual_bale_scans": {"sum": {"field": f"{_ADDL}.manualBaleScans"}},
        "stock_received": {"filter": {"bool": {"must": [
            {"term": {"Data.eventType.keyword": "RECEIVED"}},
            {"term": {"Data.reason.keyword": "RECEIVED"}}]}},
            "aggs": {"total": {"sum": {"field": "Data.physicalCount"}}}},
        "stock_returned": {"filter": {"bool": {"must": [
            {"term": {"Data.eventType.keyword": "RECEIVED"}},
            {"term": {"Data.reason.keyword": "RETURNED"}}]}},
            "aggs": {"total": {"sum": {"field": "Data.physicalCount"}}}},
        "stock_issued": {"filter": {"bool": {
            "must": [{"term": {"Data.eventType.keyword": "DISPATCHED"}}],
            "must_not": [{"exists": {"field": "Data.reason"}}]}},
            "aggs": {"total": {"sum": {"field": "Data.physicalCount"}}}},
        "stock_wasted": {"filter": {"bool": {"must": [
            {"term": {"Data.eventType.keyword": "DISPATCHED"}},
            {"exists": {"field": "Data.reason"}}]}},
            "aggs": {"total": {"sum": {"field": "Data.physicalCount"}}}},
    }
    stock_res = _search(cfg, v1, {
        "size": 0, "query": {"bool": {"must": _stock_must(cfg)}},
        "aggs": _nested_boundary_aggs(levels, stock_leaf)}, "ITN stock")

    dist_leaf = {
        "distributor": {"filter": {"term": {"Data.role.keyword": "DISTRIBUTOR"}},
                        "aggs": {"total": {"sum": {"field": "Data.quantity"}}}},
        "registrar": {"filter": {"term": {
            "Data.role.keyword": "DISTRIBUTOR_REGISTRAR"}},
            "aggs": {"total": {"sum": {"field": "Data.quantity"}}}},
        "manual_codes":  {"sum": {"field": f"{_ADDL}.manualCodes"}},
        "codes_scanned": {"sum": {"field": f"{_ADDL}.codesScanned"}},
        "dup_codes":   _dup_metric(["manualCodesList", "codesScannedList"]),
        "dup_manual":  _dup_metric(["manualCodesList"]),
        "dup_scanned": _dup_metric(["codesScannedList"]),
    }
    dist_res = _search(cfg, cfg["ES_INDEX_TASK"], {
        "size": 0, "query": {"bool": {"must": _task_must(cfg)}},
        "aggs": _nested_boundary_aggs(levels, dist_leaf)}, "ITN distribution")

    dist_map = {}
    for key, b in _walk_buckets(dist_res, levels):
        manual = b["manual_codes"]["value"] or 0
        scanned = b["codes_scanned"]["value"] or 0
        dist_map[key] = {
            "distributor": b["distributor"]["total"]["value"] or 0,
            "registrar":   b["registrar"]["total"]["value"] or 0,
            "manual_codes": manual, "codes_scanned": scanned,
            "actual_quantity": manual + scanned,
            "dup_codes":   b["dup_codes"]["value"] or 0,
            "dup_manual":  b["dup_manual"]["value"] or 0,
            "dup_scanned": b["dup_scanned"]["value"] or 0,
        }

    rows, seen = [], set()
    for key, b in _walk_buckets(stock_res, levels):
        seen.add(key)
        d = dist_map.get(key, {})
        rows.append(list(key) + [
            b["bales_quantity"]["value"] or 0,
            b["actual_bale_scans"]["value"] or 0,
            b["manual_bale_scans"]["value"] or 0,
            d.get("manual_codes", 0), d.get("codes_scanned", 0),
            d.get("actual_quantity", 0), d.get("dup_codes", 0),
            d.get("dup_manual", 0), d.get("dup_scanned", 0),
            b["stock_received"]["total"]["value"] or 0,
            b["stock_returned"]["total"]["value"] or 0,
            b["stock_issued"]["total"]["value"] or 0,
            b["stock_wasted"]["total"]["value"] or 0,
            d.get("distributor", 0), d.get("registrar", 0),
        ])
    for key, d in dist_map.items():
        if key not in seen:
            rows.append(list(key) + [
                0, 0, 0, d["manual_codes"], d["codes_scanned"],
                d["actual_quantity"], d["dup_codes"], d["dup_manual"],
                d["dup_scanned"], 0, 0, 0, 0,
                d["distributor"], d["registrar"]])
    if not rows:
        return None

    rows.sort(key=lambda r: tuple(r[:len(levels)]))
    headers = ([lvl.capitalize() for lvl in levels] + [
        "Bales Quantity", "Actual Bale Scans", "Manual Bale Scans",
        "Manual Codes", "Codes Scanned", "Actual Quantity Delivered",
        "Duplicate Bednet Codes", "Duplicate Manual Codes",
        "Duplicate Scanned Codes", "Stock Received", "Stock Returned",
        "Stock Issued", "Stock Wasted", "Distributor", "Registrar"])
    n = len(levels)
    totals = {
        "received": sum(r[n + 9] for r in rows),
        "returned": sum(r[n + 10] for r in rows),
        "issued":   sum(r[n + 11] for r in rows),
        "wasted":   sum(r[n + 12] for r in rows),
        "delivered": sum(r[n + 5] for r in rows),
        "dup_codes": sum(r[n + 6] for r in rows),
    }
    return {"variant": "itn", "levels": levels, "headers": headers,
            "rows": rows, "totals": totals, "cdd_rows": []}


# ── workbook (styles inlined — no pipeline.core here) ─────────────────────────

_thin = Side(border_style="thin", color="CCCCCC")
_BORDER = Border(left=_thin, right=_thin, top=_thin, bottom=_thin)
_BANNER_FILL = PatternFill("solid", fgColor="17365D")
_HDR_FILL = PatternFill("solid", fgColor="1A6496")
_TOTAL_FILL = PatternFill("solid", fgColor="EEEEEE")
_FLAG_FILL = PatternFill("solid", fgColor="FFC7CE")   # light red
_FLAG_COLOR = "9C0006"                                # dark red text
_TINT_FILL = PatternFill("solid", fgColor="DCE6F1")   # light blue — anchor col

# Hover notes on computed ledger headers: FORMULA ONLY (user rule 2026-09-24
# — no narrative in cell comments). Keyed by header prefix.
_LEDGER_HEADER_NOTES = {
    "Stock Given to CDDs": (
        "= Total Handovers - Rejected by CDD "
        "- (Returned by CDD to HF - Return Rejected by HF)"),
    "Received by CDD": (
        "= Stock Given to CDDs - In Transit from HF to CDD"),
    "Stock Left at HF": (
        "= Received by HF + (Returned by CDD to HF - Return Rejected by HF) "
        "- Total Handovers + Rejected by CDD "
        "- (Returned by HF to State - Return Rejected by State)"),
    "Stock Left with CDDs": (
        "= Received by CDD - Used by CDD - Redose"),
}


def _style_cell(cell, fill=None, bold=False, color=None, align="center", size=9):
    cell.border = _BORDER
    if fill:
        cell.fill = fill
    kwargs = {"bold": bold, "size": size, "name": "Calibri"}
    if color:
        kwargs["color"] = color
    cell.font = Font(**kwargs)
    cell.alignment = Alignment(horizontal=align, vertical="center", wrap_text=True)


# The 11th value on each CDD row (the high-negative-difference flag) is
# internal: it drives the red row fill here and the red rows in the Word
# audit table, but is not written as a column.
_CDD_HEADERS = ["CDD (user)", "Product", "Received", "Used", "Given Back",
                "Difference (= received - used - given back)",
                "Unused", "Partial", "Wasted", "Empty"]


def _write_tab(ws, banner, headers, rows, label_cols=3, flag_col=None,
               header_notes=None, tint_headers=()):
    """flag_col: 0-based row index whose non-empty value marks the whole row
    red (used for the CDD accountability flag). header_notes: {header prefix:
    note text} attached as hover comments on matching header cells.
    tint_headers: header prefixes whose whole column gets a light anchor
    tint (reading aid, e.g. 'Available at HF')."""
    from openpyxl.comments import Comment
    from openpyxl.utils import get_column_letter
    ws.append([banner] + [""] * (len(headers) - 1))
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=len(headers))
    _style_cell(ws.cell(row=1, column=1), fill=_BANNER_FILL, bold=True,
                color="FFFFFF", size=11)
    ws.append(headers)
    tint_cols = set()
    for ci in range(1, len(headers) + 1):
        _style_cell(ws.cell(row=2, column=ci), fill=_HDR_FILL, bold=True,
                    color="FFFFFF")
        header = str(headers[ci - 1])
        for prefix, note in (header_notes or {}).items():
            if header.startswith(prefix):
                ws.cell(row=2, column=ci).comment = Comment(note, "stock report")
        if any(header.startswith(p) for p in tint_headers):
            tint_cols.add(ci)
    for ci in range(1, len(headers) + 1):
        if ci <= label_cols:
            width = 26
        else:
            # headers carrying a formula get a wider, wrap-friendly column
            width = 22 if len(str(headers[ci - 1])) > 16 else 13
        ws.column_dimensions[get_column_letter(ci)].width = width
    for row in rows:
        # flag_col may point past the headers: such values colour the row
        # but are not written as a column.
        ws.append(row[:len(headers)])
        flagged = flag_col is not None and len(row) > flag_col and row[flag_col]
        for ci in range(1, len(headers) + 1):
            fill = (_FLAG_FILL if flagged
                    else _TINT_FILL if ci in tint_cols else None)
            _style_cell(ws.cell(row=ws.max_row, column=ci), fill=fill,
                        color=_FLAG_COLOR if flagged else None)


def _render_workbook(cfg, data, path):
    wb = Workbook()
    wb.remove(wb.active)
    period = (f"Cumulative Days 1-{cfg['DAY']}" if cfg.get("cumulative")
              else f"to Day {cfg['DAY']}")
    if data["variant"] == "smc":
        _write_tab(wb.create_sheet("STOCK LEDGER"),
                   f"{cfg['state_name']} — Stock Ledger ({period})  |  "
                   f"How to read: every column counts real doses. When the "
                   f"records are complete, no column exceeds 'Received' — "
                   f"where Used or Given IS higher, or a Stock Left number "
                   f"is negative, some handovers or receipts were not "
                   f"recorded in the app: a recording gap to follow up, not "
                   f"extra stock. CDDs return unused stock each evening; "
                   f"returns are in the Returned columns.",
                   data["headers"], data["rows"],
                   header_notes=_LEDGER_HEADER_NOTES,
                   tint_headers=("Stock Given to CDDs",))
        if data["cdd_rows"]:
            _write_tab(wb.create_sheet("CDD ACCOUNTABILITY"),
                       f"{cfg['state_name']} — CDD Stock Accountability "
                       f"({period})  |  Red rows: the CDD used more stock "
                       f"than was recorded as given to them — follow up "
                       f"with the supervisor. Flagged when the gap is at "
                       f"least 20 doses AND at least 5% of their use; "
                       f"smaller gaps are treated as timing noise.",
                       _CDD_HEADERS, data["cdd_rows"], flag_col=10)
    else:
        _write_tab(wb.create_sheet("STOCK & DISTRIBUTION"),
                   f"{cfg['state_name']} — Stock & Distribution ({period})",
                   data["headers"], data["rows"])
    ws = wb.worksheets[0]
    label_cols = len(data["levels"]) + (1 if data["variant"] == "smc" else 0)
    total_row = (["TOTAL"] + [""] * (label_cols - 1)
                 + [sum(r[ci] for r in data["rows"])
                    for ci in range(label_cols, len(data["headers"]))])
    ws.append(total_row)
    for ci in range(1, len(data["headers"]) + 1):
        _style_cell(ws.cell(row=ws.max_row, column=ci), fill=_TOTAL_FILL,
                    bold=True)
    wb.save(path)
    log.info(f"[stock] workbook saved -> {path}")
    return path


def _publish_workbook(cfg, path):
    """Upload the stock workbook to the campaign's Drive folder so the doc
    section can link it. Own upload so report.py/report_itn.py stay untouched.
    Non-fatal; respects no_upload."""
    if cfg.get("no_upload"):
        return ""
    try:
        from pipeline import notify
        fid = notify.campaign_folder_id(cfg)
        period = (f"Cumulative Days 1-{cfg['DAY']}" if cfg.get("cumulative")
                  else f"Day {cfg['DAY']}")
        link = notify.upload_file(
            path,
            f"{cfg['state_name']} {period} Stock Data — "
            f"{cfg['DATE_LABEL']} {datetime.now().strftime('%H:%M')}",
            folder_id=fid)
        cfg["stock_drive_link"] = link or ""
        return link or ""
    except Exception as e:                                       # noqa: BLE001
        log.warning(f"[stock] Drive upload failed (non-fatal): {e}")
        return ""


# ── Word section ──────────────────────────────────────────────────────────────
# Written for three readers:
#   the CAMPAIGN MANAGER / programme owner — "is supply healthy?" at a glance;
#   ON-GROUND supervisors — which exact facilities need restocking or a visit;
#   and the AUDIT (last) — per-CDD stock accountability for follow-up.

def _simple_table(doc, cols, data_rows, left_cols=(), bold_rows=(),
                  red_rows=()):
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.shared import RGBColor
    from pipeline.report import dat, hdr
    tbl = doc.add_table(rows=1, cols=len(cols))
    tbl.style = "Table Grid"
    for ci, h in enumerate(cols):
        hdr(tbl.cell(0, ci), h)
    for ri, row in enumerate(data_rows, 1):
        tr = tbl.add_row()
        for ci, val in enumerate(row):
            align = (WD_ALIGN_PARAGRAPH.LEFT if ci in left_cols
                     else WD_ALIGN_PARAGRAPH.CENTER)
            dat(tr.cells[ci], val, alt=(ri % 2 == 1), align=align)
            if ri in bold_rows or ri in red_rows:
                for p in tr.cells[ci].paragraphs:
                    for run in p.runs:
                        if ri in bold_rows:
                            run.font.bold = True
                        if ri in red_rows:
                            run.font.color.rgb = RGBColor(0x9C, 0x00, 0x06)


def _significant_diff(row):
    """The audit noise threshold: a CDD accountability difference matters
    when it is at least 20 units AND at least 5% of the larger of got/used."""
    return abs(row[5]) >= 20 and abs(row[5]) >= 0.05 * max(row[2], row[3], 1)


def stock_summary_line(cfg):
    """One-to-two-line deterministic stock summary for the report Conclusion
    and the Slack message (numbers straight from the totals, never through
    the LLM). Empty string when the stage produced nothing, so callers can
    append unconditionally. Internal audiences only."""
    data = cfg.get("stock_data")
    if not data:
        return ""
    t = data["totals"]

    def v(key):
        return t.get(key, 0) or 0

    is_azm = cfg.get("drug_type") == "AZM"
    unit = ("bottles" if is_azm
            else "bednets" if data["variant"] == "itn" else "doses")
    if data["variant"] == "itn":
        in_stock = v("received") - v("issued") + v("returned") - v("wasted")
        return (f"Stock: {v('received'):,.0f} {unit} received, "
                f"{v('delivered'):,.0f} delivered to households, "
                f"{in_stock:,.0f} in stock.")
    used_net = v("consumed") + (0 if is_azm else v("redose"))
    usage = (f", {used_net / v('received') * 100:.0f}% used"
             if v("received") else "")
    if v("balance_cdd") >= 0:
        position = (f"{v('balance_cdd'):,.0f} still with CDDs and "
                    f"{v('balance_hf'):,.0f} at facilities")
    else:
        position = (f"{v('balance_hf'):,.0f} at facilities; CDDs used "
                    f"{-v('balance_cdd'):,.0f} more than recorded handovers")
    line = (f"Stock: {v('received'):,.0f} {unit} received at health "
            f"facilities{usage}; {position}.")
    cdd = data.get("cdd_rows") or []
    if cdd:
        users = {r[0] for r in cdd}
        dirty = {r[0] for r in cdd if _significant_diff(r)}
        clean = len(users) - len(dirty)
        line += (f" {clean / len(users) * 100:.0f}% of CDDs have clean "
                 f"stock records.")
    return line


def _per_facility(data):
    """Aggregate the ledger rows to one entry per facility (across products)."""
    ix = data.get("ix") or {}
    if ix.get("hf") is None:
        return []
    out = {}
    for r in data["rows"]:
        hf = r[ix["hf"]]
        if not hf:
            continue
        e = out.setdefault(hf, {"lga": "", "consumed": 0, "received": 0,
                                "issued": 0, "bal_hf": 0, "bal_cdd": 0})
        if ix.get("lga") is not None and r[ix["lga"]]:
            e["lga"] = r[ix["lga"]]
        e["consumed"] += r[ix["consumed"]] or 0
        e["received"] += r[ix["received"]] or 0
        e["issued"] += r[ix["issued"]] or 0
        e["bal_hf"] += r[ix["bal_hf"]] or 0
        e["bal_cdd"] += r[ix["bal_cdd"]] or 0
    return [dict(hf=hf, **e) for hf, e in out.items()]


def build_stock_section(doc, cfg, heading_num="6"):
    """Append the stock section to a report doc — BOTH internal and partner
    (per user instruction 2026-09-17; note the audit table names CDD users).
    No-op when the stage produced nothing."""
    data = cfg.get("stock_data")
    if not data:
        return
    from pipeline.report import (GREY_RGB, _add_hyperlink, _two_col_table,
                                 add_heading, add_para)

    t = data["totals"]

    def v(key):
        return t.get(key, 0) or 0

    is_azm = cfg.get("drug_type") == "AZM"
    unit = ("bottles" if is_azm
            else "bednets" if data["variant"] == "itn" else "doses")

    add_heading(doc, f"{heading_num}.  Stock & Supply Chain Status", 4)
    add_para(doc, "All figures are cumulative for the campaign to date.",
             size=9, color=GREY_RGB)
    sub = 1

    # ── (manager view) supply at a glance: the STOCK-FLOW story ───────────
    # Rows follow the physical journey of the stock, so each level can only
    # shrink as you read down — a gross "handed to CDDs" figure exceeding
    # "received" (from daily give-back-and-reissue cycles) can never appear
    # here. Plain-space indentation only (no unicode symbols); every computed
    # row carries its formula in the label.
    add_heading(doc, f"{heading_num}.{sub}  Supply at a Glance", 5)
    if data["variant"] == "smc":
        net_issued = v("issued") - v("returned") - v("rejected_out")
        used_net = v("consumed") + (0 if is_azm else v("redose"))
        overview = [
            ("Received at health facilities", f"{v('received'):,.0f}"),
            ("    Given to CDDs (after give-backs)", f"{net_issued:,.0f}"),
            (f"        Used for children ({unit})", f"{v('consumed'):,.0f}"),
            ("        Repeat doses (redose)", f"{v('redose'):,.0f}"),
        ]
        if v("in_transit_out"):
            # in-transit sits in its own bucket (goods-in-transit): counted
            # with neither the CDDs nor the facility until confirmed
            overview.append(("        On the way to CDDs (in transit)",
                             f"{v('in_transit_out'):,.0f}"))
        if v("balance_cdd") >= 0:
            overview.append(("        Still with CDDs",
                             f"{v('balance_cdd'):,.0f}"))
        else:
            cov = (net_issued / used_net * 100) if used_net else 0
            overview += [
                ("        Still with CDDs", "—"),
                ("        Given without an app record (min.)",
                 f"{-v('balance_cdd'):,.0f}"),
                ("        Handovers recorded in the app", f"{cov:.1f}%"),
            ]
        overview += [
            ("    Still at health facilities", f"{v('balance_hf'):,.0f}"),
            ("    Returned by health facilities to the state health facility",
             f"{v('returned_upstream'):,.0f}"),
            ("    Damaged", f"{v('damaged'):,.0f}"),
            ("    Lost",    f"{v('lost'):,.0f}"),
        ]
        if v("rejected_in") or v("rejected_out"):
            overview += [
                ("    Rejected by health facilities",
                 f"{v('rejected_in'):,.0f}"),
                ("    Rejected by CDDs", f"{v('rejected_out'):,.0f}"),
            ]
        if v("received"):
            _redose_lbl = "" if is_azm else " + redose"
            overview.append((
                f"Stock usage (= used{_redose_lbl} / received)",
                f"{used_net / v('received') * 100:.1f}%"))
        if v("issued"):
            overview.append((
                "Give-back rate (= returned by CDDs / all handovers)",
                f"{v('returned') / v('issued') * 100:.1f}%"))
        all_cdd = data.get("cdd_rows") or []
        if all_cdd:
            users = {r[0] for r in all_cdd}
            dirty_users = {r[0] for r in all_cdd if _significant_diff(r)}
            clean = len(users) - len(dirty_users)
            overview.append((
                "CDDs with clean stock records",
                f"{clean:,} of {len(users):,} "
                f"({clean / len(users) * 100:.1f}%)"))
        # Consistency is still verified, just not shown as a table row: if
        # the flow does not add up to "received", say so in the log loudly.
        check_lhs = (net_issued + v("balance_hf") + v("returned_upstream")
                     + v("damaged") + v("lost"))
        diff = check_lhs - v("received")
        if abs(diff) >= 0.5:
            log.error(f"[stock] flow table does NOT reconcile: outflows+"
                      f"balances {check_lhs:,.0f} vs received "
                      f"{v('received'):,.0f} (difference {diff:,.0f})")
    else:
        overview = [
            ("Bednets received into stock",     f"{v('received'):,.0f}"),
            ("Bednets issued for distribution", f"{v('issued'):,.0f}"),
            ("Bednets returned to stock",       f"{v('returned'):,.0f}"),
            ("Bednets wasted",                  f"{v('wasted'):,.0f}"),
            ("Bednets delivered to households", f"{v('delivered'):,.0f}"),
            ("Duplicate bednet codes detected", f"{v('dup_codes'):,.0f}"),
        ]
    _two_col_table(doc, overview)
    notes = []
    if data["variant"] == "smc":
        if v("returned") > 0:
            notes.append("Given to CDDs counts stock after evening "
                         "give-backs; the every-handover counts are in the "
                         "Excel")
        if v("balance_cdd") < 0:
            notes.append("\"—\": CDDs used more than the recorded handovers "
                         "— a recording gap, not a stock shortage")
        notes.append("Still at facilities = received + give-backs - "
                     "handovers - sent back - damaged - lost")
        notes.append("Still with CDDs = given - used"
                     + ("" if is_azm else " - redose"))
        if is_azm:
            notes.append(f"1 bottle = {AZM_DOSES_PER_BOTTLE} doses")
    else:
        notes.append("In stock = received - issued + returned - wasted")
        notes.append("Delivered = scanned + manual codes")
    for note in notes:
        add_para(doc, note + ".", size=8, color=GREY_RGB)
    link_p = add_para(doc, "Detail per facility and product: ",
                      size=9, color=GREY_RGB)
    if cfg.get("stock_drive_link"):
        _add_hyperlink(link_p, "Stock Data ↗", cfg["stock_drive_link"])
    else:
        link_p.add_run(cfg.get("stock_xlsx", "") or _stock_xlsx(cfg))
    doc.add_paragraph()
    sub += 1

    facilities = _per_facility(data) if data["variant"] == "smc" else []
    day = max(1, int(cfg.get("DAY") or 1))

    # ── (on-ground view) restock list, or leftover list after the campaign ─
    # ONLY facilities that actually record stock in the app belong here: a
    # facility whose consumption comes solely from the task index has a
    # meaningless "balance" of 0 and would wrongly top the restock list —
    # those facilities are the RECORDING problem shown in the next section.
    recording = [f for f in facilities
                 if f["consumed"] > 0 and (f["received"] + f["issued"]) > 0]
    campaign_over = bool(cfg.get("cumulative")) or (
        day >= int(cfg.get("campaign_days") or day))
    if recording and campaign_over:
        # "Restock now" makes no sense once the campaign has ended — what
        # matters then is WHERE the unused stock sits, for retrieval.
        leftovers = sorted(
            (f for f in recording
             if f["bal_hf"] > 0 or f["bal_cdd"] > 0),
            key=lambda f: f["bal_hf"] + max(f["bal_cdd"], 0),
            reverse=True)[:5]
        if leftovers:
            add_heading(doc, f"{heading_num}.{sub}  Facilities With Stock "
                             f"Left Over", 5)
            add_para(doc, "The campaign has ended; this stock should be "
                          "returned or accounted for. Total left = at "
                          "facility + with CDDs.",
                     size=8, color=GREY_RGB)
            _simple_table(
                doc, ["#", "LGA / District", "Health Facility",
                      "At facility", "With CDDs", "Total left"],
                [[ri, f["lga"], f["hf"], f"{f['bal_hf']:,.0f}",
                  f"{max(f['bal_cdd'], 0):,.0f}",
                  f"{f['bal_hf'] + max(f['bal_cdd'], 0):,.0f}"]
                 for ri, f in enumerate(leftovers, 1)],
                left_cols=(1, 2))
            link_p = add_para(doc, "For more details, all facilities and "
                                   "products: ",
                              size=8, color=GREY_RGB)
            if cfg.get("stock_drive_link"):
                _add_hyperlink(link_p, "Stock Data ↗ (STOCK LEDGER tab)",
                               cfg["stock_drive_link"])
            else:
                link_p.add_run(cfg.get("stock_xlsx", "") or _stock_xlsx(cfg))
            doc.add_paragraph()
            sub += 1
    elif recording:
        for f in recording:
            f["daily"] = f["consumed"] / day
            f["days_left"] = (f["bal_hf"] / f["daily"]
                              if f["bal_hf"] > 0 else 0.0)
        at_risk = sorted(recording, key=lambda f: f["days_left"])[:10]
        add_heading(doc, f"{heading_num}.{sub}  Facilities to Restock First",
                    5)
        add_para(doc, "Facilities that record stock in the app, ranked by "
                      "days of stock left. Restock anything under 1 day.",
                 size=9, color=GREY_RGB)
        add_para(doc, f"Days of stock left = stock in hand / used per day.  "
                      f"Used per day = total used / {day} day(s).",
                 size=8, color=GREY_RGB)
        rows = []
        for ri, f in enumerate(at_risk, 1):
            flag = ("RESTOCK NOW" if f["days_left"] < 1
                    else "LOW" if f["days_left"] < 2 else "OK")
            rows.append([ri, f["lga"], f["hf"], f"{f['bal_hf']:,.0f}",
                         f"{f['daily']:,.0f}", f"{f['days_left']:.1f}", flag])
        _simple_table(doc, ["#", "LGA / District", "Health Facility",
                            "Stock in hand", f"Used per day ({unit})",
                            "Days of stock left", "Action"],
                      rows, left_cols=(1, 2))
        doc.add_paragraph()
        sub += 1

    # (The "Facilities Not Recording Handovers" section was removed on partner
    # feedback 2026-09; the per-CDD audit below covers the same signal.)

    # ── (audit — always LAST) per-CDD stock check ──────────────────────────
    # Noise threshold: a difference matters when it is at least 20 units and
    # at least 5% of the larger of got/used.
    cdd_rows = [r for r in data.get("cdd_rows", []) if _significant_diff(r)]
    if cdd_rows:
        add_heading(doc, f"{heading_num}.{sub}  Stock Check per CDD (audit)",
                    5)
        add_para(doc, "Largest differences first, for follow-up visits. "
                      "Positive: stock not yet accounted for. Negative: used "
                      "more than recorded — often an unrecorded handover, or "
                      "stock shared between CDDs under one name.",
                 size=9, color=GREY_RGB)
        add_para(doc, "Difference = Got - Used - Gave back. Rows in red: the "
                      "CDD used more stock than was recorded as given to "
                      "them (high negative difference).",
                 size=8, color=GREY_RGB)
        # pick the 5 biggest variances (collector sorts by |difference|),
        # then DISPLAY them in plain descending order of the difference so
        # the column reads sorted.
        top = sorted(cdd_rows[:5], key=lambda r: r[5], reverse=True)
        red_rows = tuple(ri for ri, r in enumerate(top, 1)
                         if len(r) > 10 and r[10])
        _simple_table(doc, ["#", "Distributor", "Product", "Got",
                            f"Used ({unit})", "Gave back", "Difference"],
                      [[ri, r[0], r[1], f"{r[2]:,.0f}", f"{r[3]:,.0f}",
                        f"{r[4]:,.0f}", f"{r[5]:,.0f}"]
                       for ri, r in enumerate(top, 1)],
                      left_cols=(1, 2), red_rows=red_rows)
        link_p = add_para(doc, f"All {len(cdd_rows)} CDDs with a difference: ",
                          size=8, color=GREY_RGB)
        if cfg.get("stock_drive_link"):
            _add_hyperlink(link_p, "Stock Data ↗ (CDD ACCOUNTABILITY tab)",
                           cfg["stock_drive_link"])
        else:
            link_p.add_run(cfg.get("stock_xlsx", "") or _stock_xlsx(cfg))
    doc.add_paragraph()


# ── stage entry point ─────────────────────────────────────────────────────────

def run(cfg):
    """Collect stock data, write + upload the workbook, stash
    cfg['stock_data'] / cfg['stock_xlsx'] for the report section.

    Returns the workbook path on success, None on the no-op (feature off, or
    zero stock documents matched). Never raises past the caller's guard on
    purpose-built data problems — callers wrap it non-fatally anyway."""
    if not _flag_on() and not cfg.get("stock_report"):
        log.info("[stock] stock report disabled (DST_STOCK_REPORT / "
                 "STOCK_REPORT_DEFAULT) — skipped")
        return None
    log.info(f"[stock] {cfg['state_name']} {_variant(cfg).upper()} stock "
             f"report (window to {cfg['LTE'][:10]}) ...")

    data = _collect_itn(cfg) if _variant(cfg) == "itn" else _collect_smc(cfg)
    if not data or not data["rows"]:
        log.error("[stock] zero stock documents matched — no stock section "
                  "this run")
        return None

    path = _stock_xlsx(cfg)
    cfg["stock_data"] = data
    cfg["stock_xlsx"] = path
    _render_workbook(cfg, data, path)
    _publish_workbook(cfg, path)
    return path
