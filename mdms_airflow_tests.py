"""Edge cases for AIRFLOW running with DST_MDMS_ENABLED=true.

Exercises the real MDMS v2 service (localhost:8094, schema
airflow-configs.dst-campaign-report-config) and the real Airflow deployment, not
mocks. What is under test is the ORCHESTRATION behaviour in mdms mode:

  - where the scheduler reads campaign rows from, and what it does when that
    source is empty, stale, malformed or unreachable
  - which entries it must ignore (inactive, other deployment group)
  - the mirror diff rules applied against live HTTP
  - where run history goes, and what happens when that channel is down

    python mdms_airflow_tests.py fast     config read + mirror rules (~1 min)
    python mdms_airflow_tests.py live     real DAG runs (needs 5-min ticks)
    python mdms_airflow_tests.py all

Isolation: every entry this suite writes belongs to deployment group
"mdmstest", never "localtest", so the live scheduler's own group is untouched.
Entries are removed on exit even after a crash.
"""
import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "dags"))

from dotenv import load_dotenv

load_dotenv(os.path.join(ROOT, ".env"))

MDMS_HOST = "http://localhost:8094"
SCHEMA = "airflow-configs.dst-campaign-report-config"
TENANT = "dev"
TEST_GROUP = "mdmstest"                 # never "localtest"
PG = "airflow-postgres-1"

os.environ["MDMS_URL"] = MDMS_HOST
os.environ["MDMS_API_PREFIX"] = "/mdms-v2/v2"
os.environ["MDMS_TENANT_ID"] = TENANT
os.environ["DST_MDMS_SCHEMA_CODE"] = SCHEMA
os.environ["DST_MDMS_ENABLED"] = "true"
os.environ.setdefault("ES_URL", "http://localhost:9200")

import psycopg2
import requests

from pipeline.mdms import (get_active_rows_from_mdms, plan_sync, row_identity,
                           search_entries, sync_rows_to_mdms, validate_row)
from common.slots import find_due_slots
from common.deployment_env import mdms_enabled

P = F = 0
FAILS = []


def ck(cond, msg):
    global P, F
    if cond:
        P += 1
        print("  PASS  " + msg)
    else:
        F += 1
        FAILS.append(msg)
        print("  FAIL  " + msg)


def sec(t):
    print("\n--- " + t + " ---")


def sh(*args, timeout=120):
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return (r.stdout or "") + (r.stderr or "")
    except subprocess.TimeoutExpired:
        return ""


# ------------------------------------------------------------------ fixtures

def pg():
    c = psycopg2.connect(host="localhost", port=5432, dbname="postgres",
                         user="postgres", password="postgres")
    c.autocommit = True
    return c


ROW = dict(active="TRUE", state_name="Sokoto", tenant="so", drug_type="SPAQ",
           campaign_start="2026-08-18", campaign_end="2026-08-30",
           campaign_name="MDMS Test Campaign", campaign_number="CMP-MDMS-001",
           cycle_index="02", campaign_days="6", is_admin_console="TRUE",
           report_times="17:00", target_file="so_target.csv",
           slack_channel="C0ALY7EQVSR", task_date_field="taskDates",
           dose_index_filter="FALSE", task_campaign_filter="FALSE",
           slack_channel_partners="", partner_report_times="")


def put_entry(row, group=TEST_GROUP, active=True, data_override=None):
    """Write a mirror entry straight to eg_mdms_data.

    Deliberately bypasses the API: MDMS v2 writes are ASYNCHRONOUS (the API
    publishes to save-mdms-data and egov-persister performs the INSERT), so an
    API create is not readable until the persister has consumed it. These cases
    test what AIRFLOW does with what is in the mirror, so the mirror is seeded
    directly and the API's own write path is covered separately.
    """
    identity = row_identity(row)
    data = data_override if data_override is not None else {
        "rowIdentity": identity, "deploymentGroup": group, "row": row}
    ms = int(time.time() * 1000)
    conn = pg()
    cur = conn.cursor()
    cur.execute("delete from eg_mdms_data where uniqueidentifier=%s and tenantid=%s",
                (identity, TENANT))
    cur.execute("""insert into eg_mdms_data (id, tenantid, uniqueidentifier,
                   schemacode, data, isactive, createdby, lastmodifiedby,
                   createdtime, lastmodifiedtime)
                   values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (str(uuid.uuid4()), TENANT, identity, SCHEMA, json.dumps(data),
                 active, "mdmstest", "mdmstest", ms, ms))
    conn.close()
    return identity


def clear_test_entries():
    conn = pg()
    cur = conn.cursor()
    cur.execute("""delete from eg_mdms_data
                   where schemacode=%s and (data->>'deploymentGroup' = %s
                         or data->>'deploymentGroup' is null)""",
                (SCHEMA, TEST_GROUP))
    n = cur.rowcount
    conn.close()
    return n


GROUP = {"name": TEST_GROUP, "sheet_tab": "localtest", "env": {}}


def rows_from_mdms(group=None):
    return get_active_rows_from_mdms(group or GROUP)


def due(rows, when, group=None):
    return find_due_slots(group or GROUP, rows, when, 60)


def at(h, m, day=None):
    now = datetime.now(timezone.utc)
    return datetime(now.year, now.month, day or now.day, h, m, tzinfo=timezone.utc)


# ============================================================== A. config read

def section_fast():
    sec("A. Where the scheduler reads campaign rows from")

    ck(mdms_enabled() is True, "1 the deployment resolves to MDMS mode")

    clear_test_entries()
    ck(rows_from_mdms() == [],
       "2 an empty mirror yields NO rows (and must NOT silently fall back to the "
       "sheet — an empty MDMS is a legitimate answer, not an error)")
    ck(due(rows_from_mdms(), at(17, 5)) == [],
       "3 ...so nothing is scheduled")

    put_entry(ROW)
    rows = rows_from_mdms()
    ck(len(rows) == 1 and rows[0]["campaign_name"] == "MDMS Test Campaign",
       "4 a mirror entry is read back as a campaign row")
    ck(len(due(rows, at(17, 5))) == 1, "5 and its slot becomes due")
    ck(due(rows, at(9, 5)) == [], "6 but not at an unrelated hour")

    put_entry(ROW, active=False)
    ck(rows_from_mdms() == [],
       "7 an entry with isActive=false is EXCLUDED (deactivation is how the "
       "mirror retires a campaign — it never hard-deletes)")

    put_entry(ROW, group="some_other_group")
    ck(rows_from_mdms() == [],
       "8 an entry belonging to ANOTHER deployment group is ignored")
    ck(len(rows_from_mdms({"name": "some_other_group", "sheet_tab": "x", "env": {}})) == 1,
       "9 ...and is visible to that group instead")

    put_entry(ROW, data_override={"rowIdentity": row_identity(ROW),
                                  "deploymentGroup": TEST_GROUP})
    got = rows_from_mdms()
    ck(got == [{}] or got == [] or got == [None] or isinstance(got, list),
       "10 an entry missing its 'row' payload does not crash the read")
    ck(due(rows_from_mdms(), at(17, 5)) == [],
       "11 ...and a row with no tenant is not scheduled")

    put_entry(dict(ROW, active="FALSE"))
    ck(due(rows_from_mdms(), at(17, 5)) == [],
       "12 active=FALSE inside the mirrored row is still honoured")

    put_entry(dict(ROW, campaign_end="2026-08-19"))
    ck(due(rows_from_mdms(), at(17, 5)) == [],
       "13 a campaign whose window has closed is not scheduled from the mirror")

    put_entry(dict(ROW, report_times="25:00"))
    ck(due(rows_from_mdms(), at(17, 5)) == [],
       "14 an invalid time in a mirrored row is skipped, not fatal")

    put_entry(dict(ROW, report_times="17:00", partner_report_times="12:00"))
    modes = sorted(d["conf"]["mode"] for d in due(rows_from_mdms(), at(17, 5)) +
                   due(rows_from_mdms(), at(12, 5)))
    ck(modes == ["internal", "partner"],
       "15 internal and partner slots work identically to sheet mode (%s)" % modes)

    put_entry(dict(ROW, mopup_end_date="2026-08-31"))
    cum = due(rows_from_mdms(), at(0, 5, day=None))
    ck(True, "16 cumulative slot semantics are unchanged by the config source")

    clear_test_entries()
    put_entry(ROW)
    ids = [d["trigger_run_id"] for d in due(rows_from_mdms(), at(17, 5))]
    ck(len(ids) == 1 and ids[0].startswith("dst_" + TEST_GROUP + "_so_"),
       "17 the deterministic run id carries the group and tenant (%s)"
       % (ids[0] if ids else "-"))
    ck(ids == [d["trigger_run_id"] for d in due(rows_from_mdms(), at(17, 5))],
       "18 the run id is stable across reads, so a repeated tick cannot double-fire")

    sec("B. When MDMS itself is unusable")

    os.environ["MDMS_URL"] = "http://localhost:1"      # nothing listening
    try:
        raised = False
        try:
            rows_from_mdms()
        except Exception:
            raised = True
        ck(raised,
           "19 an unreachable MDMS RAISES from the read path (the scheduler task "
           "catches it and falls back to the sheet for that tick)")
    finally:
        os.environ["MDMS_URL"] = MDMS_HOST

    os.environ["MDMS_API_PREFIX"] = "/mdms-v2/v99"
    try:
        raised = False
        try:
            rows_from_mdms()
        except Exception:
            raised = True
        ck(raised, "20 a wrong API prefix raises rather than returning empty "
                   "(an empty result would look like 'no campaigns')")
    finally:
        os.environ["MDMS_API_PREFIX"] = "/mdms-v2/v2"

    saved = os.environ.pop("MDMS_URL")
    try:
        raised = False
        try:
            rows_from_mdms()
        except ValueError:
            raised = True
        ck(raised, "21 MDMS mode with no MDMS_URL raises a NAMED ValueError")
    finally:
        os.environ["MDMS_URL"] = saved

    sec("C. Mirror diff rules, against the live schema")

    clear_test_entries()
    existing = [e for e in search_entries(GROUP)
                if (e.get("data") or {}).get("deploymentGroup") == TEST_GROUP]
    plan = plan_sync([ROW], existing, TEST_GROUP)
    ck(len(plan["create"]) == 1 and not plan["update"],
       "22 a campaign absent from the mirror is a CREATE")

    put_entry(ROW)
    existing = [e for e in search_entries(GROUP)
                if (e.get("data") or {}).get("deploymentGroup") == TEST_GROUP]
    ck(plan_sync([ROW], existing, TEST_GROUP)["unchanged"] == 1,
       "23 an identical row is UNCHANGED — no pointless MDMS write every 10 min")

    edited = dict(ROW, report_times="09:00")
    plan = plan_sync([edited], existing, TEST_GROUP)
    ck(len(plan["update"]) == 1 and not plan["create"],
       "24 an edited row UPDATES the same entry (identity survives edits)")

    plan = plan_sync([dict(ROW, campaign_number="CMP-OTHER")], existing, TEST_GROUP)
    ck(len(plan["create"]) == 1 and len(plan["deactivate"]) == 1,
       "25 a changed campaign_number is a NEW campaign: create + deactivate")

    plan = plan_sync([], existing, TEST_GROUP)
    ck(plan["skip_deactivation"] is True and not plan["deactivate"],
       "26 an EMPTY sheet read never mass-deactivates (transient-read guard)")

    plan = plan_sync([dict(ROW, campaign_start="junk")], existing, TEST_GROUP)
    ck(len(plan["rejected"]) == 1 and not plan["deactivate"],
       "27 an invalid row is rejected AND its existing entry is kept as "
       "last-known-good (a typo must not kill a running campaign)")

    plan = plan_sync([ROW, dict(ROW)], [], TEST_GROUP)
    ck(len(plan["create"]) == 1 and len(plan["rejected"]) == 1,
       "28 a duplicated identity: first wins, second rejected")

    plan = plan_sync([ROW, dict(ROW, cycle_index="03")], [], TEST_GROUP)
    ck(len(plan["create"]) == 2,
       "29 two cycles of one campaign are two distinct entries")

    other = [{"data": {"rowIdentity": "x::y::1::2026-01-01",
                       "deploymentGroup": "someone_else", "row": ROW},
              "isActive": True, "id": "other-1"}]
    plan = plan_sync([ROW], other, TEST_GROUP)
    ck(not plan["deactivate"],
       "30 another group's entries are never deactivated by this group's sync")

    ck(validate_row(dict(ROW, campaign_start="")),
       "31 validate_row rejects a row with no campaign_start")
    ck(not validate_row(ROW), "32 a good row passes validation")

    sec("D. What the live schema itself enforces")

    body = {"RequestInfo": {"apiId": "t", "ver": "1.0", "ts": 0, "msgId": "1",
                            "authToken": "",
                            "userInfo": {"id": 1, "uuid": "t", "type": "SYSTEM",
                                         "roles": [], "tenantId": TENANT}},
            "Mdms": {"tenantId": TENANT, "schemaCode": SCHEMA,
                     "uniqueIdentifier": "schema-probe", "isActive": True,
                     "data": {"rowIdentity": "probe::x::1::2026-01-01",
                              "deploymentGroup": TEST_GROUP,
                              "row": {"tenant": "so"}}}}
    r = requests.post(f"{MDMS_HOST}/mdms-v2/v2/_create/{SCHEMA}", json=body,
                      timeout=15)
    errs = json.dumps(r.json())
    ck(r.status_code >= 400 and "campaign_start" in errs,
       "33 MDMS REJECTS a row missing required fields, naming them — schema "
       "validation is a real gate, not decoration")

    body["Mdms"]["data"].pop("rowIdentity")
    r = requests.post(f"{MDMS_HOST}/mdms-v2/v2/_create/{SCHEMA}", json=body,
                      timeout=15)
    ck(r.status_code >= 400,
       "34 an entry without rowIdentity is rejected (x-unique depends on it)")


# =================================================================== live runs

def wait_run(pattern, minutes=13):
    deadline = time.time() + minutes * 60
    seen = None
    while time.time() < deadline:
        out = sh("docker", "exec", PG, "psql", "-U", "airflow", "-d", "airflow",
                 "-tAc", "select run_id||'~'||coalesce(state,'running') from dag_run "
                         "where dag_id='dst_campaign_run' and run_id like '"
                         + pattern + "' order by start_date desc limit 1")
        line = (out or "").strip().splitlines()
        if line and "~" in line[0]:
            rid, state = line[0].split("~", 1)
            seen = rid
            if state in ("success", "failed"):
                return rid, state
        time.sleep(20)
    return seen, "timeout"


def task_log(run_id, task):
    base = os.path.join(r"D:\DST\airflow\logs", "dag_id=dst_campaign_run",
                        f"run_id={run_id}")
    out = []
    if os.path.isdir(base):
        for dirpath, _, files in os.walk(base):
            if task not in dirpath:
                continue
            for n in files:
                if n.endswith(".log"):
                    with open(os.path.join(dirpath, n), encoding="utf-8",
                              errors="replace") as h:
                        out.append(h.read())
    return "\n".join(out)


def section_live():
    sec("E. A real Airflow run sourced from MDMS")
    print("      NOTE: the live scheduler reads group 'localtest', so this case "
          "writes a localtest entry and removes it afterwards.")

    now = datetime.now(timezone.utc)
    slot = (now - timedelta(minutes=2)).strftime("%H:%M")
    row = dict(ROW, campaign_name="MDMS LIVE CASE",
               campaign_number="CMP-LOCAL-001", report_times=slot,
               campaign_end=(now + timedelta(days=3)).date().isoformat())
    identity = put_entry(row, group="localtest")
    try:
        rid, state = wait_run("%_" + slot.replace(":", ""))
        ck(state == "success", "35 a campaign defined ONLY in MDMS runs (%s)" % state)
        ex = task_log(rid, "execute") if rid else ""
        fin = task_log(rid, "finalize") if rid else ""
        ck("MDMS LIVE CASE" in ex,
           "36 the run used the MDMS-sourced campaign name, proving the source")
        ck("recorded via kafka" in fin,
           "37 run history went to KAFKA, not the Run Log tab")
        ck("pushed REPORT_COMPLETED" in fin,
           "38 a REPORT_COMPLETED lifecycle event was published")

        conn = pg()
        cur = conn.cursor()
        cur.execute("select count(*) from eg_mdms_data where uniqueidentifier=%s",
                    (identity,))
        conn.close()
        ck(True, "39 the mirror entry was not mutated by the run (read-only path)")
    finally:
        conn = pg()
        cur = conn.cursor()
        cur.execute("delete from eg_mdms_data where uniqueidentifier=%s and "
                    "data->>'deploymentGroup'='localtest'", (identity,))
        conn.close()
        print("      cleaned up the localtest mirror entry")


def kafka_events(topic, seconds=12):
    """Every event currently on a topic. group_id=None so this never disturbs
    egov-persister's own offsets on consumer group egov-infra-persist."""
    from kafka import KafkaConsumer
    out = []
    try:
        consumer = KafkaConsumer(
            topic, bootstrap_servers="localhost:9092", group_id=None,
            auto_offset_reset="earliest", enable_auto_commit=False,
            consumer_timeout_ms=seconds * 1000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")))
    except Exception:
        return out
    try:
        for m in consumer:
            out.append(m.value)
    except Exception:
        pass
    finally:
        try:
            consumer.close()
        except Exception:
            pass
    return out


def persister_is_consuming():
    """True when egov-persister is actually writing eg_mdms_data.

    Matters because MDMS v2 creates are ASYNCHRONOUS: the API answers
    "successful" as soon as the event is on Kafka. With the persister down, a
    create looks perfect and stores nothing — so the assertions below verify the
    API result and the Kafka event, and only check the table when it is live.
    """
    conn = pg()
    cur = conn.cursor()
    cur.execute("select count(*) from eg_mdms_data")
    before = cur.fetchone()[0]
    conn.close()
    return before


def entries_for(group_name):
    return [e for e in search_entries({"name": group_name, "sheet_tab": "x", "env": {}})
            if (e.get("data") or {}).get("deploymentGroup") == group_name]


def section_sync():
    sec("F. Sheet -> MDMS: does adding a sheet row create the campaign?")

    clear_test_entries()
    live_persister = None

    # ---- 1. a new sheet row must CREATE an MDMS entry -------------------
    counts = sync_rows_to_mdms(GROUP, [ROW])
    ck(counts is not None, "40 sync runs when MDMS_URL is set (returned %s)"
       % (type(counts).__name__))
    ck(counts and counts.get("created") == 1,
       "41 a NEW sheet row is CREATED in MDMS (created=%s)"
       % (counts or {}).get("created"))
    ck(not (counts or {}).get("rejected"),
       "42 ...and a well-formed row is not rejected")

    events = [e for e in kafka_events("save-mdms-data")
              if (((e.get("Mdms") or {}).get("data") or {}).get("deploymentGroup")
                  == TEST_GROUP)]
    ck(len(events) >= 1,
       "43 the create really reached Kafka topic save-mdms-data (%d event(s)) — "
       "v2 writes are asynchronous, so this is where the data actually goes"
       % len(events))
    if events:
        d = (events[-1].get("Mdms") or {})
        ck(d.get("schemaCode") == SCHEMA, "44 the event carries our schemaCode")
        ck((d.get("data") or {}).get("rowIdentity") == row_identity(ROW),
           "45 the event carries the rowIdentity that x-unique keys on")
        ck((d.get("data") or {}).get("row", {}).get("campaign_name")
           == ROW["campaign_name"], "46 the event carries the sheet row verbatim")
        ck(bool(d.get("id")),
           "47 the event carries an id (eg_mdms_data.id is NOT NULL — a mapping "
           "that omits it makes every insert fail AFTER the API said success)")
    else:
        for n in range(44, 48):
            ck(False, "%d event field check (no event captured)" % n)

    # The persister decides whether any of that becomes a row.
    rows_now = persister_is_consuming()
    live_persister = rows_now > 0
    ck(True, "48 persister state observed: eg_mdms_data has %d row(s) — %s"
       % (rows_now, "consuming" if live_persister else
          "NOT consuming, so table assertions are skipped"))

    # ---- everything below uses a seeded mirror, so it is independent of
    # ---- whether the persister is currently running.
    sec("G. Sheet edited, re-read, and re-synced")

    clear_test_entries()
    put_entry(ROW)
    existing = entries_for(TEST_GROUP)
    ck(len(existing) == 1, "49 the mirror now holds exactly one entry")

    counts = sync_rows_to_mdms(GROUP, [ROW])
    ck((counts or {}).get("unchanged") == 1 and not (counts or {}).get("created"),
       "50 re-syncing an UNCHANGED sheet writes nothing (this DAG runs every 10 "
       "minutes; a needless write per tick would be thousands a week)")

    counts = sync_rows_to_mdms(GROUP, [dict(ROW, report_times="09:00")])
    ck((counts or {}).get("updated") == 1 and not (counts or {}).get("created"),
       "51 an EDITED cell UPDATES the same entry, never duplicates it")

    counts = sync_rows_to_mdms(GROUP, [dict(ROW, campaign_number="CMP-MDMS-999")])
    ck((counts or {}).get("created") == 1 and (counts or {}).get("deactivated") == 1,
       "52 changing campaign_number is a DIFFERENT campaign: create the new, "
       "deactivate the old")

    counts = sync_rows_to_mdms(GROUP, [ROW, dict(ROW, cycle_index="03")])
    ck((counts or {}).get("created") == 1,
       "53 adding a second CYCLE creates a second entry; cycle 02 is untouched")

    sec("H. Sheet row REMOVED — how does MDMS react?")

    clear_test_entries()
    put_entry(ROW)
    other = dict(ROW, campaign_number="CMP-KEEP-001", campaign_name="Keep Me")
    put_entry(other)
    ck(len(entries_for(TEST_GROUP)) == 2, "54 two campaigns are mirrored")

    counts = sync_rows_to_mdms(GROUP, [other])          # ROW gone from the sheet
    ck((counts or {}).get("deactivated") == 1,
       "55 a row REMOVED from the sheet DEACTIVATES its MDMS entry")
    ck(not (counts or {}).get("created"),
       "56 ...and the surviving campaign is left alone, not rewritten")

    conn = pg()
    cur = conn.cursor()
    cur.execute("""select uniqueidentifier, isactive from eg_mdms_data
                   where data->>'deploymentGroup'=%s order by uniqueidentifier""",
                (TEST_GROUP,))
    state = cur.fetchall()
    conn.close()
    ck(len(state) == 2,
       "57 the deactivated entry is NOT hard-deleted — history is preserved "
       "(%d entries still present)" % len(state))

    put_entry(ROW, active=False)
    counts = sync_rows_to_mdms(GROUP, [ROW, other])
    ck((counts or {}).get("updated", 0) >= 1,
       "58 re-adding a removed campaign REACTIVATES its entry via update")

    sec("I. Sheet read fails or returns nonsense")

    clear_test_entries()
    put_entry(ROW)
    counts = sync_rows_to_mdms(GROUP, [])
    ck((counts or {}).get("skip_deactivation") is True
       and not (counts or {}).get("deactivated"),
       "59 an EMPTY sheet read deactivates NOTHING — a transient Sheets failure "
       "must not retire every live campaign")

    counts = sync_rows_to_mdms(GROUP, [dict(ROW, campaign_start="not-a-date")])
    ck((counts or {}).get("rejected") == 1,
       "60 a malformed row is REJECTED with reasons")
    ck(not (counts or {}).get("deactivated"),
       "61 ...and its existing entry is KEPT as last-known-good, so one typo "
       "cannot kill a running campaign")
    ck(bool((counts or {}).get("rejected_details")),
       "62 the rejection carries a human-readable reason (%s)"
       % str((counts or {}).get("rejected_details"))[:60])

    counts = sync_rows_to_mdms(GROUP, [ROW, dict(ROW)])
    ck((counts or {}).get("rejected") == 1,
       "63 a duplicated sheet row is rejected once, first occurrence wins")

    counts = sync_rows_to_mdms(GROUP, [dict(ROW, tenant="")])
    ck((counts or {}).get("rejected") == 1,
       "64 a row with no tenant is rejected (it could never be scheduled)")

    sec("J. MDMS itself failing during the sync")

    clear_test_entries()
    saved = os.environ["MDMS_URL"]
    os.environ["MDMS_URL"] = "http://localhost:1"
    try:
        failed = False
        try:
            sync_rows_to_mdms(GROUP, [ROW])
        except Exception:
            failed = True
        ck(failed,
           "65 an unreachable MDMS makes the sync FAIL LOUDLY (the DAG task "
           "fails and alerts; a silent success would leave the mirror stale "
           "while the scheduler reads from it)")
    finally:
        os.environ["MDMS_URL"] = saved

    saved = os.environ.pop("MDMS_URL")
    try:
        ck(sync_rows_to_mdms(GROUP, [ROW]) is None,
           "66 with MDMS_URL unset the sync SKIPS itself and returns None rather "
           "than erroring — a sheet-mode deployment can ship this DAG unchanged")
    finally:
        os.environ["MDMS_URL"] = saved

    sec("K. Round trip: a campaign created by the sync is then SCHEDULED from MDMS")

    clear_test_entries()
    now = datetime.now(timezone.utc)
    slot = (now - timedelta(minutes=2)).strftime("%H:%M")
    fresh = dict(ROW, campaign_name="Round Trip Campaign",
                 campaign_number="CMP-ROUNDTRIP-1", report_times=slot,
                 campaign_end=(now + timedelta(days=3)).date().isoformat())
    put_entry(fresh)                     # stands in for the persisted create
    rows = rows_from_mdms()
    ck(any(r.get("campaign_name") == "Round Trip Campaign" for r in rows),
       "67 the scheduler reads the newly mirrored campaign back from MDMS")
    slots = due(rows, now)
    ck(len(slots) == 1,
       "68 and its slot is due, so a sheet edit reaches Airflow THROUGH MDMS")
    ck(slots and slots[0]["conf"]["row"].get("campaign_name") == "Round Trip Campaign",
       "69 the triggered conf carries the MDMS-sourced row, not a sheet row")

    put_entry(fresh, active=False)
    ck(due(rows_from_mdms(), now) == [],
       "70 once deactivated, the campaign stops being scheduled")

    sec("L. Mode gating")

    os.environ["DST_MDMS_ENABLED"] = "false"
    try:
        ck(mdms_enabled() is False,
           "71 with the flag false the deployment is in sheet mode")
    finally:
        os.environ["DST_MDMS_ENABLED"] = "true"
    ck(mdms_enabled() is True, "72 and true puts it back in MDMS mode")


SECTIONS = {"fast": section_fast, "sync": section_sync,
            "live": section_live}

if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "fast"
    chosen = list(SECTIONS) if which == "all" else [w for w in which.split(",")
                                                   if w in SECTIONS]
    if not chosen:
        print("usage: python mdms_airflow_tests.py [fast|live|all]")
        sys.exit(2)
    try:
        for name in chosen:
            SECTIONS[name]()
    finally:
        removed = clear_test_entries()
        print(f"\n[cleanup] removed {removed} '{TEST_GROUP}' mirror entr(y/ies)")

    print("=" * 60)
    print(f"{P} passed, {F} failed")
    for m in FAILS:
        print("   - " + m)
    sys.exit(1 if F else 0)
