# DST Campaign Automation

Automated daily performance reporting pipeline for DIGIT HCM SMC/SPAQ campaigns.

Fetches treatment data from Elasticsearch, generates facility-level performance Excel reports and CDD sync status reports, produces AI-written Word summaries, uploads to Google Drive, and posts to Slack — fully automated on a configurable daily schedule.

---

## How It Works

```
Google Sheet (campaign config)
        │
        ▼
   dst_campaign_scheduler  ──── Airflow DAG, every 5 min: which slots are due?
        │
        ▼
   dst_campaign_run        ──── one DAG run per due slot, row passed in conf
        │
        ▼
   pipeline/config.py  ──── reads the row, computes DAY / GTE / LTE / date labels
        │
        ├──▶  analyze.py   ──── ES task scroll → individual/HH lookup → performance.xlsx
        ├──▶  cdd_sync.py  ──── ES staff + sync aggregation → cdd_sync.xlsx
        ├──▶  report.py    ──── both xlsx + Groq narrative → Word report + Slack text
        └──▶  notify.py    ──── Drive upload + Slack post
```

Orchestration is Apache Airflow — see `dags/README.md`. ITN/LLIN campaigns use
the `_itn` module variants, selected by the `drug_type` column.

---

## The two flows

The Google Sheet is the **only human surface in both modes**. `DST_MDMS_ENABLED`
changes two things and nothing else: where the scheduler READS the campaign rows,
and where a finished run WRITES its outcome. The pipeline in between — extract,
Excels, narrative, Word report, Drive, Slack — is byte-for-byte identical.

### Sheet mode (`DST_MDMS_ENABLED=false`, the default)

```
  Google Sheet, tab per credential group
  (one row per campaign; humans edit this)
                 │
                 │  read on EVERY tick — nothing is cached
                 ▼
  dst_campaign_scheduler        every 5 min
    ├─ read the tab                              -> config.get_active_rows()
    ├─ read today's Run Log rows                 -> the retime guard
    ├─ match slots against (now-60min, now]      -> wall-clock, not data interval
    └─ trigger one run per due slot              -> row travels inside dag_run.conf
                 │
                 ▼
  dst_campaign_run              triggered, never scheduled
    ├─ execute_campaign_pipeline   analyze -> cdd_sync -> report -> notify
    │                              (one task, one process, local files)
    └─ finalize_run                append ONE row to the Run Log tab
                 │
                 ▼
  Google Drive (Excels, Word report, chart, checkpoints)
  Slack         (internal channel, partner channel)
  Run Log tab   (the audit trail: status, step failed, error, Drive link)
```

Everything lives in the sheet: config, history, and the duplicate-report guard.
No database, no message broker, nothing else to install. This is what every
current deployment runs.

### MDMS mode (`DST_MDMS_ENABLED=true`, platform-integrated)

```
  Google Sheet  (STILL the only place humans edit)
                 │
                 │  one-way mirror; the sheet always wins
                 ▼
  dst_config_sync               every 10 min, mdms mode ONLY
    read tab -> plan_sync (pure diff) -> apply_sync (HTTP)
      new identity        -> create
      changed row         -> update
      row gone            -> deactivate (isActive=false, never deleted)
      invalid row         -> REJECTED, existing entry kept as last-known-good
      tab reads empty     -> deactivation skipped entirely
                 │
                 ▼
  MDMS  (schema dst-campaign-report-config, one entry per campaign)
                 │
                 │  read on every tick; on ANY error fall back to the sheet
                 ▼
  dst_campaign_scheduler        every 5 min
    ├─ read the MDMS mirror                      -> sheet fallback per tick
    ├─ NO retime guard                            -> zero sheet/DB on this path
    ├─ match slots against (now-60min, now]
    └─ trigger one run per due slot
                 │
                 ▼
  dst_campaign_run              identical pipeline
    ├─ execute_campaign_pipeline   analyze -> cdd_sync -> report -> notify
    └─ finalize_run                publish ONE Kafka lifecycle event
                 │
                 ▼
  Kafka  save-dst-report-metadata      ({tenant}- prefixed on central instances)
                 │
                 ▼
  egov-persister  (dst-report-metadata-persister.yml)
                 │
                 ▼
  Postgres  dst_report_metadata        one row per report attempt
```

Why mirror rather than migrate: the platform expects configuration in MDMS, but
operators must keep editing a spreadsheet. So the sheet stays authoritative and
is mirrored one way — a manual MDMS edit is overwritten on the next tick.

Why Kafka rather than a direct insert: our code holds no database credentials in
any mode. The producer publishes; the platform's persister owns the write. The
event carries `event_id`, and the persister inserts `ON CONFLICT DO NOTHING`, so a
replayed event cannot duplicate a row.

### What differs, precisely

| | Sheet mode | MDMS mode |
|---|---|---|
| Humans edit | the sheet | the sheet |
| Scheduler reads config from | the sheet tab | the MDMS mirror (sheet on error) |
| `dst_config_sync` | skips itself | mirrors the sheet every 10 min |
| Run history | Run Log tab | Kafka -> persister -> `dst_report_metadata` |
| Duplicate-report guard | Run Log read | none; run-id dedup only |
| Database access by our code | zero | zero |
| Extra infrastructure | none | MDMS, Kafka, persister, one table |

### Failure behaviour of each hop

Both modes are built so a dependency being down degrades the run rather than
stopping the schedule:

- **MDMS unreachable** — the scheduler logs it and reads the sheet for that tick.
  Scheduling never stops. The two sources are identical by construction.
- **Kafka publish does not land** — the outcome is appended to the Run Log tab
  instead, so a broker outage cannot erase the audit trail, and a Kafka problem
  never fails a run.
- **A slot is missed** (Airflow down) — the next tick catches anything inside
  `DST_LOOKBACK_MINUTES`. Beyond that window it is lost.
- **The same slot seen twice** — the trigger run id is deterministic
  (`dst_{group}_{tenant}_{campaign}_{mode}_{date}_{hhmm}`), so a repeated match is
  skipped, never doubled.
- **Two campaigns on one tenant** — separate run ids, separate working
  directories, separate Drive folders. They cannot overwrite each other.
- **`cdd_sync`, the narrative, or a Drive upload fails** — the report is still
  published, the run is marked `degraded`, and a Slack warning names what is
  missing and what to do about it.
- **Malformed data or config** — fails immediately with no retries (a retry fails
  identically). Network and 5xx errors retry twice, three minutes apart. A 404 or
  401 from Elasticsearch is treated as permanent, not transient.

---

## Requirements

### Python
Python 3.9+

### Dependencies
```bash
pip install -r requirements.txt
```

### External Services
| Service | Used For | Credential |
|---|---|---|
| Elasticsearch | Task, individual, household data | `ES_URL`, `ES_USER`, `ES_PASS` in `.env` |
| Google Sheets | Campaign config + run log | `credential.json` (service account) |
| Google Drive | Report file uploads | `credential.json` (same service account) |
| Slack | Report notifications | `SLACK_TOKEN` in `.env` |
| Groq | Report narrative + Slack text | `GROQ_API_KEY` in `.env` |

### Credential Files
| File | Purpose | How to Obtain |
|---|---|---|
| `credential.json` | Google service account — **Sheets AND Drive** | Google Cloud Console → Service Accounts |
| ~~`drive_oauth_client.json`~~ | **No longer used** (only fed the retired OAuth flow). | — |
| ~~`drive_token.json`~~ | **No longer used.** The pipeline authenticates to Drive with the service account (`notify.py` and `core/drive.py` both call `config._resolve_creds_path`). Kept only for historical runs. | — |

> **Important:** `credential.json` is the ONLY credential file the pipeline needs. Drive uploads use the same service account as Sheets — verified 2026-08-21, nothing imports `drive_token.json`. Grant the service account write access to the Drive folder.

---

## Setup

### 1. Clone the repository
```bash
git clone <repo-url>
cd automation
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure the environment

Local runs: `cp .env.example .env` and fill it in.

Airflow: create the **`dst_config`** Variable instead (Admin -> Variables) and
leave the repo `.env` alone — it is deliberately not read when Airflow owns the
environment. See [Configuration](#configuration).

### 4. Add credential files
Place the following in the project root:
- `credential.json` — Google service account key
- (`drive_token.json` is not required — the service account covers Drive)

### 5. Configure the campaign in Google Sheet
Add a row to the configured Google Sheet (see [Google Sheet Schema](#google-sheet-schema)).

### 6. Add facility targets CSV
A CSV with columns `facility_name` and `individual_target` for the campaign state.
Set the path in the `target_csv` column of the Google Sheet.

---

## Running

Scheduling is owned by Airflow. Unpause `dst_campaign_scheduler` and it triggers
`dst_campaign_run` for every due slot; there is no daemon to start or stop.
Setup, Airflow Variables and the edge-case table are in `dags/README.md`.

### Run one campaign by hand
Trigger `dst_campaign_run` from the Airflow UI with a conf payload containing the
campaign row (`{"row": {...}, "mode": "both"}`) — see `dags/dst_campaign_run.py`.

### Whole-campaign cumulative report
```bash
python run.py --cumulative --state "Chad"
```
`run.py` is retained ONLY for this; the daily path runs in Airflow.

### Backdate an extract
Set `TEST_EXTRACT_DATE=YYYY-MM-DD` in the environment of the run.

---

## Google Sheet Schema

One row per active campaign. The sheet ID is set via `GOOGLE_SHEET_ID` in `.env`.

| Column | Example | Required | Notes |
|---|---|---|---|
| `active` | TRUE | ✓ | Set FALSE to skip this campaign |
| `state_name` | Bauchi | ✓ | Used in report titles and filenames |
| `tenant` | ba | ✓ | Elasticsearch index prefix |
| `drug_type` | SPAQ | ✓ | `SPAQ` or `AZM` |
| `campaign_start` | 2026-06-25 | ✓ | First day — `YYYY-MM-DD` |
| `campaign_end` | 2026-06-28 | ✓ | Last day — `YYYY-MM-DD` |
| `campaign_days` | 4 | ✓ | Total days in campaign |
| `is_admin_console` | TRUE | ✓ | `TRUE` for Nigeria states |
| `campaign_number` | CMP-2026-06-22-000401 | ✓ | From DIGIT HCM admin console |
| `target_csv` | /path/to/targets.csv | ✓ | Facility targets file (CSV or Google Sheet URL) |
| `out_dir` | output/bauchi (optional; defaults to pipeline/output/<tenant>) | ✓ | Output directory for reports |
| `slack_channel` | C0ALY7EQVSR | ✓ | Slack channel ID |
| `report_times` | 05:30,07:30,09:30,11:30 | ✓ | **UTC times**, comma-separated |
| `hfs_total` | 274 | | Total registered health facilities |
| `flws_total` | 3764 | | Total registered FLWs |

### report_times — IST to UTC conversion
All times in the sheet must be **UTC**. IST = UTC+5:30.

| IST | UTC |
|---|---|
| 9:00 AM | 03:30 |
| 11:00 AM | 05:30 |
| 1:00 PM | 07:30 |
| 3:00 PM | 09:30 |
| 5:00 PM | 11:30 |
| 7:00 PM | 13:30 |

---

## Configuration

Configuration comes from **the environment** — the pipeline only ever calls
`os.getenv`. What differs per deployment is *where that environment comes from*,
and there are exactly two supported answers.

### Local runs (laptop, JupyterHub): the repo `.env`

Copy `.env.example` to `.env` and fill it in. `pipeline/config.py` loads it
automatically.

```env
# Groq (narrative generation, OpenAI-compatible endpoint)
GROQ_API_KEY=gsk_...
GROQ_MODEL=openai/gpt-oss-120b

# Slack
SLACK_TOKEN=xoxb-...
SLACK_CHANNEL=C0XXXXXXXX
DST_ALERT_CHANNEL=C0XXXXXXXX     # ops channel for FAILURES; keep it separate
                                 # from the campaign channel partners read

# Google (one service account covers BOTH Sheets and Drive)
GOOGLE_CREDENTIALS_PATH=/path/to/credential.json
GOOGLE_SHEET_ID=your_sheet_id_here
GOOGLE_SHEET_TAB=Nigeria States
GOOGLE_DRIVE_FOLDER_ID=your_drive_folder_id_here
DST_TARGET_FOLDER_ID=drive_folder_holding_the_target_books

# Elasticsearch
ES_URL=https://elasticsearch-data.es-cluster-v8:9200
ES_USER=your_es_user
ES_PASS=your_es_password
# ES_INDEX_PREFIX: LEAVE THE LINE OUT for tenant-prefixed indices
# (ba-project-task-index-v1). Set it EMPTY only for Togo's un-prefixed cluster.
```

### Airflow: the `dst_config` Variable

A managed Airflow may not let you set pod environment variables or mount files —
on a hosted instance the only writable surface is **Admin → Variables**. So the
whole configuration lives in **one** Variable named `dst_config`, and rotating a
credential is a single edit in a single place.

```json
{
  "sheet_tab": "Nigeria States",
  "env": {
    "ES_URL": "https://elasticsearch-data.es-cluster-v8:9200",
    "ES_INDEX_PREFIX": null,
    "GOOGLE_SHEET_ID": "1MspTor...",
    "GOOGLE_DRIVE_FOLDER_ID": "0AO9...",
    "DST_TARGET_FOLDER_ID": "1IyH...",
    "GROQ_MODEL": "openai/gpt-oss-120b",
    "SLACK_CHANNEL": "C0...",
    "DST_ALERT_CHANNEL": "C0...",
    "DST_MDMS_ENABLED": "false",
    "DST_LOOKBACK_MINUTES": "60",
    "CDD_ROLE": "DISTRIBUTOR"
  },
  "secrets": {
    "ES_USER": "...", "ES_PASS": "...",
    "SLACK_TOKEN": "xoxb-...", "GROQ_API_KEY": "gsk_..."
  },
  "google_credentials_json": { "type": "service_account", "...": "..." }
}
```

- **`env`** — routing and behaviour. Keep it here: it is visible and reviewable.
- **`secrets`** — credentials only. Applied LAST, so it overrides `env`; a routing
  key placed here silently redirects real work, which is why anything from a
  known routing list is logged loudly by name if found in `secrets`.
- **`google_credentials_json`** — the service account as JSON. A Variable cannot
  create a file, so the contents are written to a 0600 temp file at task start,
  `GOOGLE_CREDENTIALS_PATH` is pointed at it, and it is deleted when the task
  ends. A real mounted file at `GOOGLE_CREDENTIALS_PATH` is used if present.
- **`null` removes a key** (tenant-prefixed indices need `ES_INDEX_PREFIX`
  absent); `""` sets it present-and-empty (Togo's un-prefixed cluster). The
  distinction selects which indices are read, so it is load-bearing.
- Applied by `dst_config.apply()` around the WHOLE task body, not just the
  per-group block, because `mdms_enabled()`, the Kafka producer and the Slack
  alert channel are all resolved outside that block.

Precedence, lowest to highest: **process env < `env` < `secrets`**.

### The repo `.env` is NOT read under Airflow

`pipeline/config.py` skips it whenever Airflow owns the environment (detected via
`AIRFLOW_HOME` / `AIRFLOW_CTX_DAG_ID` / `AIRFLOW__CORE__EXECUTOR`), and
`DST_LOAD_DOTENV=true|false` forces the decision either way.

This matters because deployments commonly bind-mount the repo. `load_dotenv`'s
`override=False` protects variables that already EXIST, but it cannot protect one
the deployment intends to be **absent** — so a mounted `.env` carrying
`ES_INDEX_PREFIX=` (correct for Togo) silently switched every tenant to
un-prefixed indices, and a run read the wrong index, matched nothing, and
published a complete report describing zero records while reporting SUCCESS. Set
`DST_LOAD_DOTENV=false` explicitly in any deployment that mounts the repo.

### Multiple credential sets

One `dst_config` = one credential set. Tenants are **rows** on the sheet tab, not
groups: the Nigeria central instance serves roughly a dozen tenants (Chad
included) from one login. A second group is only needed when a tenant genuinely
has a different ES login — Taraba and Togo — and that is what the older
`dst_groups` + `dst_secrets_<name>` Variables are for. They still work; with
`dst_config` present the group list collapses to its single entry.

### Configuration is validated, loudly

`config.build()` rejects contradictory config for ACTIVE rows, naming the field,
the value, the consequence and the fix — and the message travels into the Slack
alert, so the alert alone is enough to repair the sheet:

| Rejected | Why it used to be dangerous |
|---|---|
| `campaign_end` before `campaign_start` | never in window, so no report ever ran and nothing warned |
| `campaign_days` 0 or negative | it is the DIVISOR for every coverage figure |
| unparseable or pre-start `mopup_end_date` | the cumulative range is empty |
| non-numeric `cycle_index` | ES matches `cycleIndex` exactly, so `'O2'` yields a silently EMPTY report |
| unknown `drug_type` | selects the pipeline; a typo'd ITN silently ran the SPAQ one |
| unrecognised `is_admin_console` | selects the ES query shape |

An INACTIVE row is never validated — nobody is waiting on its report.

---

## MDMS mode (platform integration)

One deployment-wide switch, `DST_MDMS_ENABLED`:

| | `false` (default) | `true` |
|---|---|---|
| Config read | Google Sheet tab | MDMS mirror, sheet fallback per tick on any error |
| Run history | Run Log tab | Kafka event -> egov-persister -> `dst_report_metadata` |
| Retime guard | Run Log read | none (run-id dedup still prevents duplicates) |
| Database access | zero | zero (the persister owns the write) |

Extra keys for `true`:

```
KAFKA_BROKER=kafka:9092          # or the in-cluster broker address
DST_RUNS_TOPIC=save-dst-report-metadata
MDMS_URL=http://mdms-v2:8099
MDMS_API_PREFIX=/mdms-v2/v2      # /egov-mdms-service/v2 when talking DIRECT to
                                 # the service instead of through the gateway.
                                 # Governs BOTH reads and writes.
MDMS_TENANT_ID=dev
```

Platform-side installation is in `platform/`: apply `dst_report_metadata.sql`
once, and add `dst-report-metadata-persister.yml` to the persister's config path.
On central instances the producer publishes to `{tenant}-save-dst-report-metadata`,
so the persister's `fromTopic` must list every tenant's prefixed topic.

Both fallbacks are deliberate and tested: an unreachable MDMS falls back to the
sheet for that tick rather than stopping the schedule, and a Kafka publish that
does not land is appended to the Run Log tab instead — a broker outage never
loses the audit trail and never fails a run.

`MDMS_API_PREFIX` needs `/v2`: the mirror writes via `_create/{schemaCode}` and
`_update/{schemaCode}`, which MDMS v1 does not provide. A v1-only service can be
read from but cannot be mirrored into.

---

## Environment Variables reference

| Key | Purpose |
|---|---|
| `ES_URL`, `ES_USER`, `ES_PASS` | Elasticsearch |
| `ES_INDEX_PREFIX` | ABSENT = `{tenant}-` prefix; empty = none; else literal |
| `GOOGLE_SHEET_ID`, `GOOGLE_SHEET_TAB` | campaign config location |
| `GOOGLE_RUNLOG_TAB` | Run Log tab name (default `Run Log`) |
| `GOOGLE_CREDENTIALS_PATH` | service account file — Sheets AND Drive |
| `GOOGLE_DRIVE_FOLDER_ID` | root Drive folder for published reports |
| `DST_TARGET_FOLDER_ID` | Drive folder holding the per-tenant target books |
| `SLACK_TOKEN`, `SLACK_CHANNEL` | report posts |
| `DST_ALERT_CHANNEL` | ops channel for failures and degraded runs |
| `GROQ_API_KEY`, `GROQ_MODEL`, `GROQ_BASE_URL` | narrative generation |
| `CDD_ROLE` | field-staff role for SPAQ/AZM (default `DISTRIBUTOR`; Togo uses `COMMUNITY_DISTRIBUTOR`) |
| `CDD_ROLE_ITN` | field-staff role for ITN/LLIN (default `DISTRIBUTOR_REGISTRAR`) |
| `DST_MDMS_ENABLED` | `false` sheet mode, `true` platform mode |
| `DST_LOOKBACK_MINUTES` | slot-matching window, default 60 |
| `DST_LOAD_DOTENV` | force the repo `.env` on or off |
| `KAFKA_BROKER`, `DST_RUNS_TOPIC`, `IS_CENTRAL_INSTANCE_ENABLED` | mdms-mode audit path |
| `MDMS_URL`, `MDMS_API_PREFIX`, `MDMS_TENANT_ID`, `MDMS_AUTH_TOKEN` | mdms-mode config mirror |
| `TEST_EXTRACT_DATE` | backdate an extract (`YYYY-MM-DD`) |

---

## Output Files

All outputs are written to the `out_dir` defined in the Google Sheet.

```
output/bauchi/
├── performance_day1.xlsx       # Facility-level performance data (7 tabs)
├── cdd_sync_day1.xlsx          # CDD sync status by LGA and facility
├── Bauchi_Day1_Report_0730.docx  # Word report with AI-generated narrative
└── logs/
    └── 2026-06-25.log          # Daily pipeline log
```

### Performance Excel tabs
`ALL FACILITIES` | `HIGH` | `MODERATE` | `LOW` | `NO TARGET` | `LOW ACTIVITY`

### CDD Sync Excel tabs
`SUMMARY` | per-LGA tabs | `NEVER SYNCED` | `LOW SYNCED`

---

## Schedule Behaviour

- `dst_campaign_scheduler` re-reads the Google Sheet on **every 5-minute tick** — a sheet edit is live within 5 minutes, with no restart and nothing cached
- Each slot fires at most once: the trigger run id is deterministic per (group, tenant, mode, date, time)
- `DAY` is auto-computed daily from `campaign_start` — no manual updates needed across campaign days
- Outside `campaign_start` → `campaign_end` window, runs are skipped automatically
- Multiple campaigns can run in parallel — add one row per state in the sheet

---

## Data Quality Notes

The pipeline computes the following data quality metrics per facility:

| Metric | Definition |
|---|---|
| Duplicate Records | Same (HH head, child name, ward, age) appearing more than once — flags sync/retry app bugs |
| Missing HH Name | Household head not resolved from household-member index |
| Missing Child Name | Individual name not resolved from individual index |
| Age = 0 | Records where age field is 0 |
| Age > 59 months | Records outside the 0–59 month treatment window |

---

## Project Structure

```
automation/
├── dags/               # Airflow DAGs — the orchestrator (see dags/README.md)
│   ├── dst_campaign_scheduler.py
│   ├── dst_campaign_run.py
│   ├── dst_config_sync.py
│   └── common/
│       ├── dst_config.py    # THE single-Variable configuration (see Configuration)
│       ├── slots.py         # due-slot matching
│       ├── deployment_env.py# per-group env + secrets
│       ├── campaign_runner.py, run_history.py, alerts.py, dst_kafka_status.py
├── pipeline/           # the business logic (see pipeline/README.md)
│   ├── config.py       # sheet row → resolved config
│   ├── analyze.py      # ES task scroll + name lookups → performance Excel
│   ├── cdd_sync.py     # ES staff + sync aggregation → CDD sync Excel
│   ├── report.py       # Excel + Groq narrative → Word report + Slack text
│   ├── notify.py       # Google Drive upload + Slack post
│   ├── mdms.py         # sheet → MDMS mirror (mdms mode)
│   ├── run_log.py      # Run Log tab (sheet mode)
│   └── core/           # es / excel / word / llm / drive / checkpoint helpers
├── platform/           # egov persister config + DDL for mdms mode
├── run.py              # cumulative (whole-campaign) report CLI only
├── requirements.txt
├── .env                # Secrets — never committed
├── .env.example        # Template for .env
└── README.md
```

---

## Troubleshooting

**A report was published but every number is zero**

- Almost always the wrong Elasticsearch index. Check the resolved index in the
  task log (`ES_INDEX_TASK`): a tenant-prefixed deployment must have
  `ES_INDEX_PREFIX` **absent**, not empty. If the repo is bind-mounted, confirm
  `DST_LOAD_DOTENV=false` so a stray `.env` cannot supply it.
- Then check `campaign_number` and `cycle_index` against ES. `cycleIndex` is
  matched exactly as a zero-padded string (`"02"`).

**The run says SUCCESS but a report is missing from Drive**

- Look for `not uploaded to Drive` in the task log. Uploads retry three times;
  if one still fails the run is marked `degraded` and alerts. Google's resumable
  endpoint intermittently 500s on the first upload of a run.

**A Slack alert says the report is INCOMPLETE**

- The report was published but is missing data — the alert names what and why.
  Most often CDD sync numbers are absent (`campaign_number` / `project_type_id`
  wrong, no staff registered, or `CDD_ROLE` not matching this tenant's role).

**A slot did not fire at the expected time**
- All `report_times` in the sheet must be UTC, not IST
- Check the `dst_campaign_scheduler` task log for that tick: it logs the row
  count and every due slot it matched

**`GOOGLE_CREDENTIALS_PATH not set or missing`**
- Verify path in `.env` points to `credential.json` on the server

**`Drive upload failed`**
- Check the service account has write access to `GOOGLE_DRIVE_FOLDER_ID` (it is a Shared Drive; the account must be a member)
- A repeated `HttpError 500 Internal Error` on the FIRST upload of a run is a known Drive flakiness

**`ES connection error`**
- Verify `ES_URL`, `ES_USER`, `ES_PASS` in `.env` are correct

**High duplicate count in report**
- Expected if the DIGIT HCM app has sync/retry issues — duplicates are genuine
- Each unique (HH head name, child name, ward, age) is counted once; all repeats are flagged
