# DST Campaign Automation

Daily performance reporting for DIGIT HCM health campaigns: **SMC (SPAQ)**, **AZM** and
**ITN / LLIN (bed nets)**.

For every active campaign row in a Google Sheet, the pipeline reads Elasticsearch, builds
a performance Excel and a CDD sync Excel, writes an internal and a partner Word report
with an AI-written narrative, uploads everything to Google Drive and posts the summary to
Slack — on the times set in the sheet.

This README describes the `main` branch, which is what runs on the JupyterHub box.

---

## How it works

```
Google Sheet (one row per campaign)
        │
        ▼
 scheduler.py ── reloads the sheet every hour, fires each row at its report_times
        │
        ▼
 config.py ── turns the row into a config: Day N, date window, ES index names
        │
        ├── SPAQ / AZM ──► analyze.py ─► cdd_sync.py ─► report.py ─┐
        │                                                         ├─► notify.py
        └── ITN / LLIN ──► analyze_itn.py ─► cdd_sync_itn.py ─► report_itn.py ─┘
                                                        (Drive upload + Slack post)
```

- `drug_type` on the sheet row picks the module set: `ITN` / `LLIN` use the `_itn`
  modules (household-based bed-net delivery); everything else uses the SPAQ/AZM modules
  (per-child doses).
- `cdd_sync` failures are non-fatal: the report still goes out, without sync data.
- Runs are locked per `state_name`: if a run for that state is still going when the next
  time arrives, the new trigger is skipped. Two active campaigns in the same state (e.g.
  Borno SMC and Borno ITN) therefore share one lock, and also one default `out_dir`.

---

## Requirements

- Python 3.9+
- `pip install -r requirements.txt` (also installed automatically on first run)

| Service | Used for | Configured by |
|---|---|---|
| Elasticsearch | task, individual, household, staff and sync data | `ES_URL`, `ES_USER`, `ES_PASS` |
| Google Sheets | campaign config + Run Log tab | service account `credential.json` |
| Google Drive | report and Excel uploads | the same service account |
| Slack | internal + partner posts | `SLACK_TOKEN` |
| Groq (OpenAI-compatible API) | report narrative, issues log, Slack text | `GROQ_API_KEY`, `GROQ_MODEL` |

**One service account** (`credential.json`) is used for both Sheets and Drive — no OAuth
token is needed. Give it Editor access to the config sheet and to the Drive folder.

---

## Setup

1. Clone the repo and `pip install -r requirements.txt`.
2. `cp .env.example .env` and fill it in (see [Environment variables](#environment-variables)).
3. Put `credential.json` (service account key) in the project root, or point
   `GOOGLE_CREDENTIALS_PATH` at it.
4. Add the campaign row to the sheet tab named in `GOOGLE_SHEET_TAB`
   (see [Google Sheet columns](#google-sheet-columns)).
5. Provide the targets file named in `target_csv`.

---

## Running

### Scheduler (normal operation, JupyterHub terminal)

```bash
bash start.sh     # start in the background (refuses to start a second copy)
bash status.sh    # running? + last 10 log lines
bash stop.sh      # stop the watchdog AND its child process
```

`start.sh` runs `scheduler.py` in watchdog mode: it restarts the scheduler automatically if
it crashes. Logs: `logs/scheduler_bg.log` and `logs/scheduler_YYYY-MM-DD.log`.

On Windows, `schedule.bat` does the same (restart loop, log in `logs\scheduler_bat.log`).

From a Jupyter cell:

```python
import scheduler
scheduler.launch_background()   # start
scheduler.status()              # check
scheduler.stop()                # clear all jobs
```

### One-off runs

```bash
python run.py                                  # daily run for every active row, now
python run.py --state Borno                    # one campaign only
python run.py --cumulative                     # whole-campaign report up to campaign_end
python run.py --cumulative 20261006 --state Borno   # up to (and including) this date
```

**Cumulative** covers the whole campaign (distribution + mop-up days) and measures coverage
against the full campaign target. It writes the files and uploads them to Drive, and prints
the Drive links — it **does not post to Slack**. Share the links manually.

To test against a past day, set `TEST_EXTRACT_DATE=YYYY-MM-DD` in `.env`.

---

## Google Sheet columns

One row per campaign. The tab is chosen by `GOOGLE_SHEET_TAB` (each deployment reads its own
tab of the same sheet). Extra columns are ignored.

| Column | Example | Notes |
|---|---|---|
| `active` | TRUE | FALSE skips the row |
| `campaign_name` | SMC Cycle 4 | used in titles and the Drive folder name |
| `state_name` | Borno | used in titles, file names and the Drive folder name |
| `tenant` | bo | ES index prefix (see `ES_INDEX_PREFIX`) |
| `drug_type` | SPAQ | `SPAQ`, `AZM`, `ITN` or `LLIN` — picks the module set |
| `campaign_start` / `campaign_end` | 2026-09-24 | `YYYY-MM-DD` or `DD/MM/YYYY`; runs outside this window are skipped |
| `campaign_days` | 4 | campaign length; Day N is clamped to it (default 4) |
| `is_admin_console` | TRUE | TRUE = filter by `campaign_number` (Nigeria) |
| `campaign_number` | CMP-2026-07-07-000434 | campaign id from the admin console (required for ITN) |
| `project_type_id` / `project_type` | | used when `is_admin_console` is FALSE |
| `cycle_index` | 4 | padded to `04` automatically |
| `task_date_field` | taskDates | ES date field for the task window (default `taskDates`) |
| `dose_index_filter` | FALSE | TRUE adds `doseIndex = 1` to the treatment query |
| `task_campaign_filter` | FALSE | TRUE adds the campaign filter to the task query (AZM / shared tenants) |
| `secondary_product` | ORS-ZINC | extra product counted alongside the main drug; list format `name\|label\|ageMin\|ageMax; ...` |
| `dup_matrix` | FALSE | ITN only: duplicate-distribution matrix (same/different user x same/different day) |
| `target_csv` | /path/targets.csv | targets file (CSV or Google Sheet URL) |
| `report_times` | 05:30,11:30 | **UTC**, comma-separated — internal (and partner, if `partner_report_times` is empty) |
| `partner_report_times` | 12:00,17:00 | UTC; if set, `report_times` become internal-only and these drive the partner post |
| `slack_channel` | C0XXXXXXX | internal channel id |
| `slack_channel_partners` | C0XXXXXXX | partner channel id; the partner report is built when this is set (and always for `--cumulative`) |
| `out_dir` | | optional output folder (default `pipeline/output/<tenant>`) |
| `hfs_total` / `flws_total` / `lgas_total` | 274 | optional totals shown in the report |
| `google_sheet_id` | | read but currently unused (the Sheets write in `notify.py` is not called) |

**Times are UTC.** IST = UTC + 5:30 (e.g. 11:00 IST = 05:30 UTC). The scheduler re-reads
the sheet every hour, so a changed time takes effect within the hour.

---

## Environment variables

Set in `.env`:

| Key | Required | Meaning |
|---|---|---|
| `ES_URL`, `ES_USER`, `ES_PASS` | yes | Elasticsearch connection |
| `ES_INDEX_PREFIX` | no | unset = `<tenant>-` prefix (Nigeria central); empty = no prefix (Togo); any value = that prefix |
| `GOOGLE_CREDENTIALS_PATH` | yes | service account key (falls back to `credential.json` in the project root) |
| `GOOGLE_SHEET_ID` | yes | the campaign config sheet |
| `GOOGLE_SHEET_TAB` | no | tab to read (default `Sheet1`); also names the top Drive folder |
| `GOOGLE_RUNLOG_TAB` | no | Run Log tab (default `Run Log`) |
| `GOOGLE_DRIVE_FOLDER_ID` | yes | root Drive folder for uploads |
| `SLACK_TOKEN` | yes | Slack bot token |
| `SLACK_CHANNEL` | no | fallback channel for failure alerts |
| `GROQ_API_KEY` | yes | narrative generation (placeholder text if missing) |
| `GROQ_MODEL` | no | default `openai/gpt-oss-120b` |
| `GROQ_BASE_URL` | no | default `https://api.groq.com/openai/v1` |
| `TEST_EXTRACT_DATE` | no | `YYYY-MM-DD` — pretend today is this date (testing only) |

Never commit `.env` or `credential.json`.

---

## Outputs

Written to `out_dir` and uploaded to Drive under
`<sheet tab>/<state>/<campaign>/<Day N | Cumulative (Days 1-N)>`.

**SPAQ / AZM**
- `performance_dayN.xlsx` — `ALL FACILITIES`, one tab per band (`LOW`, `MODERATE`, `HIGH`,
  `NO TARGET`, `LOW ACTIVITY`, `NOT REPORTED`), plus a tab per secondary product.
- `cdd_sync_dayN.xlsx` — `SUMMARY`, one tab per LGA, `NEVER SYNCED`, `LOW SYNCED`,
  `NOT SYNCED BY 1730`.

**ITN / LLIN** (LGA grain — targets exist per LGA)
- `performance_dayN.xlsx` — `ALL LGAS`, one tab per band, `FACILITY DETAIL`, plus `DUP ...`
  tabs when `dup_matrix` is on. A daily copy is kept in `itn_history/`.
- `cdd_sync_dayN.xlsx` — same tab set as SPAQ, roster built from sync records.

**Word reports** — `<State>_DayN_Report_HHMM.docx` (internal) and
`<State>_DayN_PartnerReport_HHMM.docx` (partner: data-quality sections removed, and it links
a performance Excel with the data-quality columns stripped).

**Slack** — the summary plus a Drive link goes to `slack_channel`; the same summary with the
partner report link goes to `slack_channel_partners`.

**Run Log** — `run.py` appends one row per run to the `Run Log` tab:
Timestamp, State, Campaign, Day, Time, Status, Step Failed, Error, Drive Link.
Scheduled runs (`scheduler.py`) do **not** write the Run Log — check the scheduler log instead.

---

## ITN notes (current `main`)

The ITN modules on `main` follow the **Chad** data model:
- campaign scoped by `additionalDetails.projectReferenceId`;
- facility tier `boundaryHierarchy.sppSfd`, LGA from `district`;
- CDD role on sync records `DISTRIBUTOR_REGISTRAR` (hard-coded in `cdd_sync_itn.py`);
- targets joined on the LGA **code** (`boundaryHierarchyCode.district`), not the name.

Nigeria-convention ITN campaigns (e.g. Borno: top-level `campaignNumber`, `state`/`lga`,
role `DISTRIBUTOR`) are handled on the `scanner-stock-merge` branch, which also adds the
stock report, the eGov error tracer and the `stock_report` / `itn_scanner` / `cdd_role`
sheet columns.

---

## Data quality metrics

| Metric | Meaning |
|---|---|
| Duplicate Records | same household head, child, ward and age more than once (sync/retry repeats); ITN: records minus distinct households |
| Missing HH Name | household head not resolved from the household-member index |
| Missing Child Name | child name not resolved from the individual index |
| Age = 0 / Age > 59 months | records outside the treatment age window |
| Missing GPS | ITN: delivery records without latitude/longitude |

Names always come from the individual index (via the household-member join), never from
task `additionalDetails`.

---

## Project structure

```
automation/
├── run.py              # one-off runs (daily, --state, --cumulative); writes the Run Log
├── scheduler.py        # scheduler + watchdog (reads the sheet, fires runs at report_times)
├── start.sh / stop.sh / status.sh   # JupyterHub process control
├── schedule.bat        # Windows restart loop
├── pipeline/
│   ├── config.py       # sheet reader + per-row config builder
│   ├── analyze.py      # SPAQ/AZM: ES task scroll + name lookups -> performance Excel
│   ├── cdd_sync.py     # SPAQ/AZM: staff + sync -> CDD sync Excel
│   ├── report.py       # SPAQ/AZM: Word reports + Slack text (Groq narrative)
│   ├── analyze_itn.py  # ITN: household-based aggregation -> performance Excel
│   ├── cdd_sync_itn.py # ITN: sync-derived CDD roster -> CDD sync Excel
│   ├── report_itn.py   # ITN: Word reports + Slack text
│   └── notify.py       # Drive folders/uploads + Slack posts
├── utils/gen_drive_token.py   # legacy OAuth helper (not used — Drive uses the service account)
├── requirements.txt
└── .env.example
```

---

## Troubleshooting

| Symptom | Check |
|---|---|
| Report did not go out at the expected time | `report_times` must be **UTC**; `bash status.sh`; `tail -50 logs/scheduler_bg.log` |
| Two reports at once / duplicate posts | more than one scheduler running — `bash stop.sh` then `bash start.sh` |
| `credential.json not found` | `GOOGLE_CREDENTIALS_PATH` in `.env`, or put the file in the project root |
| `Worksheet tab ... not found` | `GOOGLE_SHEET_TAB` must match the tab name exactly |
| Drive upload failed | the service account needs Editor access to the Drive folder |
| Report narrative says "[Narrative not generated]" | `GROQ_API_KEY` / `GROQ_MODEL` (a retired model returns 404) |
| All coverage reads N/A or 0% | `target_csv` path / column names; ITN targets must be keyed by LGA code |
| ES `404 index_not_found` | `tenant` and `ES_INDEX_PREFIX` (tenant-prefixed vs un-prefixed cluster) |
| High duplicate count | expected when the app re-sends records after sync retries; each repeat is counted once |
