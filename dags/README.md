# Airflow DAGs — developer guide

Airflow owns all scheduling. The pipeline business logic in `pipeline/` is
untouched — these DAGs only orchestrate it.

```
dst_campaign_scheduler   every 5 min: read the Google Sheet, find due slots,
                         trigger one dst_campaign_run per slot
dst_campaign_run         trigger-only: run one campaign report end to end
common/
  slots.py               due-slot matching (pure functions, unit-tested)
  deployment_env.py      credential groups: sheet tab + env + secrets per group
  run_history.py         retime guard over the Run Log tab (sheet mode)
  dst_kafka_status.py    run lifecycle events to Kafka (mdms mode)
  campaign_runner.py     the pipeline chain in one task, error classification
  alerts.py              Slack failure callback
```

## How the sheet is fetched

- The sheet is read ONLY inside `find_due_campaigns` tasks, on every 5-minute
  tick — never at DAG-parse time (the dag-processor re-executes DAG files
  every ~30 s; a parse-time read would hammer the Sheets API).
- Each tick is stateless: read sheet -> compute slots -> trigger. There is no
  cached schedule to invalidate, so a sheet edit is live within 5 minutes.
- The row travels to `dst_campaign_run` inside `dag_run.conf` — the processor
  never reads the sheet, and a run always executes the row snapshot that made
  it due (a mid-run sheet edit cannot half-apply).
- One `find_due_campaigns` task instance per deployment group (dynamic task
  mapping over `dst_groups`), because groups exist per CREDENTIAL SET: reading
  a group's tab needs only the shared `credential.json`, but executing its
  campaigns needs that group's ES secrets, applied by `group_environment`
  only inside the execute task.

## Scheduling semantics and edge cases

| Edge case | Handling |
|---|---|
| Airflow 3 zero-width data interval | slots match against wall-clock `[now - lookback, now]` (`DST_LOOKBACK_MINUTES`, default 60) |
| Same slot seen by several ticks (wide lookback) | deterministic run id `dst_{group}_{tenant}_{mode}_{date}_{hhmm}` + `skip_when_already_exists` — a slot can never fire twice |
| Airflow down over a slot | first tick after recovery catches anything inside the lookback |
| `report_times` edited backwards after the old time fired | retime guard: a past-due slot is skipped when a successful run for that (tenant, mode-or-both) already exists at/after the slot time today; failed runs do not count, so a retimed slot can replace a failed report |
| Same time in internal and partner lists | merged into one `both` run (`compute_trigger_slots` in `pipeline/schedule_utils.py`) |
| Window crossing midnight | candidate slots are built for every date the window touches |
| Invalid time cell (`25:00`, `bad`) | logged and skipped; other slots unaffected |
| Sheet unreachable at tick | task retries once, then the tick fails with a Slack alert; next tick starts fresh |
| Two runs of one tenant overlap | per-tenant lock in the metadata DB; the loser is a routine no-op, not a failure |
| Pod killed before releasing the lock | locks older than `DST_LOCK_TTL_MINUTES` (45) are stolen by the next claim; release is scoped to (tenant, run id) so stealing cannot break a live run |
| Malformed data/config (KeyError, ValueError, ...) | `AirflowFailException` — fail immediately, no retry (a retry fails identically) |
| Network/ES/Drive/Slack errors | normal raise -> `retries=2` with 3-minute delay |
| `cdd_sync` fails | non-fatal: report proceeds without sync data, audit row says "cdd_sync degraded" |
| Run fails mid-pipeline | checkpoints still upload to the campaign's Drive `temp/` folder (finally block) — debuggable from any machine via `rerun_from_checkpoint` |
| Row inactive / outside campaign window | routine no-op: marker `ok=None`, no audit row, no alert |
| 3 consecutive failed runs | `max_consecutive_failed_dag_runs=3` auto-pauses the DAG (failure alerts have already fired each time) |
| Manual trigger without conf | `execute_campaign_pipeline` fails fast with an explicit message |

## Where files live

The whole pipeline chain runs inside ONE task (`execute_campaign_pipeline`),
so all intermediate files stay on that task's local disk — pod-local under
KubernetesExecutor, no shared volume needed. Durable artifacts leave the pod
before it ends: reports and Excels to Google Drive (converted, link-shared),
checkpoints to the campaign's Drive `temp/` folder (raw, private), and the run
outcome to the `dst_campaign_runs` audit table. Airflow's own DB stores only
metadata and small XCom markers — never file contents.

## Configuration (Airflow Variables, set via the UI — never in git)

- `dst_groups` — JSON list of deployment groups:
  `[{"name": "nigeria_states", "sheet_tab": "Nigeria States", "env": {}}, ...]`
  In `env`: `null` REMOVES a variable (tenant-prefixed ES indices need
  `ES_INDEX_PREFIX` absent); `""` sets present-and-empty (Togo's un-prefixed
  cluster). Unset Variable = single default group from the process `.env`.
- `dst_secrets_<name>` — JSON dict of that group's secrets
  (`ES_USER`, `ES_PASS`, ...).
- Process env: `GOOGLE_SHEET_ID`, `GOOGLE_DRIVE_FOLDER_ID`, `SLACK_TOKEN`,
  `GROQ_API_KEY`, `DST_LOOKBACK_MINUTES`, `DST_LOCK_TTL_MINUTES`, and
  `PYTHONPATH` including the repo root (so `from pipeline import ...` resolves).

## Testing

- DAG parse check without a running Airflow:
  `docker run --rm --entrypoint bash -v <repo>:/repo:ro apache/airflow:3.1.6
   -c "export PYTHONPATH=/repo:/repo/dags && python -c 'import dst_campaign_scheduler, dst_campaign_run'"`
- End to end: point `dst_groups` at the `localtest` tab, unpause both DAGs,
  and watch one tick trigger one run.

## Audit and observability

- `dst_campaign_runs` (Airflow metadata DB): one row per real report attempt —
  tenant, slot, status, error, Drive link. Routine no-ops leave no row.
- Slack: every task failure alerts the campaign's own channel (falls back to
  `SLACK_CHANNEL`).
