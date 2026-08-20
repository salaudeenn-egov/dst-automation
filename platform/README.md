# Platform-side artifacts for DST_MODE=mdms

Everything the platform team needs to onboard the DST report automation in
mdms mode (zero database access from our side). Sheet mode needs none of this.

| File | What | Applied |
|---|---|---|
| `../dags/mdms_schema/dst-campaign-report-config.schema.json` | MDMS schema for the campaign-config mirror (synced from the Google Sheet by the dst_config_sync DAG) | once per environment root tenant, via the MDMS schema `_create` API |
| `dst-report-metadata-persister.yml` | egov-persister config consuming `save-dst-report-metadata` | added to the environment's persister configs |
| `dst_report_metadata.sql` | table the persister writes into (16 columns; every one varies per run) | once, on the reporting database |

## Environment variables the Airflow deployment needs in mdms mode

```
DST_MODE=mdms
MDMS_URL=http://egov-mdms-service.egov:8080        # in-cluster; dummy authToken suffices
KAFKA_BROKER=<broker:9092>
DST_RUNS_TOPIC=save-dst-report-metadata            # default
IS_CENTRAL_INSTANCE_ENABLED=false                  # true -> topic becomes {tenant}-save-dst-report-metadata,
                                                   #   and the persister fromTopic MUST list each prefixed topic
```

Also required in the image: `kafka-python`. Without it the producer is a
silent no-op and the audit table stays empty while every DAG run goes green.

## Data flow in mdms mode

```
Google Sheet (humans edit)
  -> dst_config_sync DAG (every 10 min): create/update/deactivate MDMS entries
  -> MDMS: airflow-configs.dst-campaign-report-config
  -> dst_campaign_scheduler reads MDMS (sheet fallback on MDMS outage)
  -> dst_campaign_run executes the report (Drive + Slack)
  -> Kafka: save-dst-report-metadata (REPORT_COMPLETED / REPORT_FAILED)
  -> egov-persister -> dst_report_metadata table
```

One row per report attempt, keyed by (tenant, campaign_identifier,
cycle_index, slot_date, slot_time). `mode` is both | internal | partner |
cumulative, the last being the whole-campaign report fired at 23:59 on the
row's mopup_end_date. `drive_folder_url` is the campaign Drive folder, which
holds every artifact the run published; `step_failed` names the stage that
died, with the exception text left in the Airflow task log under `dag_run_id`.
Redelivery is safe: the INSERT is ON CONFLICT (event_id) DO NOTHING.

No component of ours reads or writes any database; the persister owns the
one DB write, exactly like the platform's own hcm report automation.
