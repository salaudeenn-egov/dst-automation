# Platform-side artifacts for DST_MODE=mdms

Everything the platform team needs to onboard the DST report automation in
mdms mode (zero database access from our side). Sheet mode needs none of this.

| File | What | Applied |
|---|---|---|
| `../dags/mdms_schema/dst-campaign-report-config.schema.json` | MDMS schema for the campaign-config mirror (synced from the Google Sheet by the dst_config_sync DAG) | once per environment root tenant, via the MDMS schema `_create` API |
| `dst-report-metadata-persister.yml` | egov-persister config consuming `save-dst-report-metadata` | added to the environment's persister configs |
| `dst_report_metadata.sql` | table the persister writes into | once, on the reporting database |

## Environment variables the Airflow deployment needs in mdms mode

```
DST_MODE=mdms
MDMS_URL=http://egov-mdms-service.egov:8080        # in-cluster; dummy authToken suffices
KAFKA_BROKER=<broker:9092>
DST_RUNS_TOPIC=save-dst-report-metadata            # default; tenant-prefixed on central instances
```

## Data flow in mdms mode

```
Google Sheet (humans edit)
  -> dst_config_sync DAG (every 10 min): create/update/deactivate MDMS entries
  -> MDMS: airflow-configs.dst-campaign-report-config
  -> dst_campaign_scheduler reads MDMS (sheet fallback on MDMS outage)
  -> dst_campaign_run executes the report (Drive + Slack)
  -> Kafka: save-dst-report-metadata (REPORT_COMPLETED / REPORT_FAILED events)
  -> egov-persister -> dst_report_metadata table
```

No component of ours reads or writes any database; the persister owns the
one DB write, exactly like the platform's own hcm report automation.
