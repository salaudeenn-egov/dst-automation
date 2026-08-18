-- One-time table creation for the DST report automation lifecycle events,
-- written by egov-persister (platform/dst-report-metadata-persister.yml)
-- from the save-dst-report-metadata Kafka topic. Run once per environment
-- against the reporting database (same DB that holds hcm_report_metadata).
--
-- Event-sourced: one row PER LIFECYCLE EVENT, not per run. The current state
-- of a run is its row with the highest status_order (40 = terminal), same
-- convention as the platform's hcm report events.

CREATE TABLE IF NOT EXISTS dst_report_metadata (
    id                  SERIAL PRIMARY KEY,
    event_id            VARCHAR(64)  NOT NULL UNIQUE,
    tenant_id           VARCHAR(64)  NOT NULL,
    state_name          VARCHAR(128),
    campaign_identifier VARCHAR(128) NOT NULL,
    report_name         VARCHAR(100) NOT NULL,
    trigger_frequency   VARCHAR(32),
    mode                VARCHAR(16),
    slot_date           VARCHAR(10),
    slot_time           VARCHAR(5),
    day                 VARCHAR(8),
    dag_run_id          VARCHAR(250) NOT NULL,
    dag_name            VARCHAR(100) NOT NULL,
    status              VARCHAR(32)  NOT NULL,
    status_order        INTEGER      NOT NULL DEFAULT 0,
    error_message       VARCHAR(1000),
    drive_link          VARCHAR(500),
    timestamp_ms        BIGINT       NOT NULL,
    event_timestamp     VARCHAR(40)
);

-- "Latest state of a run" and "today's runs for a campaign" are the two
-- read patterns.
CREATE INDEX IF NOT EXISTS idx_dst_report_metadata_run
    ON dst_report_metadata (dag_run_id, status_order DESC);
CREATE INDEX IF NOT EXISTS idx_dst_report_metadata_campaign_day
    ON dst_report_metadata (tenant_id, campaign_identifier, slot_date);
