-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Part 2 - SDP expectation metrics from the pipeline event log
-- MAGIC
-- MAGIC The pipeline publishes its event log to `telco_dq_silver.sdp_event_log` (see `resources/pipeline.yml`).
-- MAGIC Every `flow_progress` event carries pass/fail counts per expectation, so expectation results are
-- MAGIC plain SQL - ready for an AI/BI dashboard, an alert or Genie.
-- MAGIC
-- MAGIC Attach to serverless compute or a SQL warehouse and change the catalog below if needed.

-- COMMAND ----------

USE CATALOG main; -- change to the catalog you deployed to

-- COMMAND ----------

-- MAGIC %md ## 1. Pass / fail per expectation, per update (latest first)

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW expectations AS
SELECT
  timestamp,
  origin.update_id,
  origin.flow_name                     AS dataset,
  e.name                               AS expectation,
  e.passed_records,
  e.failed_records,
  round(e.failed_records / nullif(e.passed_records + e.failed_records, 0) * 100, 2) AS failed_pct
FROM telco_dq_silver.sdp_event_log
LATERAL VIEW explode(
  from_json(details:flow_progress:data_quality:expectations,
            'array<struct<name:string, dataset:string, passed_records:bigint, failed_records:bigint>>')
) t AS e
WHERE event_type = 'flow_progress'
  AND details:flow_progress:data_quality:expectations IS NOT NULL;

SELECT * FROM expectations ORDER BY timestamp DESC, failed_records DESC;

-- COMMAND ----------

-- MAGIC %md ## 2. Totals per expectation (all updates) - which rules hurt the most?

-- COMMAND ----------

SELECT dataset, expectation,
       sum(passed_records) AS passed,
       sum(failed_records) AS failed,
       round(sum(failed_records) / nullif(sum(passed_records + failed_records), 0) * 100, 2) AS failed_pct
FROM expectations
GROUP BY ALL
ORDER BY failed DESC;

-- COMMAND ----------

-- MAGIC %md ## 3. Dropped rows per update (rows written vs. rows dropped)

-- COMMAND ----------

SELECT timestamp,
       origin.flow_name                                     AS dataset,
       details:flow_progress:metrics:num_output_rows::bigint AS output_rows,
       details:flow_progress:data_quality:dropped_records::bigint AS dropped_rows
FROM telco_dq_silver.sdp_event_log
WHERE event_type = 'flow_progress'
  AND details:flow_progress:status = 'COMPLETED'
  AND details:flow_progress:data_quality IS NOT NULL
ORDER BY timestamp DESC;

-- COMMAND ----------

-- MAGIC %md ## 4. What was dropped? (quarantine pattern)

-- COMMAND ----------

SELECT rule, count(*) AS rows
FROM telco_dq_silver.usage_events_quarantine LATERAL VIEW explode(failed_rules) t AS rule
GROUP BY ALL
ORDER BY rows DESC;

-- COMMAND ----------

SELECT * FROM telco_dq_silver.usage_events_quarantine ORDER BY _ingested_at DESC LIMIT 50;
