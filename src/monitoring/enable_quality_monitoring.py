# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1 warm-up - Unity Catalog data quality monitoring
# MAGIC
# MAGIC The demo itself is done **in Catalog Explorer** (see `docs/DEMO_GUIDE.md`). This notebook only
# MAGIC pre-enables monitoring so there is history to show when the demo starts:
# MAGIC
# MAGIC | Capability | Scope | Enabled here on | Left for the live demo |
# MAGIC |---|---|---|---|
# MAGIC | **Anomaly detection** (freshness + completeness, no config) | schema | `silver`, `gold` | `bronze` (toggle it live) |
# MAGIC | **Data profiling** (stats, drift, dashboard) | table | `silver.usage_events` (time series), `silver.dim_customers` (snapshot), `gold.daily_usage_by_state` (time series) | `silver.invoices` (create live) |
# MAGIC
# MAGIC Both are the same API surface (`/api/data-quality/v1/monitors`) the UI uses.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("bronze_schema", "telco_dq_bronze")
dbutils.widgets.text("silver_schema", "telco_dq_silver")
dbutils.widgets.text("gold_schema", "telco_dq_gold")
dbutils.widgets.text("monitoring_schema", "telco_dq_monitoring")

catalog = dbutils.widgets.get("catalog")
silver = dbutils.widgets.get("silver_schema")
gold = dbutils.widgets.get("gold_schema")
monitoring = dbutils.widgets.get("monitoring_schema")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorCronSchedule, MonitorSnapshot, MonitorTimeSeries

w = WorkspaceClient()
me = w.current_user.me().user_name

# COMMAND ----------
# MAGIC %md ## Anomaly detection on schemas

# COMMAND ----------

for schema in (silver, gold):
    schema_id = w.schemas.get(f"{catalog}.{schema}").schema_id
    try:
        w.api_client.do(
            "POST",
            "/api/data-quality/v1/monitors",
            body={"object_type": "schema", "object_id": schema_id, "anomaly_detection_config": {}},
        )
        print(f"anomaly detection enabled: {catalog}.{schema}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"anomaly detection already enabled: {catalog}.{schema}")
        else:
            raise

# COMMAND ----------
# MAGIC %md ## Data profiling on tables

# COMMAND ----------

daily_6am = MonitorCronSchedule(quartz_cron_expression="0 0 6 * * ?", timezone_id="America/Sao_Paulo")

PROFILES = {
    f"{catalog}.{silver}.usage_events": dict(
        time_series=MonitorTimeSeries(timestamp_col="event_ts", granularities=["1 day"]),
        slicing_exprs=["event_type"],
    ),
    f"{catalog}.{silver}.dim_customers": dict(
        snapshot=MonitorSnapshot(),
        slicing_exprs=["segment"],
    ),
    f"{catalog}.{gold}.daily_usage_by_state": dict(
        time_series=MonitorTimeSeries(timestamp_col="event_date", granularities=["1 day"]),
    ),
}

for table, cfg in PROFILES.items():
    common = dict(output_schema_name=f"{catalog}.{monitoring}", schedule=daily_6am, **cfg)
    try:
        w.quality_monitors.create(
            table_name=table, assets_dir=f"/Workspace/Users/{me}/telco_dq_demo/monitoring", **common
        )
        print(f"profile created: {table}")
    except Exception as e:
        if "already exist" not in str(e).lower():
            raise
        w.quality_monitors.update(table_name=table, **common)
        print(f"profile updated: {table}")
    try:
        w.quality_monitors.run_refresh(table_name=table)
        print("  refresh triggered")
    except Exception as e:
        # A new monitor queues its first refresh automatically; don't stack another one
        if "PENDING" not in str(e).upper():
            raise
        print("  a refresh is already queued")

# COMMAND ----------
# MAGIC %md
# MAGIC Profile refreshes take a few minutes. Results:
# MAGIC * Catalog Explorer -> table -> **Quality** tab (profile, drift, generated dashboard)
# MAGIC * `<catalog>.telco_dq_monitoring.*_profile_metrics` / `*_drift_metrics`
# MAGIC * Schema -> **Details/Quality** -> anomaly detection results (freshness / completeness per table)
