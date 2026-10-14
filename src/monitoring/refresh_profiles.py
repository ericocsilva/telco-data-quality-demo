# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh UC data profiles
# MAGIC Triggers a refresh on every data-profiling monitor under the demo schemas so a freshly
# MAGIC injected incident (e.g. `null_spike`) shows up on the Quality tab without waiting for the
# MAGIC daily schedule.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("silver_schema", "telco_dq_silver")
dbutils.widgets.text("gold_schema", "telco_dq_gold")

catalog = dbutils.widgets.get("catalog")
schemas = [dbutils.widgets.get("silver_schema"), dbutils.widgets.get("gold_schema")]

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

for schema in schemas:
    for t in w.tables.list(catalog_name=catalog, schema_name=schema):
        try:
            w.quality_monitors.get(table_name=t.full_name)
        except Exception:
            continue  # no profile on this table
        try:
            w.quality_monitors.run_refresh(table_name=t.full_name)
            print(f"refresh triggered: {t.full_name}")
        except Exception as e:
            if "PENDING" not in str(e).upper():
                raise
            print(f"refresh already queued: {t.full_name}")
