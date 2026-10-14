# Databricks notebook source
# MAGIC %md
# MAGIC # Part 3 (bonus) - the same DQX rules, as code
# MAGIC
# MAGIC DQX Studio stores rules in the open DQX YAML/JSON format. This notebook applies the rule files in
# MAGIC `dqx_rules/` with the DQX library, which is exactly what Studio's task-runner job does - so rules
# MAGIC authored in the UI can be promoted to a CI/CD-managed job (or a pipeline) without rewriting them.
# MAGIC
# MAGIC Output per table: `<table>_dqx_valid` (clean rows) and `<table>_dqx_quarantine` (rows with
# MAGIC `_errors` / `_warnings` explaining every failed rule) in the silver schema.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("bronze_schema", "telco_dq_bronze")
dbutils.widgets.text("silver_schema", "telco_dq_silver")

catalog = dbutils.widgets.get("catalog")
bronze = dbutils.widgets.get("bronze_schema")
silver = dbutils.widgets.get("silver_schema")

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

dq_engine = DQEngine(WorkspaceClient())

TARGETS = {
    # rules file              -> input table, output prefix
    "customers.yaml": (f"{catalog}.{bronze}.customers_raw", f"{catalog}.{silver}.customers"),
    "usage_events.yaml": (f"{catalog}.{silver}.usage_events", f"{catalog}.{silver}.usage_events"),
    "invoices.yaml": (f"{catalog}.{silver}.invoices", f"{catalog}.{silver}.invoices"),
}


def load_rules(file_name):
    # Rule files use a <catalog> placeholder; point them at the catalog this job runs against
    text = open(f"../../dqx_rules/{file_name}").read().replace("<catalog>", catalog)
    checks = yaml.safe_load(text)
    status = dq_engine.validate_checks(checks)
    assert not status.has_errors, status.errors
    return checks

# COMMAND ----------

summary = []
for file_name, (source, prefix) in TARGETS.items():
    checks = load_rules(file_name)
    df = spark.read.table(source)
    valid, quarantine = dq_engine.apply_checks_by_metadata_and_split(df, checks)
    valid.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{prefix}_dqx_valid")
    quarantine.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{prefix}_dqx_quarantine")
    total = spark.read.table(source).count()
    bad = spark.read.table(f"{prefix}_dqx_quarantine").count()
    summary.append((source, len(checks), total, bad, round(bad / max(total, 1) * 100, 2)))

display(spark.createDataFrame(summary, "table string, rules int, rows long, quarantined long, pct double"))

# COMMAND ----------
# MAGIC %md ## Which rules fail the most?

# COMMAND ----------

failures = " UNION ALL ".join(
    f"SELECT '{prefix.split('.')[-1]}' AS table, '{kind}' AS level, e.name AS rule, count(*) AS rows "
    f"FROM {prefix}_dqx_quarantine LATERAL VIEW explode({col}) t AS e GROUP BY ALL"
    for _, prefix in TARGETS.values()
    for kind, col in (("error", "_errors"), ("warn", "_warnings"))
)
display(spark.sql(failures + " ORDER BY rows DESC"))
