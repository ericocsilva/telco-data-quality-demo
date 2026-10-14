# Bronze: raw ingestion with Auto Loader.
#
# Bronze keeps *everything* (no rows dropped) so nothing is lost; expectations here are
# warn-only and act as an early signal. Schemas are declared explicitly and any unexpected
# column lands in `_rescued_data` -> the `no_rescued_data` expectation flags schema drift.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")
bronze = spark.conf.get("bronze_schema")
landing = spark.conf.get("landing_path")

SCHEMAS = {
    "plans": "plan_id STRING, plan_name STRING, segment STRING, monthly_fee_brl DOUBLE, data_gb INT",
    "customers": (
        "customer_id STRING, cpf STRING, full_name STRING, email STRING, msisdn STRING, uf STRING, "
        "segment STRING, plan_id STRING, birth_date STRING, created_at STRING, updated_at STRING"
    ),
    "usage_events": (
        "event_id STRING, customer_id STRING, msisdn STRING, event_ts STRING, event_type STRING, "
        "duration_sec BIGINT, data_mb DOUBLE, cell_id STRING, device_type STRING, roaming BOOLEAN, charge_brl DOUBLE"
    ),
    "invoices": (
        "invoice_id STRING, customer_id STRING, billing_month STRING, issue_date STRING, due_date STRING, "
        "paid_date STRING, amount_brl DOUBLE, status STRING, payment_method STRING"
    ),
}


def auto_loader(entity, fmt):
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .option("rescuedDataColumn", "_rescued_data")
        .schema(SCHEMAS[entity])
    )
    if fmt == "csv":
        reader = reader.option("header", "true")
    return (
        reader.load(f"{landing}/{entity}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


BRONZE_WARN = {"no_rescued_data": "_rescued_data IS NULL"}


@dp.table(name=f"{catalog}.{bronze}.plans_raw", comment="Raw product catalog (CSV)")
@dp.expect_all(BRONZE_WARN)
def plans_raw():
    return auto_loader("plans", "csv")


@dp.table(name=f"{catalog}.{bronze}.customers_raw", comment="Raw CRM extracts, new customers + updates (CSV)")
@dp.expect_all(BRONZE_WARN)
def customers_raw():
    return auto_loader("customers", "csv")


@dp.table(name=f"{catalog}.{bronze}.usage_events_raw", comment="Raw CDR/xDR usage events from mediation (JSON)")
@dp.expect_all(BRONZE_WARN)
def usage_events_raw():
    return auto_loader("usage_events", "json")


@dp.table(name=f"{catalog}.{bronze}.invoices_raw", comment="Raw invoices from billing (JSON)")
@dp.expect_all(BRONZE_WARN)
def invoices_raw():
    return auto_loader("invoices", "json")
