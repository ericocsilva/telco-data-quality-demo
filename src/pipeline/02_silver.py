# Silver (Python API): typed, validated tables.
#
# Shows the three expectation actions side by side:
#   expect / expect_all                 -> WARN: keep the row, record the violation
#   expect_or_drop / expect_all_or_drop -> DROP: filter the row out, record the violation
#   expect_or_fail / expect_all_or_fail -> FAIL: stop the update (bad reference data must never flow)
# plus the quarantine pattern (keep dropped rows somewhere they can be fixed / replayed) and
# rules kept as data (a dict that could just as well be loaded from a Delta table).
from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")
bronze = spark.conf.get("bronze_schema")

UFS = "('AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO')"

# ---------------------------------------------------------------------------
# Plans: reference data -> FAIL if broken
# ---------------------------------------------------------------------------


@dp.materialized_view(name="plans", comment="Telco plan catalog (reference data)")
@dp.expect_all_or_fail({
    "plan_id_not_null": "plan_id IS NOT NULL",
    "valid_plan_segment": "segment IN ('PRE', 'CONTROLE', 'POS')",
    "non_negative_fee": "monthly_fee_brl >= 0",
})
def plans():
    return spark.read.table(f"{catalog}.{bronze}.plans_raw").drop("_rescued_data")


# ---------------------------------------------------------------------------
# Customers: validated change feed + SCD1 dimension via AUTO CDC
# ---------------------------------------------------------------------------

CUSTOMER_DROP = {
    "customer_id_not_null": "customer_id IS NOT NULL",
    "updated_at_not_null": "updated_at IS NOT NULL",
}
CUSTOMER_WARN = {
    "valid_cpf_format": r"cpf RLIKE '^[0-9]{3}\\.[0-9]{3}\\.[0-9]{3}-[0-9]{2}$' AND cpf <> '000.000.000-00'",
    "valid_email": r"email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'",
    "valid_uf": f"uf IN {UFS}",
    "valid_segment": "segment IN ('PRE', 'CONTROLE', 'POS')",
    "adult_customer": "birth_date <= add_months(current_date(), -12 * 18)",
}


@dp.table(name="customers_changes", comment="CRM change feed, typed and validated")
@dp.expect_all_or_drop(CUSTOMER_DROP)
@dp.expect_all(CUSTOMER_WARN)
def customers_changes():
    return (
        spark.readStream.table(f"{catalog}.{bronze}.customers_raw")
        .select(
            "customer_id", "cpf", "full_name", F.lower("email").alias("email"), "msisdn",
            F.upper(F.trim("uf")).alias("uf"), F.upper("segment").alias("segment"), "plan_id",
            F.to_date("birth_date").alias("birth_date"),
            F.to_timestamp("created_at").alias("created_at"),
            F.to_timestamp("updated_at").alias("updated_at"),
            "_source_file",
        )
    )


dp.create_streaming_table(name="dim_customers", comment="Current state of each customer (SCD type 1)")
dp.create_auto_cdc_flow(
    target="dim_customers",
    source="customers_changes",
    keys=["customer_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type=1,
)

# ---------------------------------------------------------------------------
# Usage events: DROP invalid CDRs, WARN on incomplete ones, quarantine the dropped rows
# ---------------------------------------------------------------------------

USAGE_DROP = {
    "event_id_not_null": "event_id IS NOT NULL",
    "valid_msisdn": "msisdn RLIKE '^55[1-9][0-9]9[0-9]{8}$'",
    "valid_event_type": "event_type IN ('DATA', 'VOICE', 'SMS')",
    "non_negative_duration": "duration_sec IS NOT NULL AND duration_sec >= 0",
    "non_negative_charge": "charge_brl IS NOT NULL AND charge_brl >= 0",
    "event_not_in_future": "event_ts IS NOT NULL AND event_ts <= current_timestamp() + INTERVAL 1 HOUR",
}
USAGE_WARN = {
    "has_cell_id": "cell_id IS NOT NULL",
    "has_device_type": "device_type IS NOT NULL",
}


def typed_usage():
    return (
        spark.readStream.table(f"{catalog}.{bronze}.usage_events_raw")
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("uf", F.when(F.col("cell_id").isNotNull(), F.split("cell_id", "-")[0]))
    )


@dp.table(name="usage_events", comment="Validated usage events (CDR/xDR)", cluster_by=["event_date"])
@dp.expect_all_or_drop(USAGE_DROP)
@dp.expect_all(USAGE_WARN)
def usage_events():
    return typed_usage().drop("_rescued_data")


@dp.table(name="usage_events_quarantine", comment="Usage events rejected by silver expectations, with the failed rules")
def usage_events_quarantine():
    all_valid = " AND ".join(f"coalesce({rule}, false)" for rule in USAGE_DROP.values())
    failed_rules = F.filter(
        F.array(*[F.when(~F.coalesce(F.expr(rule), F.lit(False)), F.lit(name)) for name, rule in USAGE_DROP.items()]),
        lambda x: x.isNotNull(),
    )
    return typed_usage().where(f"NOT ({all_valid})").withColumn("failed_rules", failed_rules)
