-- Gold: business aggregates (materialized views).
-- Expectations also work on MVs: FAIL protects KPIs consumed by finance, WARN surfaces
-- referential issues that row-level rules in silver cannot see.

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.fact_usage (
  CONSTRAINT known_customer EXPECT (segment IS NOT NULL)
)
COMMENT 'Usage events enriched with customer and plan attributes'
AS SELECT
  u.event_id,
  u.event_ts,
  u.event_date,
  u.customer_id,
  u.msisdn,
  u.event_type,
  u.duration_sec,
  u.data_mb,
  u.charge_brl,
  u.roaming,
  u.device_type,
  coalesce(u.uf, c.uf) AS uf,
  c.segment,
  c.plan_id
FROM ${catalog}.${silver_schema}.usage_events u
LEFT JOIN ${catalog}.${silver_schema}.dim_customers c USING (customer_id);

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.daily_usage_by_state (
  CONSTRAINT non_negative_revenue EXPECT (prepaid_revenue_brl >= 0) ON VIOLATION FAIL UPDATE
)
COMMENT 'Daily traffic and prepaid revenue per state and service'
AS SELECT
  event_date,
  coalesce(uf, 'UNKNOWN')                     AS uf,
  event_type,
  count(*)                                    AS events,
  count(DISTINCT customer_id)                 AS active_customers,
  round(sum(duration_sec) / 60, 1)            AS voice_minutes,
  round(sum(data_mb) / 1024, 2)               AS data_gb,
  round(sum(charge_brl), 2)                   AS prepaid_revenue_brl
FROM ${catalog}.${gold_schema}.fact_usage
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.monthly_billing (
  CONSTRAINT non_negative_billed EXPECT (billed_brl >= 0) ON VIOLATION FAIL UPDATE
)
COMMENT 'Billing KPIs per month and segment'
AS SELECT
  i.billing_month,
  coalesce(c.segment, 'UNKNOWN')                                AS segment,
  count(*)                                                      AS invoices,
  round(sum(i.amount_brl), 2)                                   AS billed_brl,
  round(sum(CASE WHEN i.status = 'PAID' THEN i.amount_brl END), 2) AS collected_brl,
  count_if(i.status = 'OVERDUE')                                AS overdue_invoices,
  round(count_if(i.status = 'OVERDUE') / count(*) * 100, 2)     AS overdue_pct
FROM ${catalog}.${silver_schema}.invoices i
LEFT JOIN ${catalog}.${silver_schema}.dim_customers c USING (customer_id)
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.customer_360
COMMENT 'One row per customer: profile, last-30-day usage and billing status'
AS WITH usage_30d AS (
  SELECT customer_id,
         count(*)                         AS events_30d,
         round(sum(data_mb) / 1024, 2)    AS data_gb_30d,
         round(sum(duration_sec) / 60, 1) AS voice_min_30d,
         max(event_ts)                    AS last_event_ts
  FROM ${catalog}.${silver_schema}.usage_events
  WHERE event_ts >= current_timestamp() - INTERVAL 30 DAYS
  GROUP BY customer_id
), billing AS (
  SELECT customer_id,
         count_if(status = 'OVERDUE')     AS overdue_invoices,
         round(sum(amount_brl), 2)        AS billed_total_brl
  FROM ${catalog}.${silver_schema}.invoices
  GROUP BY customer_id
)
SELECT c.customer_id, c.full_name, c.uf, c.segment, c.plan_id, p.plan_name, p.monthly_fee_brl,
       u.events_30d, u.data_gb_30d, u.voice_min_30d, u.last_event_ts,
       b.overdue_invoices, b.billed_total_brl
FROM ${catalog}.${silver_schema}.dim_customers c
LEFT JOIN ${catalog}.${silver_schema}.plans p USING (plan_id)
LEFT JOIN usage_30d u USING (customer_id)
LEFT JOIN billing b USING (customer_id);
