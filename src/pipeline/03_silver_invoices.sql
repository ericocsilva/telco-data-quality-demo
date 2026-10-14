-- Silver (SQL API): the same expectation semantics with SQL syntax.
--   CONSTRAINT ... EXPECT (...)                          -> WARN
--   CONSTRAINT ... EXPECT (...) ON VIOLATION DROP ROW    -> DROP
--   CONSTRAINT ... EXPECT (...) ON VIOLATION FAIL UPDATE -> FAIL
CREATE OR REFRESH STREAMING TABLE invoices (
  CONSTRAINT invoice_id_not_null EXPECT (invoice_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_amount     EXPECT (amount_brl IS NOT NULL AND amount_brl > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_status        EXPECT (status IN ('PAID', 'OPEN', 'OVERDUE', 'CANCELLED')) ON VIOLATION DROP ROW,
  CONSTRAINT due_after_issue     EXPECT (due_date >= issue_date),
  CONSTRAINT paid_after_issue    EXPECT (paid_date IS NULL OR paid_date >= issue_date),
  CONSTRAINT paid_has_method     EXPECT (status <> 'PAID' OR payment_method IS NOT NULL)
)
COMMENT 'Validated invoices from billing'
AS SELECT
  invoice_id,
  customer_id,
  billing_month,
  to_date(issue_date) AS issue_date,
  to_date(due_date)   AS due_date,
  to_date(paid_date)  AS paid_date,
  amount_brl,
  status,
  payment_method,
  _source_file
FROM STREAM(${catalog}.${bronze_schema}.invoices_raw);
