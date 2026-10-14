# Demo guide — Data Quality on Databricks for Telco (≈ 45 min)

The script has three parts, ordered from *zero effort* to *full control*, all on the same mock telco data:

| Part | Capability | Time | Story line |
|---|---|---|---|
| 1 | **UC data quality monitoring** (Catalog Explorer) | 12 min | "Turn it on and know when something breaks, without writing a single rule." |
| 2 | **SDP expectations** | 15 min | "Engineers declare the rules where data is written: warn, drop or fail." |
| 3 | **DQX Studio** | 15 min | "Business stewards own the rules for any table, with approvals, schedules and quarantine." |
| — | Wrap-up | 3 min | Complementary layers: prevent → validate → observe |

Workspace: `<workspace-url>` · Catalog: `<catalog>`
DQX Studio: `<dqx-studio-app-url>`

---

## 0. Preparation (the day before)

1. Deploy and bootstrap (≈ 15 min). See the [README](../README.md#5-deploy):
   ```bash
   databricks bundle deploy && databricks bundle run telco_dq_bootstrap
   ```
2. Leave the **Telco DQ - 2. Hourly ingest** job **unpaused** for at least a few hours (ideally 24h) so anomaly detection learns the hourly commit pattern of every table.
3. **Stage a freshness incident** (≈ 3–4 h before the demo): open the job → *Edit* → *Job parameters* and set `skip_entities = invoices`. From then on the hourly runs stop delivering billing data, and `bronze.invoices_raw` / `silver.invoices` go stale. (Remember to clear the parameter after the demo.)
4. **Stage a volume/null incident** (≈ 1 h before): *Run now with different parameters* → `scenario = null_spike`, `refresh_profiles = true`. Then run again with `scenario = volume_drop`.
5. Open these browser tabs:
   - Catalog Explorer → `<catalog>` → `telco_dq_silver`
   - Jobs & Pipelines → **Telco DQ - SDP Medallion Pipeline (Expectations)**
   - Workspace → `.bundle/telco-data-quality-demo/demo/files/src/pipeline/02_silver.py`
   - Notebook `src/monitoring/sdp_expectations_report.sql` (attached to *Serverless Starter Warehouse*)
   - DQX Studio
   - `dqx_rules/*.yaml` from this repo (to paste)
6. Sanity check: in DQX Studio, verify you can browse `telco_dq_bronze` (the bundle grants `SELECT` to the Studio service principals).

> **Live fallback:** if you could not warm up, every incident can still be injected live (*Run now with different parameters* on the ingest job). Profiling and expectations react within minutes; anomaly detection needs history, so show its results on `silver`/`gold` (enabled during bootstrap) and explain the learning period.

---

## Part 1 — Unity Catalog data quality monitoring (no code, ≈ 12 min)

**Message:** *"Monitoring is a property of the catalog, not of a pipeline. One switch covers every table in a schema, including tables loaded by tools you don't control."*

### 1.1 Turn on anomaly detection for a whole schema (live)
1. Catalog Explorer → `<catalog>` → **`telco_dq_bronze`** → **Details** tab.
2. Next to **Data Quality Monitoring** click **Enable** → keep **Anomaly detection** on → **Save**.
3. Talk track:
   - No thresholds, no rules, no per-table config. It learns each table's commit pattern and volume from history.
   - Two checks today: **Freshness** (is the next commit unusually late?) and **Completeness** (rows written in the last 24h below the predicted range?).
   - It needs `MANAGE` on the schema and runs on serverless.
   - A scan starts right away; the history-based model gets better over time.

### 1.2 Show results on a schema that was already monitored
1. Open **`telco_dq_silver`** → Details → **Data Quality Monitoring → View results**.
2. Point at the **health indicators** per table:
   - `invoices` → **Stale** (the billing feed stopped, see prep step 3). *"Nobody wrote a freshness rule. The platform knew this table normally updates every hour."*
   - `usage_events` → **Incomplete** after the `volume_drop` batch (5% of normal volume). *"A mediation outage would show up here before finance sees the revenue gap."*
3. Open the table → **Quality** tab: the same status at table level, plus the downstream tables affected (lineage).
4. Optional SQL (shows it's all queryable / alertable):
   ```sql
   SELECT * FROM system.data_quality_monitoring.table_results
   WHERE catalog_name = '<catalog>' AND schema_name LIKE 'telco_dq_%'
   ORDER BY event_time DESC;
   ```

### 1.3 Data profiling: statistics and drift on a key table
1. Catalog Explorer → `telco_dq_silver.usage_events` → **Quality** tab → data profile (time series on `event_ts`, sliced by `event_type`, created by the bootstrap job).
2. Open the **dashboard** (auto-generated AI/BI):
   - Row count per day: the `volume_drop` dip.
   - **Null %** of `cell_id` / `device_type`: jumps from ~1% to ~60% on the `null_spike` batch. *"This row was valid (cell_id is optional, it's a WARN expectation), so it went through the pipeline. Profiling shows the drift."*
   - Distribution drift on `data_mb`, `duration_sec`.
3. Show the metric tables in `telco_dq_monitoring` (`usage_events_profile_metrics`, `usage_events_drift_metrics`). They can feed SQL alerts and Genie.
4. **Create one live** to show it's 3 clicks: `telco_dq_silver.invoices` → Quality → **Create** (enable data profiling) → profile type **Snapshot** (or Time series on `issue_date`) → Create. (Takes a few minutes to compute; come back to it at the end.)

**Transition:** *"Monitoring tells you something is wrong with a table. To stop specific bad records from getting in, you put rules where the data is written. That's the pipeline."*

---

## Part 2 — SDP expectations (≈ 15 min)

**Message:** *"Data quality as code, in the same file as the transformation. Three actions: warn, drop, fail. Metrics for free."*

### 2.1 The pipeline graph
1. Open **Telco DQ - SDP Medallion Pipeline (Expectations)** → latest update → graph:
   `bronze.*_raw` (Auto Loader) → `silver.usage_events` / `usage_events_quarantine` / `customers_changes` → `dim_customers` (AUTO CDC) / `invoices` / `plans` → `gold.*`.
2. Click **`usage_events`** → **Data quality** panel on the right: each expectation with passed/failed records and the action. Roughly 2% of CDRs are dropped (invalid MSISDN, future timestamp, negative duration/charge, unknown event type).

### 2.2 The code: three actions
Open `src/pipeline/02_silver.py`:

| Action | Where in the demo | Code |
|---|---|---|
| **WARN** (keep + count) | `has_cell_id`, `has_device_type`; customer CPF/e-mail/UF | `@dp.expect_all(USAGE_WARN)` |
| **DROP** (filter out) | invalid CDRs | `@dp.expect_all_or_drop(USAGE_DROP)` |
| **FAIL** (stop the update) | plan catalog (reference data), gold KPIs | `@dp.expect_all_or_fail(...)` / `ON VIOLATION FAIL UPDATE` |

Points to make:
- Rules are a **Python dict**, so they can live in a Delta table or YAML and be shared across pipelines (metadata-driven).
- The **quarantine pattern** (`usage_events_quarantine`): same rules, inverted, plus a `failed_rules` array. Nothing is lost; data stewards can fix and replay.
- Open `03_silver_invoices.sql`: the **same semantics in SQL** (`CONSTRAINT ... EXPECT ... ON VIOLATION DROP ROW`).
- Open `04_gold.sql`: expectations on **materialized views**. `FAIL UPDATE` protects finance KPIs, and `known_customer` (WARN) after a join shows referential issues.

### 2.3 Inject a bad batch live
1. Jobs → **Telco DQ - 2. Hourly ingest** → **Run now with different parameters** → `scenario = bad_data_burst`.
2. While it runs (≈ 3–4 min), open `sdp_expectations_report.sql`:
   - Query 1: pass/fail per expectation per update.
   - Query 2: totals: which rules hurt the most.
   - Query 4: the quarantine, grouped by failed rule.
3. When the update finishes, refresh the pipeline graph: the drop % on `usage_events` jumps from ~2% to ~25%, while **gold is still correct**.

### 2.4 (Optional) Schema drift
Run the ingest job with `scenario = schema_drift`. The upstream renames `charge_brl` → `charge_amount` and adds `network_tech`:
- Bronze keeps the data in `_rescued_data` (nothing lost), and the WARN expectation `no_rescued_data` lights up.
- Silver drops those rows via `non_negative_charge` (null charge), and they go to quarantine.

**Limitation to say out loud (it sets up Part 3):** expectations are **row-level** and **only inside SDP**. They can't express "event_id is unique", "every CDR belongs to a CRM customer", "invoice ≥ plan fee", and they don't cover tables loaded outside SDP.

---

## Part 3 — DQX Studio (≈ 15 min)

**Message:** *"A rules catalog for any table in Unity Catalog. Profile → generate → approve → schedule → review. Owned by data stewards, stored in the open DQX format, runs on serverless."*

### 3.1 Profile a raw table and generate rules
1. DQX Studio → **Profile & generate** → select `<catalog>.telco_dq_bronze.customers_raw` → **Run Profile** (serverless job, ~1–2 min; tell the OBO story while it runs: *"it only sees what you can see in UC"*).
2. Review the suggestions (null thresholds, value lists for `uf`/`segment`, ranges). Pick a few → save as **draft** rules.
3. Show **Generate with AI**: describe *"CPF must follow 000.000.000-00 and customers must be 18+"* → generated checks.

### 3.2 Import the curated rule set (YAML)
1. **Import Rules** → **DQX YAML** → paste `dqx_rules/customers.yaml` → target table `telco_dq_bronze.customers_raw`.
2. **Dry run**: valid / error / warning counts and a sample of failing rows with the reason per rule (bad CPF, invalid UF `XX`, minors, `PREMIUM` segment…).
   *"Same table the pipeline reads. DQX shows you every bad record and why, and you didn't have to touch the pipeline."*
3. **Submit for review**.

### 3.3 Rules that expectations can't express
1. **Tables** → **Monitor a table** → `telco_dq_silver.usage_events` → **Apply Rules** → import `dqx_rules/usage_events.yaml`:
   - `event_id_unique` (**is_unique**): duplicate CDRs from mediation retries passed every SDP row rule.
   - `customer_exists_in_crm` (**foreign_key** → `dim_customers`): orphan CDRs.
   - `voice_call_has_duration` (multi-column SQL expression), `valid_device_type`, `cell_id_present`, `data_session_under_10gb`.
2. **Cross-table rules** → paste the `amount_not_below_plan_fee` SQL from `dqx_rules/invoices.yaml` (invoice vs. customer's plan fee), or use **Generate with AI**: *"invoices in ...telco_dq_silver.invoices whose amount_brl is lower than the monthly_fee_brl of the customer's plan (join dim_customers and plans)"*.
3. **Dry run** each and show the per-rule metrics.

### 3.4 Governance: approve, schedule, review
1. **Review & Approve** (as `RULE_APPROVER`): approve the drafts. Show the rule **history / audit** and comments.
2. On the table → **Schedule** hourly. Scheduled runs use the app's service principal (*"no user at 3 AM"*), which is why the bundle grants it `SELECT` on the demo schemas.
3. **Runs History** → open a run: sample bad rows, **review status** (*Pending review → Acknowledged → Resolved / False positive*), comments, export quarantine.
4. **Results** / Insights dashboard (embedded AI/BI): error rate trend per table, rule and label. Run the ingest job with `bad_data_burst` once more and watch the next scheduled run jump.

### 3.5 From UI to code (1 min)
Open `src/dqx/dqx_as_code.py`: the **same YAML files** applied with the DQX library in a job (`apply_checks_by_metadata_and_split`). Rules authored in Studio can be exported and promoted through CI/CD, and DQX also runs inside SDP pipelines and streaming jobs. Show the result tables `telco_dq_silver.*_dqx_quarantine` with the `_errors` / `_warnings` columns.

---

## Wrap-up (≈ 3 min)

| | Prevent | Validate | Observe |
|---|---|---|---|
| Tool | SDP expectations | DQX / DQX Studio | UC data quality monitoring |
| Caught in this demo | invalid CDRs, broken plan catalog, negative KPIs | duplicates, orphans, invoice < plan fee, raw CRM issues | billing feed stopped, volume drop, null spike / drift |
| Owner | Data engineer | Data steward / business | Platform / data owner |

*"Start by switching on anomaly detection for every schema (free insight, zero effort). Add expectations to the pipelines you own. Use DQX Studio for business rules, for cross-table checks, and for everything you don't own. All of it is governed by Unity Catalog and queryable in SQL."*

---

## Reset after the demo

- Ingest job: clear `skip_entities`. Pause the schedule if you no longer need it (`databricks bundle deploy --var ingest_schedule_status=PAUSED`).
- Optionally disable anomaly detection on `telco_dq_bronze` and delete the live-created profile on `silver.invoices` (Quality tab).
- Full reset: `databricks bundle destroy`, then `deploy` + `run telco_dq_bootstrap`.

## Troubleshooting

| Symptom | Fix |
|---|---|
| Pipeline update fails with `EXPECTATION_VIOLATION` on `plans` / gold | A FAIL expectation tripped (that's the feature). Check the event log, fix the source, and re-run |
| DQX Studio can't see the demo tables / scheduled run fails with permission error | Check `dqx_app_sp` / `dqx_job_sp` in `databricks.yml` match the app SP and the task-runner job `run_as`, then redeploy |
| No anomaly detection result yet | Needs history (several hourly commits). Keep the ingest schedule unpaused longer |
| Profile dashboard empty | Refresh still running. Check the Quality tab → refresh history, or run the ingest job with `refresh_profiles = true` |
| `is_in_list` rule fails with `UNRESOLVED_COLUMN` | In DQX ≥ 0.16, `allowed` values are SQL expressions: quote literals (`"'SP'"`) |
