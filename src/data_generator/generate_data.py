# Databricks notebook source
# MAGIC %md
# MAGIC # Telco DQ demo - mock telco data generator
# MAGIC
# MAGIC Writes raw files to the landing volume, the same way an upstream system (BSS/OSS, mediation,
# MAGIC billing) would drop extracts. Every batch carries a small baseline of quality issues; the
# MAGIC `scenario` widget injects an incident on demand.
# MAGIC
# MAGIC | Entity | Format | Path | Source system it mimics |
# MAGIC |---|---|---|---|
# MAGIC | `plans` | CSV | `raw/plans/` | Product catalog (static, seed only) |
# MAGIC | `customers` | CSV | `raw/customers/` | CRM - new customers + updates (CDC-like) |
# MAGIC | `usage_events` | JSON lines | `raw/usage_events/` | Mediation platform - voice/SMS/data CDRs |
# MAGIC | `invoices` | JSON lines | `raw/invoices/` | Billing system |
# MAGIC
# MAGIC | Scenario | What it does | Who catches it |
# MAGIC |---|---|---|
# MAGIC | `seed` | 30 days of usage + 6 months of invoices, ~3% defects | baseline for everything |
# MAGIC | `normal` | one hour of data, ~3% defects | - |
# MAGIC | `bad_data_burst` | ~30% defective rows | SDP expectations (drop counts), DQX Studio |
# MAGIC | `null_spike` | 60% of `cell_id` / `device_type` null (warn-only in SDP, so it reaches silver) | UC data profiling (null % drift), DQX |
# MAGIC | `volume_drop` | 5% of the normal volume | UC anomaly detection (completeness) |
# MAGIC | `schema_drift` | upstream renames `charge_brl` -> `charge_amount` and adds `network_tech` | SDP `_rescued_data` expectation, DQX not-null |
# MAGIC | `skip_entities` | listed entities are not written | UC anomaly detection (freshness) |

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("landing_schema", "telco_dq_landing")
dbutils.widgets.dropdown(
    "scenario", "normal", ["seed", "normal", "bad_data_burst", "null_spike", "volume_drop", "schema_drift"]
)
dbutils.widgets.text("skip_entities", "")

catalog = dbutils.widgets.get("catalog")
landing_schema = dbutils.widgets.get("landing_schema")
scenario = dbutils.widgets.get("scenario")
skip_entities = {e.strip() for e in dbutils.widgets.get("skip_entities").split(",") if e.strip()}

base_path = f"/Volumes/{catalog}/{landing_schema}/raw"
print(f"scenario={scenario} skip={skip_entities or '-'} -> {base_path}")

# COMMAND ----------

import json
import os
import uuid
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

NOW = datetime.utcnow().replace(microsecond=0)
BATCH_ID = NOW.strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
rng = np.random.default_rng()

N_BASE_CUSTOMERS = 2000
EVENTS_PER_HOUR = 300
INVOICES_PER_DAY = N_BASE_CUSTOMERS // 30  # billing cycles spread over the month

DEFECT_RATE = {"bad_data_burst": 0.30}.get(scenario, 0.03)
VOLUME_FACTOR = {"volume_drop": 0.05}.get(scenario, 1.0)

# UF -> DDD used to build realistic MSISDNs; weights roughly follow the subscriber base
UF_DDD = {"SP": "11", "RJ": "21", "MG": "31", "RS": "51", "PR": "41", "BA": "71", "PE": "81",
          "CE": "85", "DF": "61", "SC": "48", "GO": "62", "PA": "91", "AM": "92", "ES": "27"}
UF_WEIGHTS = np.array([30, 12, 10, 6, 6, 6, 5, 4, 4, 4, 3, 4, 3, 3], dtype=float)
UF_WEIGHTS /= UF_WEIGHTS.sum()

FIRST = ["Ana", "Bruno", "Carla", "Diego", "Eduarda", "Felipe", "Gabriela", "Henrique", "Isabela", "João",
         "Larissa", "Marcos", "Natália", "Otávio", "Paula", "Rafael", "Sofia", "Thiago", "Vanessa", "Wagner"]
LAST = ["Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Ferreira", "Alves", "Pereira", "Lima",
        "Gomes", "Costa", "Ribeiro", "Martins", "Carvalho", "Almeida", "Lopes", "Barbosa", "Rocha"]

PLANS = [
    # plan_id, plan_name, segment, monthly_fee_brl, data_gb
    ("PRE-10", "Telco Pré Turbo 10GB", "PRE", 0.00, 10),
    ("PRE-25", "Telco Pré Turbo 25GB", "PRE", 0.00, 25),
    ("CTL-15", "Telco Controle 15GB", "CONTROLE", 59.99, 15),
    ("CTL-30", "Telco Controle 30GB", "CONTROLE", 79.99, 30),
    ("POS-50", "Telco Pós 50GB", "POS", 129.99, 50),
    ("POS-100", "Telco Pós 100GB", "POS", 179.99, 100),
    ("POS-200", "Telco Pós Família 200GB", "POS", 299.99, 200),
    ("SEL-ILIM", "Telco Ilimitado", "POS", 399.99, 999),
]
PLAN_IDS = [p[0] for p in PLANS]
SEGMENT_PLANS = {s: [p[0] for p in PLANS if p[2] == s] for s in ("PRE", "CONTROLE", "POS")}


def cpf(rand):
    """Brazilian CPF with valid check digits, formatted 000.000.000-00."""
    d = list(rand.integers(0, 10, 9))
    for _ in range(2):
        s = sum(v * w for v, w in zip(d, range(len(d) + 1, 1, -1)))
        d.append((s * 10 % 11) % 10)
    s = "".join(map(str, d))
    return f"{s[:3]}.{s[3:6]}.{s[6:9]}-{s[9:]}"


def defect_mask(n, rate=None):
    return rng.random(n) < (DEFECT_RATE if rate is None else rate)


def write_csv(df, entity):
    path = f"{base_path}/{entity}"
    os.makedirs(path, exist_ok=True)
    df.to_csv(f"{path}/{entity}_{BATCH_ID}.csv", index=False)
    print(f"  {entity}: {len(df):,} rows")


def write_jsonl(records, entity, suffix=""):
    path = f"{base_path}/{entity}"
    os.makedirs(path, exist_ok=True)
    with open(f"{path}/{entity}_{BATCH_ID}{suffix}.json", "w") as f:
        for r in records:
            f.write(json.dumps(r, default=str, ensure_ascii=False) + "\n")
    return len(records)

# COMMAND ----------
# MAGIC %md ## Base customer population (deterministic, so usage/invoices always reference the same customers)

# COMMAND ----------

base_rng = np.random.default_rng(42)
base_customers = []
for i in range(1, N_BASE_CUSTOMERS + 1):
    uf = base_rng.choice(list(UF_DDD), p=UF_WEIGHTS)
    segment = base_rng.choice(["PRE", "CONTROLE", "POS"], p=[0.45, 0.25, 0.30])
    first, last = base_rng.choice(FIRST), base_rng.choice(LAST)
    base_customers.append({
        "customer_id": f"C{i:07d}",
        "cpf": cpf(base_rng),
        "full_name": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}{i}@email.com.br".replace("ã", "a").replace("é", "e").replace("á", "a"),
        "msisdn": f"55{UF_DDD[uf]}9{base_rng.integers(10_000_000, 99_999_999)}",
        "uf": uf,
        "segment": segment,
        "plan_id": base_rng.choice(SEGMENT_PLANS[segment]),
        "birth_date": (date(1950, 1, 1) + timedelta(days=int(base_rng.integers(0, 365 * 55)))).isoformat(),
        "created_at": (datetime(2020, 1, 1) + timedelta(days=int(base_rng.integers(0, 365 * 5)))).isoformat(),
    })
customers_df = pd.DataFrame(base_customers)


def corrupt_customers(df):
    """Typical CRM issues: bad CPF/e-mail, invalid UF, minors, unknown segment, missing IDs."""
    df = df.copy()
    n = len(df)
    df.loc[defect_mask(n), "cpf"] = rng.choice(["123.456.789", "000.000.000-00", None, "CPF INVALIDO"])
    df.loc[defect_mask(n), "email"] = rng.choice(["sem-email", "joao@", None, "maria.silva#gmail.com"])
    df.loc[defect_mask(n), "uf"] = rng.choice(["XX", "S.P", "", None])
    df.loc[defect_mask(n), "segment"] = rng.choice(["PREMIUM", "pos", None])
    df.loc[defect_mask(n), "birth_date"] = rng.choice(["2015-03-10", "2031-01-01"])  # minor / future
    df.loc[defect_mask(n, DEFECT_RATE / 3), "customer_id"] = None
    return df

# COMMAND ----------
# MAGIC %md ## Usage events (CDR / xDR)

# COMMAND ----------

EVENT_TYPES = ["DATA", "VOICE", "SMS"]
DEVICE_TYPES = ["SMARTPHONE", "MODEM", "TABLET", "IOT"]


def usage_events(start, end, n):
    idx = rng.integers(0, N_BASE_CUSTOMERS, n)
    cust = customers_df.iloc[idx].reset_index(drop=True)
    secs = rng.integers(0, int((end - start).total_seconds()), n)
    etype = rng.choice(EVENT_TYPES, n, p=[0.6, 0.3, 0.1])
    duration = np.where(etype == "VOICE", rng.gamma(2.0, 90.0, n).round(0), 0).astype(int)
    data_mb = np.where(etype == "DATA", rng.gamma(1.5, 40.0, n).round(2), 0.0)
    roaming = rng.random(n) < 0.02
    charge = np.where(cust["segment"] == "PRE",
                      duration / 60 * 0.99 + data_mb * 0.01 + (etype == "SMS") * 0.25,
                      0.0).round(2)
    cell_null = 0.60 if scenario == "null_spike" else 0.01

    events = []
    for i in range(n):
        e = {
            "event_id": str(uuid.uuid4()),
            "customer_id": cust.at[i, "customer_id"],
            "msisdn": cust.at[i, "msisdn"],
            "event_ts": (start + timedelta(seconds=int(secs[i]))).isoformat(),
            "event_type": etype[i],
            "duration_sec": int(duration[i]),
            "data_mb": float(data_mb[i]),
            "cell_id": None if rng.random() < cell_null else f"{cust.at[i, 'uf']}-ERB-{rng.integers(1, 400):04d}",
            "device_type": None if rng.random() < cell_null else rng.choice(DEVICE_TYPES, p=[0.85, 0.05, 0.05, 0.05]),
            "roaming": bool(roaming[i]),
            "charge_brl": float(charge[i]),
        }
        if rng.random() < DEFECT_RATE:
            # Pick one defect per bad row so each issue type shows up in the metrics
            kind = rng.integers(0, 8)
            if kind == 0:
                e["msisdn"] = None
            elif kind == 1:
                e["duration_sec"] = -int(rng.integers(1, 500))
                e["event_type"] = "VOICE"
            elif kind == 2:
                e["event_ts"] = (NOW + timedelta(days=int(rng.integers(1, 30)))).isoformat()
            elif kind == 3:
                e["event_type"] = rng.choice(["MMS", "UNKNOWN", ""])
            elif kind == 4:
                e["charge_brl"] = -round(float(rng.random() * 50), 2)
            elif kind == 5:
                e["customer_id"] = f"C9{rng.integers(100000, 999999)}"  # orphan: not in CRM
            elif kind == 6:
                e["msisdn"] = "5511" + str(rng.integers(1000, 9999))  # truncated number
            elif kind == 7 and events:
                e["event_id"] = events[-1]["event_id"]  # duplicate from mediation retry
        if scenario == "schema_drift":
            e["charge_amount"] = e.pop("charge_brl")
            e["network_tech"] = rng.choice(["4G", "5G"])
        events.append(e)
    return events

# COMMAND ----------
# MAGIC %md ## Invoices (billing)

# COMMAND ----------

INVOICE_STATUS = ["PAID", "OPEN", "OVERDUE", "CANCELLED"]


def invoices_for_day(day, n):
    idx = rng.integers(0, N_BASE_CUSTOMERS, n)
    cust = customers_df.iloc[idx].reset_index(drop=True)
    fee = {p[0]: p[3] for p in PLANS}
    out = []
    for i in range(n):
        issue = day
        due = issue + timedelta(days=10)
        status = rng.choice(INVOICE_STATUS, p=[0.75, 0.12, 0.10, 0.03]) if (NOW.date() - issue).days > 10 else "OPEN"
        amount = round(fee[cust.at[i, "plan_id"]] + float(rng.gamma(2.0, 8.0)), 2)
        paid = issue + timedelta(days=int(rng.integers(1, 12))) if status == "PAID" else None
        inv = {
            "invoice_id": f"INV-{issue:%Y%m}-{uuid.uuid4().hex[:10].upper()}",
            "customer_id": cust.at[i, "customer_id"],
            "billing_month": issue.strftime("%Y-%m"),
            "issue_date": issue.isoformat(),
            "due_date": due.isoformat(),
            "paid_date": paid.isoformat() if paid else None,
            "amount_brl": amount,
            "status": status,
            "payment_method": rng.choice(["BOLETO", "PIX", "CARTAO", "DEBITO_AUTOMATICO"]) if paid else None,
        }
        if rng.random() < DEFECT_RATE:
            kind = rng.integers(0, 6)
            if kind == 0:
                inv["amount_brl"] = rng.choice([0.0, -round(amount, 2)])
            elif kind == 1:
                inv["due_date"] = (issue - timedelta(days=5)).isoformat()
            elif kind == 2:
                inv["status"] = rng.choice(["PAGO", "pending", ""])
            elif kind == 3:
                inv["paid_date"] = (issue - timedelta(days=3)).isoformat()
                inv["status"] = "PAID"
            elif kind == 4:
                inv["invoice_id"] = None
            elif kind == 5 and fee[cust.at[i, "plan_id"]] > 0:
                inv["amount_brl"] = round(fee[cust.at[i, "plan_id"]] * 0.5, 2)  # discount applied twice
        out.append(inv)
    return out

# COMMAND ----------
# MAGIC %md ## Write the batch

# COMMAND ----------

print(f"Batch {BATCH_ID}")

if scenario == "seed":
    if "plans" not in skip_entities:
        write_csv(pd.DataFrame(PLANS, columns=["plan_id", "plan_name", "segment", "monthly_fee_brl", "data_gb"]), "plans")

    if "customers" not in skip_entities:
        seed = customers_df.copy()
        seed["updated_at"] = seed["created_at"]
        write_csv(corrupt_customers(seed), "customers")

    if "usage_events" not in skip_entities:
        total = 0
        start_day = NOW.replace(hour=0, minute=0, second=0) - timedelta(days=30)
        for d in range(30):
            day_start = start_day + timedelta(days=d)
            total += write_jsonl(usage_events(day_start, day_start + timedelta(days=1), EVENTS_PER_HOUR * 24),
                                 "usage_events", suffix=f"_d{d:02d}")
        total += write_jsonl(usage_events(start_day + timedelta(days=30), NOW, EVENTS_PER_HOUR * max(NOW.hour, 1)),
                             "usage_events", suffix="_today")
        print(f"  usage_events: {total:,} rows")

    if "invoices" not in skip_entities:
        records = []
        for d in range(180, -1, -1):
            records += invoices_for_day(NOW.date() - timedelta(days=d), INVOICES_PER_DAY)
        print(f"  invoices: {write_jsonl(records, 'invoices'):,} rows")

else:
    if "customers" not in skip_entities:
        # CRM delta: a few new customers + plan/e-mail changes on existing ones
        n_new, n_upd = 15, 25
        new = customers_df.sample(n_new, random_state=int(rng.integers(1_000_000_000))).copy()
        new["customer_id"] = [f"C{NOW:%y%m%d%H}{i:02d}" for i in range(n_new)]
        new["created_at"] = NOW.isoformat()
        upd = customers_df.sample(n_upd, random_state=int(rng.integers(1_000_000_000))).copy()
        upd["plan_id"] = [rng.choice(SEGMENT_PLANS[s]) for s in upd["segment"]]
        delta = pd.concat([new, upd])
        delta["updated_at"] = NOW.isoformat()
        write_csv(corrupt_customers(delta), "customers")

    if "usage_events" not in skip_entities:
        n = max(int(EVENTS_PER_HOUR * VOLUME_FACTOR), 1)
        print(f"  usage_events: {write_jsonl(usage_events(NOW - timedelta(hours=1), NOW, n), 'usage_events'):,} rows")

    if "invoices" not in skip_entities:
        n = max(int(INVOICES_PER_DAY / 24 * 4 * VOLUME_FACTOR), 1)
        print(f"  invoices: {write_jsonl(invoices_for_day(NOW.date(), n), 'invoices'):,} rows")

print("done")
