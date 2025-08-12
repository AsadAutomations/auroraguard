#!/usr/bin/env python3
"""
AuroraGuard Stage 1 - Data Acquisition & Enrichment

- Loads IEEE-CIS train_transaction + train_identity, merges on TransactionID
- Adds synthetic features: device_id, ip (with geo), merchant_id, billing_country
- Simulates 45-day chargeback label delay
- Bootstraps to target size with timestamp + amount jitter
- Exports local Bronze sample as Parquet

Usage:
  python scripts/enrich_dataset.py \
    --raw-dir data/raw \
    --out data/bronze/bronze_sample.parquet \
    --target-rows 1200000 \
    --seed 42
"""
from __future__ import annotations
import argparse
import hashlib
import ipaddress
import math
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
import numpy as np
import pandas as pd

# ---------------- Bronze schema (columns & dtypes) ----------------
# Keep this stable; downstream stages will rely on it.
BRONZE_COLUMNS = [
    # ids & core amounts
    ("transaction_id", "int64"),
    ("event_ts", "datetime64[ns]"),
    ("event_date", "string"),
    ("transaction_amt", "float64"),
    ("currency", "string"),
    # enrichment
    ("device_id", "string"),
    ("ip", "string"),
    ("ip_country", "string"),
    ("merchant_id", "string"),
    ("billing_country", "string"),
    # labels & latency simulation
    ("is_fraud", "int8"),
    ("label_available_ts", "datetime64[ns]"),
]

DEFAULT_CURRENCY = "USD"  # IEEE-CIS has no currency; keep simple for Bronze.

# Example IPv4 country ranges (representative, not authoritative).
# Each value is a list of CIDR blocks we will sample from.
COUNTRY_CIDRS = {
    "US": ["3.0.0.0/8", "8.8.8.0/24", "44.0.0.0/8", "52.0.0.0/8"],
    "GB": ["51.140.0.0/14", "35.176.0.0/15", "18.128.0.0/9"],
    "DE": ["18.184.0.0/13", "35.156.0.0/14"],
    "FR": ["15.236.0.0/15", "35.180.0.0/14"],
    "CA": ["15.222.0.0/15", "52.95.0.0/16"],
    "AU": ["13.236.0.0/14", "52.62.0.0/15"],
    "IN": ["13.232.0.0/14", "15.206.0.0/15"],
    "BR": ["15.228.0.0/14", "18.228.0.0/14"],
}

# Country distribution to sample from when assigning IPs
COUNTRY_PRIORS = pd.Series(
    {
        "US": 0.45, "GB": 0.08, "DE": 0.07, "FR": 0.06,
        "CA": 0.06, "AU": 0.04, "IN": 0.14, "BR": 0.10,
    }
)
COUNTRY_PRIORS = COUNTRY_PRIORS / COUNTRY_PRIORS.sum()

# Merchant catalog with skewed fraud risk (zipf-like)
def build_merchants(n: int = 200, high_risk_top_k: int = 20, seed: int = 42):
    rng = np.random.default_rng(seed)
    # Popularity ~ Zipf
    popularity = 1 / np.arange(1, n + 1)
    popularity = popularity / popularity.sum()
    merchants = [f"m_{i:04d}" for i in range(1, n + 1)]
    # Higher base fraud risk for top_k merchants
    base_risk = np.linspace(0.08, 0.40, high_risk_top_k).tolist() + \
                np.linspace(0.01, 0.05, n - high_risk_top_k).tolist()
    rng.shuffle(base_risk)
    risk = pd.Series(base_risk, index=merchants)
    pop = pd.Series(popularity, index=merchants)
    return merchants, pop, risk

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--raw-dir", default="data/raw", help="Folder with IEEE-CIS CSVs")
    p.add_argument("--out", default="data/bronze/bronze_sample.parquet", help="Output Parquet path")
    p.add_argument("--target-rows", type=int, default=1_200_000, help="Target rows after bootstrapping")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--label-delay-days", type=int, default=45)
    p.add_argument("--start-date", default="2017-12-01", help="Base calendar start for TransactionDT (YYYY-MM-DD)")
    return p.parse_args()

def load_ieee(raw_dir: Path) -> pd.DataFrame:
    tt = pd.read_csv(raw_dir / "train_transaction.csv")
    ti = pd.read_csv(raw_dir / "train_identity.csv")
    df = tt.merge(ti, how="left", on="TransactionID")
    return df

def deterministic_hash(s: str, salt: str = "auroraguard") -> str:
    h = hashlib.sha256((salt + "|" + s).encode("utf-8")).hexdigest()
    return h[:16]  # short, still sufficiently opaque

def gen_device_id(row: pd.Series) -> str:
    # Mix reasonably stable signals; fall back to TransactionID
    parts = [
        str(row.get("card1", "")),
        str(row.get("addr1", "")),
        str(row.get("P_emaildomain", "")),
        str(row.get("uid", "")),
        str(row.get("uid2", "")),
        str(row.get("TransactionID", "")),
    ]
    return deterministic_hash("|".join(parts))

def ip_from_cidr(cidr: str, rng: np.random.Generator) -> str:
    net = ipaddress.ip_network(cidr)
    # Avoid network & broadcast if possible
    size = net.num_addresses
    if size <= 2:
        return str(list(net.hosts())[0])
    offset = rng.integers(1, size - 1)
    ip = int(net.network_address) + int(offset)
    return str(ipaddress.ip_address(ip))

def sample_ip_and_country(rng: np.random.Generator) -> tuple[str, str]:
    country = rng.choice(COUNTRY_PRIORS.index, p=COUNTRY_PRIORS.values)
    cidrs = COUNTRY_CIDRS[country]
    cidr = rng.choice(cidrs)
    ip = ip_from_cidr(cidr, rng)
    return ip, country

def assign_billing_country(ip_country: str, rng: np.random.Generator, mismatch_prob: float = 0.10) -> str:
    if rng.random() > mismatch_prob:
        return ip_country
    # choose a different country
    others = [c for c in COUNTRY_PRIORS.index if c != ip_country]
    return rng.choice(others, p=(COUNTRY_PRIORS[others] / COUNTRY_PRIORS[others].sum()).values)

def seconds_to_datetime(txn_dt_seconds: pd.Series, start_date: str) -> pd.Series:
    """
    Convert TransactionDT (seconds since a reference point) into
    pandas datetime Series starting from start_date.
    """
    base = pd.Timestamp(start_date)  # naive datetime (no timezone)
    return base + pd.to_timedelta(txn_dt_seconds, unit="s")

def bootstrap_rows(df: pd.DataFrame, target_rows: int, seed: int) -> pd.DataFrame:
    if len(df) >= target_rows:
        return df.sample(n=target_rows, replace=False, random_state=seed).reset_index(drop=True)
    reps = math.ceil(target_rows / len(df))
    df_rep = pd.concat([df]*reps, ignore_index=True)
    df_rep = df_rep.sample(n=target_rows, replace=False, random_state=seed)
    return df_rep.reset_index(drop=True)

def jitter_numeric(series: pd.Series, rng: np.random.Generator, rel_std: float = 0.02) -> pd.Series:
    s = series.astype(float).fillna(0.0)
    noise = rng.normal(loc=0.0, scale=rel_std, size=len(s))
    return (s * (1.0 + noise)).clip(lower=0.0)

def validate_bronze_schema(df: pd.DataFrame) -> list[str]:
    issues: list[str] = []
    expected = dict(BRONZE_COLUMNS)

    # presence
    missing = [c for c in expected.keys() if c not in df.columns]
    extra   = [c for c in df.columns if c not in expected.keys()]
    if missing: issues.append(f"Missing columns: {missing}")
    if extra:   issues.append(f"Unexpected columns: {extra}")

    # dtypes (best-effort checks)
    for col, want in BRONZE_COLUMNS:
        if col not in df.columns: 
            continue
        got = str(df[col].dtype)
        if want.startswith("datetime64"):
            if not got.startswith("datetime64"):
                issues.append(f"{col}: want {want}, got {got}")
        elif want in {"int8", "int64", "float64"}:
            if got != want:
                issues.append(f"{col}: want {want}, got {got}")
        elif want == "string":
            # accept pandas StringDtype or plain object but prefer string
            if got not in {"string", "object"}:
                issues.append(f"{col}: want string, got {got}")
    return issues

def main():
    args = parse_args()
    rng = np.random.default_rng(args.seed)

    raw_dir = Path(args.raw_dir)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print("[1/7] Loading IEEE-CIS...")
    df = load_ieee(raw_dir)

    print("[2/7] Core selection & base timestamps...")
    # Map TransactionDT (seconds from a reference) to calendar time
    # If TransactionDT missing, create synthetic sequence
    if "TransactionDT" in df.columns:
        tx_time = seconds_to_datetime(df["TransactionDT"], args.start_date)
    else:
        tx_time = pd.date_range(args.start_date, periods=len(df), freq="min")
    df["_event_ts"] = tx_time

    print("[3/7] Enrichment: device_id...")
    df["device_id"] = df.apply(gen_device_id, axis=1)

    print("[4/7] Enrichment: ip + ip_country + billing_country...")
    ips, ip_countries, billing = [], [], []
    for _ in range(len(df)):
        ip, c = sample_ip_and_country(rng)
        ips.append(ip); ip_countries.append(c)
        billing.append(assign_billing_country(c, rng))
    df["ip"] = ips
    df["ip_country"] = ip_countries
    df["billing_country"] = billing

    print("[5/7] Enrichment: merchant_id with fraud-biased assignment...")
    merchants, pop, risk = build_merchants(seed=args.seed)
    # Conditional assignment: fraud rows more likely to map to higher-risk merchants
    fraud = (df["isFraud"] == 1).fillna(False).values
    m_fraud = np.random.default_rng(args.seed + 1).choice(pop.index, size=fraud.sum(),
                    p=(0.5*pop.values + 0.5*(risk.values/risk.values.sum())))
    m_clean = np.random.default_rng(args.seed + 2).choice(pop.index, size=(~fraud).sum(),
                    p=(0.8*pop.values + 0.2*(risk.values/risk.values.sum())))
    merchant_id = np.empty(len(df), dtype=object)
    merchant_id[fraud] = m_fraud
    merchant_id[~fraud] = m_clean
    df["merchant_id"] = merchant_id

    print("[6/7] Prepare core Bronze fields & jitter for bootstrap...")
    # Base event_ts and amount with small jitter (before bootstrapping)
    df["event_ts"] = df["_event_ts"] + pd.to_timedelta(rng.normal(0, 120, size=len(df)).astype(int), unit="s")
    df["transaction_amt"] = df["TransactionAmt"].astype(float).fillna(0.0)
    df["transaction_amt"] = jitter_numeric(df["transaction_amt"], rng, rel_std=0.015)
    df["currency"] = DEFAULT_CURRENCY
    df["is_fraud"] = df["isFraud"].fillna(0).astype("int8")
    df["label_available_ts"] = (df["event_ts"] + pd.to_timedelta(args.label_delay_days, unit="D"))
    df["event_date"] = df["event_ts"].dt.date.astype(str)

    # Map raw TransactionID -> bronze transaction_id
    if "TransactionID" in df.columns:
        transaction_id_series = pd.to_numeric(df["TransactionID"], errors="coerce")
        # fill any NA with incremental ids to keep int64
        na_mask = transaction_id_series.isna()
        if na_mask.any():
            start = 1
            transaction_id_series[na_mask] = np.arange(start, start + na_mask.sum())
        df["transaction_id"] = transaction_id_series.astype("int64")
    else:
        # fallback: generate sequential ids
        df["transaction_id"] = pd.Series(np.arange(1, len(df) + 1), dtype="int64")


    # Keep only Bronze columns pre-bootstrap (for memory)
    bronze = pd.DataFrame({name: df[name] for name, _ in BRONZE_COLUMNS})

    print("[7/7] Bootstrap to target rows & finalize jitter...")
    bronze = bootstrap_rows(bronze, args.target_rows, args.seed).reset_index(drop=True)

    # Add a tiny extra jitter to timestamps after bootstrap to avoid perfect duplicates
    bronze["event_ts"] = pd.to_datetime(bronze["event_ts"]) + pd.to_timedelta(
        np.random.default_rng(args.seed + 3).integers(-90, 90, size=len(bronze)), unit="s"
    )
    bronze["event_date"] = bronze["event_ts"].dt.date.astype(str)
    bronze["label_available_ts"] = bronze["event_ts"] + pd.to_timedelta(args.label_delay_days, unit="D")

    # Enforce dtypes and ordering
    for col, dtype in BRONZE_COLUMNS:
        if dtype.startswith("datetime64"):
            bronze[col] = pd.to_datetime(bronze[col])
        elif dtype == "int8":
            bronze[col] = bronze[col].astype("int8")
        elif dtype == "int64":
            # IEEE TransactionID can exceed int32; create from original index if missing
            if col not in bronze.columns or bronze[col].isna().any():
                bronze[col] = pd.Series(np.arange(1, len(bronze)+1), dtype="int64")
            else:
                bronze[col] = bronze[col].astype("int64")
        elif dtype == "float64":
            bronze[col] = bronze[col].astype("float64")
        else:
            bronze[col] = bronze[col].astype("string")


    problems = validate_bronze_schema(bronze)
    if problems:
        print("[Schema check] Issues found:")
        for p in problems:
            print(" -", p)
    else:
        print("[Schema check] Bronze schema OK.")

    # Write Parquet
    out_path.parent.mkdir(parents=True, exist_ok=True)
    bronze.to_parquet(out_path, index=False)
    # Optional convenience copy at project root if requested
    root_copy = Path("bronze_sample.parquet")
    try:
        bronze.sample(n=min(10000, len(bronze)), random_state=1).to_parquet(root_copy, index=False)
    except Exception:
        pass

    print(f"Done. Wrote: {out_path}  (rows={len(bronze):,})")
    if root_copy.exists():
        print(f"Sample also at: {root_copy} (up to 10k rows for quick inspection)")
if __name__ == "__main__":
    main()
