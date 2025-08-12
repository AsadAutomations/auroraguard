import pandas as pd
import pyarrow.parquet as pq

p = "data/bronze/bronze_sample.parquet"
df = pd.read_parquet(p)
print("Rows:", len(df))
print("\nDtypes:\n", df.dtypes)
print("\nNull counts (top 10):\n", df.isnull().sum().sort_values(ascending=False).head(10))

# Basic sanity
print("\nFraud rate:", round(df['is_fraud'].mean(), 4))
print("Date range:", df['event_ts'].min(), "->", df['event_ts'].max())
print("Unique merchants:", df['merchant_id'].nunique(), "Unique devices:", df['device_id'].nunique())

# Check label delay (should be 45 days)
d = (df['label_available_ts'] - df['event_ts']).dt.days
print("Label delay unique (days):", sorted(d.unique())[:5])

# Spot-check country mismatch rate
mismatch = (df['ip_country'] != df['billing_country']).mean()
print("Billing/IP country mismatch:", round(mismatch, 3))
