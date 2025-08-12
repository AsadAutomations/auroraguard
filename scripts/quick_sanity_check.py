import pandas as pd
from pathlib import Path

raw = Path("data/raw")
tt = pd.read_csv(raw / "train_transaction.csv")
ti = pd.read_csv(raw / "train_identity.csv")
df = tt.merge(ti, how="left", on="TransactionID")

print("Rows:", len(df), "Cols:", len(df.columns))
print("Fraud rate:", df['isFraud'].mean())
print("Sample columns:", df.columns[:8].tolist())

df.head(5).to_csv("data/interim/peek_train_merged.csv", index=False)
print("Wrote data/interim/peek_train_merged.csv")
