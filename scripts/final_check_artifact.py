import pandas as pd
p = "data/bronze/bronze_sample.parquet"
df = pd.read_parquet(p)
print("Rows:", len(df), "| Columns:", len(df.columns))
print("Columns:", list(df.columns))
