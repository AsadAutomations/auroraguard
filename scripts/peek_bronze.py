import pandas as pd

df = pd.read_parquet("data/bronze/bronze_sample.parquet")
print("Rows:", len(df))
print("Columns:", df.columns.tolist())
print(df.head(3))
print(df.dtypes)
