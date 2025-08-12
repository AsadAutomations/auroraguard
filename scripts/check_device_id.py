import pandas as pd, re

df = pd.read_parquet('data/bronze/bronze_sample.parquet')

print('Rows:', len(df))
print(df[['transaction_id','device_id']].head(10))

# uniqueness / collisions
nuniq = df['device_id'].nunique(dropna=True)
collisions = len(df) - nuniq
print('unique device_id:', nuniq, '| collisions:', collisions)

# nulls
print('null device_id:', int(df['device_id'].isna().sum()))

# length + charset (should be 16 hex chars)
lens = df['device_id'].astype(str).str.len()
print('length min/median/max:', lens.min(), int(lens.median()), lens.max())
hex_ok = df['device_id'].astype(str).str.fullmatch(r'[0-9a-f]{16}').mean()
print('hex16 proportion:', float(hex_ok))
