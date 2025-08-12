import pandas as pd

df = pd.read_parquet('data/bronze/bronze_sample.parquet')

# Basic distribution checks
print('Rows:', len(df))
print('Unique IP count:', df['ip'].nunique())
print('Country distribution (approx):')
print((df['ip_country'].value_counts(normalize=True)*100).round(1).sort_index())

# Billing vs IP country mismatch rate
mismatch_rate = (df['ip_country'] != df['billing_country']).mean()
print('Billing country mismatch rate:', round(mismatch_rate, 3))

# Sample rows
print(df[['ip','ip_country','billing_country']].head(10))
