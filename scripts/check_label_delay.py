import pandas as pd

# Load parquet file
df = pd.read_parquet("data/bronze/bronze_sample.parquet")

# Calculate actual delay in days
delays = (df['label_available_ts'] - df['event_ts']).dt.days
print("Delay stats (days):\n", delays.describe())

# Sanity check: all equal to 45
unique_delays = delays.unique()
print("Unique delay values:", unique_delays)

# Show sample with fraud labels
print(df[df.is_fraud==1][['event_ts', 'label_available_ts', 'is_fraud']].head())
