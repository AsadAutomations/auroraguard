import pandas as pd

df = pd.read_parquet("data/bronze/bronze_sample.parquet")

# Overall distribution by merchant
overall = df["merchant_id"].value_counts(normalize=True).rename("overall_share")

# Fraud vs non-fraud distribution
fraud_share = df[df.is_fraud == 1]["merchant_id"].value_counts(normalize=True).rename("fraud_share")
clean_share = df[df.is_fraud == 0]["merchant_id"].value_counts(normalize=True).rename("clean_share")

# Join and compute lift (fraud_share / overall_share)
rep = pd.concat([overall, fraud_share, clean_share], axis=1).fillna(0.0)
rep["fraud_lift"] = (rep["fraud_share"] / rep["overall_share"]).replace([float("inf")], 0)

# Top merchants by fraud_lift and by fraud volume
top_lift = rep.sort_values("fraud_lift", ascending=False).head(15)
top_fraud_vol = df[df.is_fraud == 1]["merchant_id"].value_counts().head(15)

print("Top 15 merchants by FRAUD LIFT:")
print(top_lift[["overall_share", "fraud_share", "fraud_lift"]].round(3))

print("\nTop 15 merchants by FRAUD COUNT:")
print(top_fraud_vol)

# Concentration check: share of fraud among top-20 merchants (by fraud_share)
top20 = rep.sort_values("fraud_share", ascending=False).head(20).index
fraud_conc = (df[(df.is_fraud == 1) & (df.merchant_id.isin(top20))].shape[0]) / (df.is_fraud == 1).sum()
print(f"\nFraud concentration in top-20 merchants: {fraud_conc:.3f}")

# Quick sanity: fraud rate by merchant (should vary)
fr_by_m = df.groupby("merchant_id")["is_fraud"].mean().sort_values(ascending=False).head(10)
print("\nHighest merchant-level fraud rates (top 10):")
print(fr_by_m.round(3).to_string())
