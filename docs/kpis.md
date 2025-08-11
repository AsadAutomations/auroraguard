# AuroraGuard – KPIs & SLOs

## Scope
- Fraud type: **E-commerce Card-Not-Present (CNP)**
- Real-time scoring target: **P99 = 120 ms** end-to-end
  - Path: API Gateway ? Step Functions (Express) ? Lambda ? (DynamoDB lookups) ? SageMaker endpoint ? response
- Data: IEEE-CIS base dataset enriched with device_id, IP?geo, merchant_id risk, billing_country, and **45-day label delay**.
- Cost cap: **< £100/month** (serverless-first; shut down non-serverless when idle)

## Detection Strategy
- **Hybrid**: Rules (velocity, geo mismatch, merchant risk prior) + ML (XGBoost/LightGBM with calibrated probabilities)
- **Interpretability**: SHAP summaries + rule hits returned as reasons

## KPIs
1. **PR-AUC (validation & offline test)**  
   - Target: **= 0.40** (baseline) with stretch **= 0.50** after feature iterations
2. **Recall @ 0.5% FPR** (operating point for review/decline)  
   - Target: **= 0.60** recall at **0.5%** false positive rate
3. **P99 Latency (production scoring)**  
   - Target: **= 120 ms** end-to-end (cold starts excluded from steady-state SLO; tracked separately)
4. **Cost Guardrail**  
   - Target: **< £100/month** blended; automated alerts at **£70** and **£90**
5. **Uptime & Availability**  
   - API success rate **= 99.9%** (5xx as failures)
6. **Data Freshness (features)**  
   - DynamoDB velocity aggregates staleness **= 60s**; batch features SLA **< 24h**

## Observability & Alerts
- **Latency**: CloudWatch SLO dashboards for P50/P90/P99; alert if P99 > 120 ms for 5 consecutive mins.
- **Cost**: AWS Budgets alerts at £70 and £90; action: scale down SageMaker endpoint or switch to serverless inference.
- **Model Drift**: (Future) SageMaker Model Monitor; alert on data/label shift beyond thresholds.

## Evaluation Protocol
- **Train/Val/Test**: time-based split; holdout reflects 45-day label delay.
- **Calibration**: Platt/Isotonic; evaluate Brier score and reliability curves.
- **Explainability**: SHAP global + per-record top features; rule hit indicators.

## Glossary
- **FPR**: False Positive Rate
- **PR-AUC**: Area under Precision–Recall curve
- **CNP**: Card-Not-Present
