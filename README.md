# AuroraGuard

AuroraGuard is a real-time **e-commerce Card-Not-Present (CNP) fraud detection** platform on AWS.  
It combines **rules** (velocity, geo mismatch) with **ML** (XGBoost with calibrated probabilities) to deliver **P99 = 120 ms** decisions via API Gateway ? Step Functions (Express) ? Lambda ? SageMaker.

## Key Goals
- Latency SLO: **P99 = 120 ms**
- Metrics: **PR-AUC**, **Recall @ 0.5% FPR**
- Cost cap: **< £100/month** (serverless-first; scale to zero where possible)

## Tech Stack
- Languages: Python 3.11 (Lambdas, Glue, SageMaker)
- AWS: API Gateway, Step Functions (Express/Standard), Lambda, SageMaker, S3 (Bronze/Silver/Gold), Glue + Data Catalog, Athena, DynamoDB, CloudWatch/X-Ray, IAM/KMS/Secrets Manager
- IaC: **AWS CDK (Python)**

## Repo Layout
- /infra – CDK app (Python)
- /lambdas – real-time + utilities
- /sagemaker – training scripts, inference code
- /glue_jobs – ETL & feature builds
- /sql – Athena/SparkSQL
- /scripts – CLIs & helpers
- /notebooks – exploration
- /docs – architecture & KPIs

See docs/kpis.md for KPIs/SLOs and docs/architecture.md for the architecture draft.
