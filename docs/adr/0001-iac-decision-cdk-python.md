# ADR 0001: Infrastructure as Code – AWS CDK (Python)

**Status:** Accepted  
**Date:** 2025-08-11

## Context
AuroraGuard requires reproducible, testable, and modular infrastructure that integrates tightly with Python-heavy workloads (Lambdas, Glue, SageMaker) and supports multi-env deployment.

## Decision
Use **AWS CDK (Python)** as the Infrastructure-as-Code framework.

## Rationale
- Native fit with Python 3.11 stack and type checking (mypy/ruff).
- Constructs enable higher-level abstractions for Step Functions, Lambda, SageMaker, and IAM.
- Reuse across micro-stacks (real-time, batch/MLOps, data lake).
- Good local dev ergonomics (cdk synth/diff/deploy) and integration with CI.
- Easier to keep costs under control via programmatic checks (e.g., alarms, min/max scaling).

## Consequences
- CDK bootstrap required per account/region.
- Lock versions in infra/requirements.txt to ensure repeatable synth.
- Add CI check to fail on stacks with provisioned SageMaker endpoints left enabled in non-prod.

## Alternatives Considered
- **Terraform:** mature ecosystem; chosen against to keep one language (Python) across code + infra.
- **SAM:** great for pure serverless; less convenient for multi-service graph (SageMaker, Glue, Step Functions).
- **CloudFormation (raw):** verbose; slower iteration.

## Implementation Notes
- Create infra/ CDK app in Python with separate stacks: ealtime, data_platform, mlops.
- Enforce tags: Project=AuroraGuard, Owner=<your-alias>, CostCenter=personal, Env=<dev|prod>.
- Use context defaults for region/account; allow overrides via cdk.json.
