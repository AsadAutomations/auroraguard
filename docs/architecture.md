# AuroraGuard – Architecture (Draft)

> Placeholder diagram image (to be produced in a later stage):
>
> ![Architecture Diagram Placeholder](./architecture.png)
> _File not yet created – serves as a placeholder._

## High-Level Flow
- **Ingress**: API Gateway (REST/HTTP)
- **Orchestration**: Step Functions (Express) ? Lambda(s)
- **Features**: DynamoDB (velocity/recency aggregates), Athena/Glue for batch features
- **Model**: SageMaker endpoint (serverless or provisioned; scaled down when idle)
- **Storage**: S3 data lake (Bronze/Silver/Gold)
- **Observability**: CloudWatch metrics/alarms, X-Ray traces
- **Security**: IAM roles, KMS, Secrets Manager

## IaC Decision
- Infrastructure as Code: **AWS CDK (Python)**
- See ADR: [docs/adr/0001-iac-decision-cdk-python.md](./adr/0001-iac-decision-cdk-python.md)

## Mermaid Sketch (to be refined later)
`mermaid
flowchart LR
  Client -->|HTTPS| APIGW
  APIGW --> SFN[Step Functions Express]
  SFN --> L1[Lambda - Orchestrator]
  L1 --> DDB[(DynamoDB velocity)]
  L1 --> SM[SageMaker Endpoint]
  L1 --> S3[(S3 Gold)]
  SM --> L1
  L1 --> APIGW
