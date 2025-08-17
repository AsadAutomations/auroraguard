#!/usr/bin/env python3
import os
import aws_cdk as cdk
from stacks.infra_stack import AuroraGuardInfraStack

app = cdk.App()

env = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("CDK_DEFAULT_REGION")
)

AuroraGuardInfraStack(
    app,
    "AuroraGuardInfra",
    env=env,
    description="AuroraGuard - Stage 2 foundational infrastructure"
)

app.synth()
