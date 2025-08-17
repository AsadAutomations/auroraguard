from aws_cdk import (
    Aws,
    Duration,
    RemovalPolicy,
    Stack,
    aws_kms as kms,
    aws_s3 as s3,
    aws_iam as iam,
    aws_glue as glue,
    aws_athena as athena,
    aws_dynamodb as ddb,
    aws_logs as logs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_apigateway as apigw,
    aws_cloudwatch as cw,
)


from constructs import Construct


class AuroraGuardInfraStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # -------- KMS: single CMK for data lake buckets (alias: alias/auroraguard-s3) --------
        data_kms_key = kms.Key(
            self,
            "DataLakeKmsKey",
            alias="alias/auroraguard-s3",
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.RETAIN,
            description="KMS CMK for AuroraGuard S3 (bronze/silver/gold) and client-side envelope keys as needed.",
        )
                # Allow CloudWatch Logs service in this region to use the CMK for the SFN log group
        data_kms_key.add_to_resource_policy(
            iam.PolicyStatement(
                sid="AllowCWLogsToUseKey",
                principals=[iam.ServicePrincipal(f"logs.{Aws.REGION}.amazonaws.com")],
                actions=[
                    "kms:Encrypt",
                    "kms:Decrypt",
                    "kms:ReEncrypt*",
                    "kms:GenerateDataKey*",
                    "kms:DescribeKey",
                ],
                resources=["*"],
                # scope it to our account and the /aws/vendedlogs/states/* log-groups
                conditions={
                    "StringEquals": {"aws:SourceAccount": Aws.ACCOUNT_ID},
                    "ArnLike": {
                        "kms:EncryptionContext:aws:logs:arn": f"arn:aws:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws/vendedlogs/states/*"
                    },
                },
            )
        )


        # Helper: secure bucket factory
        def secure_bucket(logical_id: str, tier: str) -> s3.Bucket:
            bucket = s3.Bucket(
                self,
                logical_id,
                # NOTE: no bucket_name � let CDK auto-name to avoid token validation error
                versioned=True,
                block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                encryption=s3.BucketEncryption.KMS,
                encryption_key=data_kms_key,
                enforce_ssl=True,
                object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
                # Keep data by default; change to DESTROY for ephemeral environments.
                removal_policy=RemovalPolicy.RETAIN,
                auto_delete_objects=False,
            )

            # Lifecycle hygiene
            bucket.add_lifecycle_rule(
                id="AbortIncompleteMultipart",
                abort_incomplete_multipart_upload_after=Duration.days(7),
            )

            # Policy: deny unencrypted (non-KMS) object puts
            bucket.add_to_resource_policy(
                iam.PolicyStatement(
                    sid="DenyIncorrectEncryptionHeader",
                    effect=iam.Effect.DENY,
                    principals=[iam.AnyPrincipal()],
                    actions=["s3:PutObject"],
                    resources=[bucket.arn_for_objects("*")],
                    conditions={"StringNotEquals": {"s3:x-amz-server-side-encryption": "aws:kms"}},
                )
            )

            # Policy: require the specific CMK key id on PutObject (defense-in-depth)
            bucket.add_to_resource_policy(
                iam.PolicyStatement(
                    sid="DenyWrongKmsKey",
                    effect=iam.Effect.DENY,
                    principals=[iam.AnyPrincipal()],
                    actions=["s3:PutObject"],
                    resources=[bucket.arn_for_objects("*")],
                    conditions={
                        "StringNotEquals": {
                            "s3:x-amz-server-side-encryption-aws-kms-key-id": data_kms_key.key_arn
                        }
                    },
                )
            )

            # Policy: deny non-TLS
            bucket.add_to_resource_policy(
                iam.PolicyStatement(
                    sid="DenyInsecureTransport",
                    effect=iam.Effect.DENY,
                    principals=[iam.AnyPrincipal()],
                    actions=["s3:*"],
                    resources=[bucket.bucket_arn, bucket.arn_for_objects("*")],
                    conditions={"Bool": {"aws:SecureTransport": "false"}},
                )
            )

            return bucket

        # -------- Buckets: Bronze, Silver, Gold --------
        bronze_bucket = secure_bucket("BronzeBucket", "bronze")
        silver_bucket = secure_bucket("SilverBucket", "silver")
        gold_bucket = secure_bucket("GoldBucket", "gold")

                # -------- Glue Data Catalog: database for AuroraGuard --------
        glue_db = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=Aws.ACCOUNT_ID,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="auroraguard_bronze",  # initial logical db (we can add silver/gold dbs later)
                description="AuroraGuard Bronze layer database (tables created later via Glue/Athena).",
            ),
        )

        # -------- Athena WorkGroup: results to Gold bucket (KMS-encrypted) --------
        athena_results_prefix = "athena-results/"
        athena_cfg = athena.CfnWorkGroup.WorkGroupConfigurationProperty(
            result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                output_location=f"s3://{gold_bucket.bucket_name}/{athena_results_prefix}",
                encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                    encryption_option="SSE_KMS",
                    kms_key=data_kms_key.key_arn,
                ),
            ),
            enforce_work_group_configuration=True,
            publish_cloud_watch_metrics_enabled=True,
            requester_pays_enabled=False,
        )

        athena_wg = athena.CfnWorkGroup(
            self,
            "AthenaWorkgroup",
            name="auroraguard_wg",
            description="AuroraGuard primary workgroup",
            state="ENABLED",
            recursive_delete_option=True,  # allows delete on stack destroy
            work_group_configuration=athena_cfg,
        )

        # Allow Athena service to write to the results prefix in Gold bucket
        gold_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="AllowAthenaToWriteResults",
                principals=[iam.ServicePrincipal("athena.amazonaws.com")],
                actions=["s3:PutObject", "s3:AbortMultipartUpload"],
                resources=[gold_bucket.arn_for_objects(f"{athena_results_prefix}*")],
                conditions={"StringEquals": {"aws:SourceAccount": Aws.ACCOUNT_ID}},
            )
        )

                # -------- DynamoDB: recent aggregates (TTL) --------
        # Generic key design for flexibility:
        #   pk: e.g., "device#<id>" | "ip#<addr>" | "merchant#<id>" | "user#<id>"
        #   sk: e.g., "window#5m" | "feature#txn_count" (allows multiple features/windows)
        # Attributes (examples stored by app): count:int, sum_amount:float, updated_at:epoch
        # TTL attribute: ttl_epoch (unix seconds); items disappear automatically
        recent_agg_table = ddb.Table(
            self,
            "RecentAggTable",
            table_name="auroraguard_recent_agg",
            partition_key=ddb.Attribute(name="pk", type=ddb.AttributeType.STRING),
            sort_key=ddb.Attribute(name="sk", type=ddb.AttributeType.STRING),
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,  # on-demand for simplicity + cost control
            encryption=ddb.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=data_kms_key,
            time_to_live_attribute="ttl_epoch",
            point_in_time_recovery=True,
            removal_policy=RemovalPolicy.RETAIN,  # keep by default
        )

        # Optional GSI for “most recent by entity” lookups in batch analysis
        recent_agg_table.add_global_secondary_index(
            index_name="gsi1_updated",
            partition_key=ddb.Attribute(name="pk", type=ddb.AttributeType.STRING),
            sort_key=ddb.Attribute(name="updated_at", type=ddb.AttributeType.NUMBER),
            projection_type=ddb.ProjectionType.ALL,
        )

        # (Export for other stacks)
        self.recent_agg_table_name = recent_agg_table.table_name




                # -------- CloudWatch Log Group for SFN (KMS-encrypted) --------
        sfn_log_group = logs.LogGroup(
            self,
            "TxnEvalLogGroup",
            log_group_name=f"/aws/vendedlogs/states/AuroraGuardTxnEval",
            removal_policy=RemovalPolicy.RETAIN,
            retention=logs.RetentionDays.ONE_MONTH,
            encryption_key=data_kms_key,
        )

        # -------- Step Functions (Express) placeholder --------
        # Simple placeholder: echoes input and returns a canned decision
        definition = sfn.Chain.start(
            sfn.Pass(
                self,
                "AssembleDecision",
                result=sfn.Result.from_object({
                    "decision": "ALLOW",
                    "score": 0.01,
                    "explanations": ["placeholder"],
                }),
                result_path="$.result"
            )
        )

        sfn_role = iam.Role(
            self,
            "SfnExecRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            description="Role for AuroraGuard Express state machine to write logs (expand later).",
        )
        # Allow SFN to write logs to our encrypted log group
        sfn_role.add_to_policy(iam.PolicyStatement(
            actions=["logs:CreateLogStream", "logs:PutLogEvents"],
            resources=[f"{sfn_log_group.log_group_arn}:*"]
        ))

        txn_state_machine = sfn.StateMachine(
            self,
            "TxnEvalExpress",
            state_machine_name="AuroraGuardTxnEvalExpress",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.seconds(10),  # placeholder; real path must stay <120ms P99
            logs=sfn.LogOptions(
                destination=sfn_log_group,
                level=sfn.LogLevel.ALL,
                include_execution_data=True,
            ),
            tracing_enabled=True,
            state_machine_type=sfn.StateMachineType.EXPRESS,
            role=sfn_role,
        )

        # -------- API Gateway REST API with /txn -> StepFunctions:StartSyncExecution --------
        api = apigw.RestApi(
            self,
            "AuroraGuardApi",
            rest_api_name="auroraguard-api",
            description="AuroraGuard synchronous transaction scoring API",
            cloud_watch_role=True,
            deploy_options=apigw.StageOptions(
                metrics_enabled=True,
                logging_level=apigw.MethodLoggingLevel.INFO,
                data_trace_enabled=False,
                tracing_enabled=True,
            ),
            default_cors_preflight_options=apigw.CorsOptions(
                allow_origins=apigw.Cors.ALL_ORIGINS,
                allow_methods=["POST", "OPTIONS"],
            ),
        )

        txn_resource = api.root.add_resource("txn")

        # Execution role that API Gateway uses to call Step Functions
        apigw_sfn_role = iam.Role(
            self,
            "ApiGwSfnInvokeRole",
            assumed_by=iam.ServicePrincipal("apigateway.amazonaws.com"),
            description="Allows API Gateway to call Step Functions StartSyncExecution",
        )
        apigw_sfn_role.add_to_policy(iam.PolicyStatement(
            actions=["states:StartSyncExecution"],
            resources=[txn_state_machine.state_machine_arn]
        ))
        # CloudWatch Logs for API GW (account-wide service role handled by AWS, no explicit KMS binding here)

        # Request/response mapping for StartSyncExecution
        start_sync_integration = apigw.AwsIntegration(
            service="states",
            action="StartSyncExecution",
            integration_http_method="POST",
            options=apigw.IntegrationOptions(
                credentials_role=apigw_sfn_role,
                request_templates={
                    "application/json": (
                        # Build StartSyncExecution payload
                        '{'
                        f'"stateMachineArn":"{txn_state_machine.state_machine_arn}",'
                        '"input": "$util.escapeJavaScript($input.body)"'
                        '}'
                    )
                },
                integration_responses=[
                    apigw.IntegrationResponse(
                        status_code="200",
                        # The sync execution returns JSON with output string; we unwrap it
                        response_templates={
                            "application/json":
                                # Parse the output field, which is a JSON string
                                '''
                                #set($parsed = $util.parseJson($input.body))
                                $parsed.output
                                '''
                        },
                    ),
                    apigw.IntegrationResponse(
                        selection_pattern="5\\d{2}",
                        status_code="500",
                        response_templates={"application/json": '{"message":"Internal error"}'},
                    ),
                ],
            ),
        )

        method = txn_resource.add_method(
            http_method="POST",
            integration=start_sync_integration,
            method_responses=[
                apigw.MethodResponse(status_code="200"),
                apigw.MethodResponse(status_code="500"),
            ],
        )


                # -------- IAM: Shared inline policy helpers --------
        # CloudWatch Logs baseline for Lambda-like runtimes
        def logs_policy_for(prefix: str) -> iam.PolicyStatement:
            return iam.PolicyStatement(
                actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                resources=[f"arn:{Aws.PARTITION}:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:{prefix}*"]
            )

        # -------- IAM Role: LambdaTxnEvalRole (future Lambda used by SFN or API) --------
        lambda_role = iam.Role(
            self,
            "LambdaTxnEvalRole",
            role_name=f"AuroraGuard-LambdaTxnEvalRole-{Aws.REGION}",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Lambda role for transaction evaluation (DDB velocity reads/writes, KMS, X-Ray)",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AWSXRayDaemonWriteAccess"),
            ],
        )
        # DDB table access (fine-grained)
        lambda_role.add_to_policy(iam.PolicyStatement(
            actions=["dynamodb:GetItem","dynamodb:PutItem","dynamodb:UpdateItem","dynamodb:BatchGetItem","dynamodb:BatchWriteItem","dynamodb:Query"],
            resources=[f"arn:{Aws.PARTITION}:dynamodb:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/auroraguard_recent_agg",
                       f"arn:{Aws.PARTITION}:dynamodb:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/auroraguard_recent_agg/index/*"]
        ))
        # S3 read (bronze/silver/gold) limited to feature/model prefixes we’ll use later
        lambda_role.add_to_policy(iam.PolicyStatement(
            actions=["s3:GetObject","s3:ListBucket"],
            resources=[
                f"arn:{Aws.PARTITION}:s3:::auroraguard-bronze-{Aws.ACCOUNT_ID}-{Aws.REGION}",
                f"arn:{Aws.PARTITION}:s3:::auroraguard-bronze-{Aws.ACCOUNT_ID}-{Aws.REGION}/*",
                f"arn:{Aws.PARTITION}:s3:::auroraguard-silver-{Aws.ACCOUNT_ID}-{Aws.REGION}",
                f"arn:{Aws.PARTITION}:s3:::auroraguard-silver-{Aws.ACCOUNT_ID}-{Aws.REGION}/*",
                f"arn:{Aws.PARTITION}:s3:::auroraguard-gold-{Aws.ACCOUNT_ID}-{Aws.REGION}",
                f"arn:{Aws.PARTITION}:s3:::auroraguard-gold-{Aws.ACCOUNT_ID}-{Aws.REGION}/*",
            ]
        ))
        # Logs & KMS
        lambda_role.add_to_policy(logs_policy_for("/aws/lambda/AuroraGuard-"))
        # Grant encrypt/decrypt via KMS key grant (preferred over wildcards)
        data_kms_key.grant_encrypt_decrypt(lambda_role)

        # -------- IAM Role: Glue/Athena service usage (ETL & DDL later) --------
        glue_athena_role = iam.Role(
            self,
            "GlueAthenaRole",
            role_name=f"AuroraGuard-GlueAthenaRole-{Aws.REGION}",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description="Glue job/crawler role for AuroraGuard data lake (S3, Glue Catalog, Athena query outputs)",
        )
        glue_athena_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "glue:GetDatabase","glue:GetDatabases","glue:GetTable","glue:GetTables","glue:CreateTable","glue:UpdateTable","glue:DeleteTable",
                "glue:GetPartition","glue:GetPartitions","glue:CreatePartition","glue:BatchCreatePartition","glue:UpdatePartition","glue:DeletePartition"
            ],
            resources=["*"]  # Glue Catalog ARNs are verbose; tighten later if needed
        ))
        glue_athena_role.add_to_policy(iam.PolicyStatement(
            actions=["athena:StartQueryExecution","athena:GetQueryExecution","athena:GetQueryResults","athena:StopQueryExecution"],
            resources=[f"arn:{Aws.PARTITION}:athena:{Aws.REGION}:{Aws.ACCOUNT_ID}:workgroup/auroraguard_wg"]
        ))
        glue_athena_role.add_to_policy(iam.PolicyStatement(
            actions=["s3:GetObject","s3:PutObject","s3:ListBucket","s3:AbortMultipartUpload"],
            resources=[
                f"arn:{Aws.PARTITION}:s3:::auroraguard-bronze-{Aws.ACCOUNT_ID}-{Aws.REGION}",
                f"arn:{Aws.PARTITION}:s3:::auroraguard-bronze-{Aws.ACCOUNT_ID}-{Aws.REGION}/*",
                f"arn:{Aws.PARTITION}:s3:::auroraguard-silver-{Aws.ACCOUNT_ID}-{Aws.REGION}",
                f"arn:{Aws.PARTITION}:s3:::auroraguard-silver-{Aws.ACCOUNT_ID}-{Aws.REGION}/*",
                f"arn:{Aws.PARTITION}:s3:::auroraguard-gold-{Aws.ACCOUNT_ID}-{Aws.REGION}",
                f"arn:{Aws.PARTITION}:s3:::auroraguard-gold-{Aws.ACCOUNT_ID}-{Aws.REGION}/*",
            ]
        ))
        data_kms_key.grant_encrypt_decrypt(glue_athena_role)

        # -------- IAM Role: SageMaker invocation client (for API/Lambda to call endpoint) --------
        sagemaker_invoke_role = iam.Role(
            self,
            "SageMakerInvokeRole",
            role_name=f"AuroraGuard-SMInvokeRole-{Aws.REGION}",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),  # typically used by a Lambda invoker
            description="Role for invoking AuroraGuard SageMaker endpoints from Lambda",
        )
        sagemaker_invoke_role.add_to_policy(iam.PolicyStatement(
            actions=["sagemaker:InvokeEndpoint"],
            resources=[f"arn:{Aws.PARTITION}:sagemaker:{Aws.REGION}:{Aws.ACCOUNT_ID}:endpoint/auroraguard-*"]
        ))
        sagemaker_invoke_role.add_to_policy(logs_policy_for("/aws/lambda/AuroraGuard-"))
        data_kms_key.grant_encrypt_decrypt(sagemaker_invoke_role)

        # -------- Outputs (role ARNs) --------
        self.lambda_role_arn = lambda_role.role_arn
        self.glue_athena_role_arn = glue_athena_role.role_arn
        self.sm_invoke_role_arn = sagemaker_invoke_role.role_arn


                # -------- CloudWatch Dashboard: AuroraGuard-Infra --------
        dashboard = cw.Dashboard(
            self,
            "InfraDashboard",
            dashboard_name="AuroraGuard-Infra",
        )

        # Convenience handles
        stage = api.deployment_stage  # default "prod"

        # API Gateway widgets
        api_latency = cw.GraphWidget(
            title="API /txn Latency (p50/p90/p99)",
            left=[
                stage.metric_latency(statistic="p50"),
                stage.metric_latency(statistic="p90"),
                stage.metric_latency(statistic="p99"),
            ],
            width=12,
        )

        api_errors = cw.GraphWidget(
            title="API /txn Errors (4XX/5XX)",
            left=[
                stage.metric_client_error(statistic="Sum"),
                stage.metric_server_error(statistic="Sum"),
            ],
            width=12,
        )

        # Step Functions widgets
        sfn_rate = cw.GraphWidget(
            title="SFN Execs (Success/Failed/Throttled)",
            left=[
                txn_state_machine.metric_succeeded(),
                txn_state_machine.metric_failed(),
                txn_state_machine.metric_throttled(),
            ],
            width=12,
        )
        sfn_duration = cw.GraphWidget(
            title="SFN Execution Time (avg)",
            left=[
                txn_state_machine.metric(
                    "ExecutionTime",
                    statistic="Average",
                    period=Duration.minutes(5),
                )
            ],
            width=12,
        )

        # DynamoDB widgets
        ddb_throttles = cw.GraphWidget(
            title="DynamoDB Throttled Requests",
            left=[recent_agg_table.metric_throttled_requests_for_operation("All")],
            width=12,
        )


        ddb_rcu_wcu = cw.GraphWidget(
            title="DynamoDB RCU/WCU (on-demand)",
            left=[recent_agg_table.metric_consumed_read_capacity_units(),
                  recent_agg_table.metric_consumed_write_capacity_units()],
            width=12,
        )

        # S3 storage view (Gold bucket as proxy; repeat if desired)
        s3_storage = cw.GraphWidget(
            title="S3 Storage - Gold (Bytes)",
            left=[
                cw.Metric(
                    namespace="AWS/S3",
                    metric_name="BucketSizeBytes",
                    dimensions_map={
                        "BucketName": gold_bucket.bucket_name,
                        "StorageType": "StandardStorage",
                    },
                    statistic="Average",
                    period=Duration.hours(24),
                )
            ],
            width=12,
        )


        # Text banner
        banner = cw.TextWidget(
            markdown="### AuroraGuard Infra — API `/txn` → SFN (Express) • DDB `auroraguard_recent_agg` • S3 Bronze/Silver/Gold",
            width=24,
            height=2,
        )

        dashboard.add_widgets(banner)
        dashboard.add_widgets(api_latency, api_errors)
        dashboard.add_widgets(sfn_rate, sfn_duration)
        dashboard.add_widgets(ddb_throttles, ddb_rcu_wcu)
        dashboard.add_widgets(s3_storage)



        # Output useful ARNs/names for downstream use
        self.api_url = api.url
        self.txn_state_machine_arn = txn_state_machine.state_machine_arn


        # Export names/arns if needed by later stacks
        self.bronze_bucket_name = bronze_bucket.bucket_name
        self.silver_bucket_name = silver_bucket.bucket_name
        self.gold_bucket_name = gold_bucket.bucket_name
        self.data_kms_key_arn = data_kms_key.key_arn
