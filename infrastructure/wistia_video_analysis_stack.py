from __future__ import annotations

import os
import tempfile
from pathlib import Path

from aws_cdk import (
    CfnOutput,
    RemovalPolicy,
    Stack,
    Tags,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_s3_assets as s3_assets,
    aws_secretsmanager as secretsmanager,
)
from constructs import Construct


class WistiaVideoAnalysisStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        project_name = os.getenv("PROJECT_NAME", "wistia-video-analysis")
        environment_name = os.getenv("ENVIRONMENT", "dev")
        bucket_name = os.getenv("S3_BUCKET_NAME")

        bronze_prefix = os.getenv("BRONZE_PREFIX", "bronze/wistia")
        silver_prefix = os.getenv("SILVER_PREFIX", "silver/wistia")
        gold_prefix = os.getenv("GOLD_PREFIX", "gold/wistia")

        # Gold table names are configurable so the same stack can be reused across
        # environments without changing code. These names become Athena table names
        # inside the Glue database.
        dim_media_table_name = os.getenv("DIM_MEDIA_TABLE_NAME", "dim_media")
        dim_visitor_table_name = os.getenv("DIM_VISITOR_TABLE_NAME", "dim_visitor")
        fact_media_engagement_table_name = os.getenv(
            "FACT_MEDIA_ENGAGEMENT_TABLE_NAME",
            "fact_media_engagement",
        )

        glue_database_name = os.getenv("GLUE_DATABASE_NAME", "wistia_video_analytics")
        glue_role_name = os.getenv(
            "GLUE_ROLE_NAME",
            f"{project_name}-{environment_name}-glue-role",
        )
        wistia_secret_name = os.getenv(
            "WISTIA_SECRET_NAME",
            f"/{project_name}/{environment_name}/wistia-api-token",
        )

        # Add Glue Job Config Variables
        glue_version = os.getenv("GLUE_VERSION", "5.0")
        glue_worker_type = os.getenv("GLUE_WORKER_TYPE", "G.1X")
        glue_number_of_workers = int(os.getenv("GLUE_NUMBER_OF_WORKERS", "2"))
        script_prefix = os.getenv("GLUE_SCRIPT_PREFIX", "glue-scripts")
        config_prefix = os.getenv("PIPELINE_CONFIG_PREFIX", "config")

        ingestion_job_name = os.getenv(
            "INGESTION_JOB_NAME",
            f"{project_name}-{environment_name}-ingest-bronze",
        )
        bronze_to_silver_job_name = os.getenv(
            "BRONZE_TO_SILVER_JOB_NAME",
            f"{project_name}-{environment_name}-bronze-to-silver",
        )
        silver_to_gold_job_name = os.getenv(
            "SILVER_TO_GOLD_JOB_NAME",
            f"{project_name}-{environment_name}-silver-to-gold",
        )
        gold_quality_job_name = os.getenv(
            "GOLD_QUALITY_JOB_NAME",
            f"{project_name}-{environment_name}-gold-quality",
        )

        # Glue Workflow coordinates the four Glue jobs as one pipeline run.
        # The schedule is kept configurable so dev can run manually while prod can run daily.
        workflow_name = os.getenv(
            "GLUE_WORKFLOW_NAME",
            f"{project_name}-{environment_name}-workflow",
        )
        pipeline_schedule = os.getenv("PIPELINE_SCHEDULE", "cron(0 2 * * ? *)")
        schedule_enabled = os.getenv("PIPELINE_SCHEDULE_ENABLED", "false").lower() == "true"

        scheduled_ingestion_trigger_name = f"{workflow_name}-scheduled-ingest"
        bronze_to_silver_trigger_name = f"{workflow_name}-bronze-to-silver"
        silver_to_gold_trigger_name = f"{workflow_name}-silver-to-gold"
        gold_quality_trigger_name = f"{workflow_name}-gold-quality"

        additional_python_modules = os.getenv(
            "GLUE_ADDITIONAL_PYTHON_MODULES",
            "requests==2.33.0,tenacity==9.1.4,PyYAML==6.0.3,python-dotenv==1.2.2",
        )


        Tags.of(self).add("Project", project_name)
        Tags.of(self).add("Environment", environment_name)

        if bucket_name:
            data_bucket = s3.Bucket.from_bucket_name(
                self,
                "WistiaDataLakeBucket",
                bucket_name,
            )
        else:
            data_bucket = s3.Bucket(
                self,
                "WistiaDataLakeBucket",
                block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                encryption=s3.BucketEncryption.S3_MANAGED,
                enforce_ssl=True,
                removal_policy=RemovalPolicy.RETAIN,
            )

        wistia_api_secret = secretsmanager.Secret(
            self,
            "WistiaApiTokenSecret",
            secret_name=wistia_secret_name,
            description="Wistia API token used by the video analytics ingestion job.",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template="{}",
                generate_string_key="token",
            ),
            removal_policy=RemovalPolicy.RETAIN,
        )

        glue_role = iam.Role(
            self,
            "WistiaGlueRole",
            role_name=glue_role_name,
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        data_bucket.grant_read_write(glue_role)
        wistia_api_secret.grant_read(glue_role)

        glue_role.add_to_policy(
            iam.PolicyStatement(
                sid="AllowGlueTempAndSparkLogs",
                actions=[
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts",
                ],
                resources=[data_bucket.arn_for_objects("*")],
            )
        )

        # Glue database is the logical catalog namespace Athena queries from.
        # The external gold tables below are registered inside this database
        glue_database = glue.CfnDatabase(
            self,
            "WistiaGlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=glue_database_name,
                description="Glue database for Wistia video analytics bronze, silver, and gold tables.",
            ),
        )

        gold_tables = self._create_gold_tables(
            database=glue_database,
            database_name=glue_database_name,
            bucket=data_bucket,
            gold_prefix=gold_prefix,
            dim_media_table_name=dim_media_table_name,
            dim_visitor_table_name=dim_visitor_table_name,
            fact_media_engagement_table_name=fact_media_engagement_table_name,
        )

        self._deploy_prefix_placeholders(
            bucket=data_bucket,
            prefixes=[
                bronze_prefix,
                f"{bronze_prefix}/media_metadata",
                f"{bronze_prefix}/media_stats",
                f"{bronze_prefix}/media_engagement",
                f"{bronze_prefix}/media_stats_by_date",
                f"{bronze_prefix}/events",
                f"{bronze_prefix}/visitor_stats",
                silver_prefix,
                gold_prefix,
                "tmp",
                "spark-logs",
                "glue-scripts",
                "config",
            ],
        )

        CfnOutput(self, "WistiaDataLakeBucketName", value=data_bucket.bucket_name)
        CfnOutput(self, "WistiaGlueRoleArn", value=glue_role.role_arn)
        CfnOutput(self, "WistiaGlueDatabaseName", value=glue_database_name)
        CfnOutput(self, "WistiaDimMediaTableName", value=dim_media_table_name)
        CfnOutput(self, "WistiaDimVisitorTableName", value=dim_visitor_table_name)
        CfnOutput(self, "WistiaFactMediaEngagementTableName", value=fact_media_engagement_table_name)
        CfnOutput(self, "WistiaApiTokenSecretName", value=wistia_api_secret.secret_name)


        # Add Script Upload + Job Definitions
        project_root = Path(__file__).resolve().parents[1]

        source_asset = s3_assets.Asset(
            self,
            "WistiaProjectSourceAsset",
            path=str(project_root),
            exclude=[
                ".git",
                ".venv",
                "data",
                "docs",
                "infrastructure/cdk.out",
                "infrastructure/.env",
                "infrastructure/.env.*",
                "**/__pycache__",
                "**/*.pyc",
            ],
        )
        source_asset.grant_read(glue_role)

        self._deploy_glue_sources(
            bucket=data_bucket,
            project_root=project_root,
            script_prefix=script_prefix,
            config_prefix=config_prefix,
        )

        pipeline_config_s3_path = f"s3://{data_bucket.bucket_name}/{config_prefix}/pipeline.yml"

        common_default_arguments = {
            "--job-language": "python",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-metrics": "true",
            "--enable-spark-ui": "true",
            "--spark-event-logs-path": f"s3://{data_bucket.bucket_name}/spark-logs/",
            "--TempDir": f"s3://{data_bucket.bucket_name}/tmp/",
            "--extra-py-files": source_asset.s3_object_url,
            "--additional-python-modules": additional_python_modules,
            "--config": pipeline_config_s3_path,
            "--storage-mode": "s3",
        }

        ingestion_job = self._create_glue_job(
            job_id="WistiaIngestionJob",
            job_name=ingestion_job_name,
            glue_role=glue_role,
            glue_version=glue_version,
            worker_type=glue_worker_type,
            number_of_workers=glue_number_of_workers,
            script_location=f"s3://{data_bucket.bucket_name}/{script_prefix}/ingestion/ingest_wistia_raw.py",
            default_arguments={
                **common_default_arguments,
                "--wistia-secret-name": wistia_secret_name,
            },
        )

        bronze_to_silver_job = self._create_glue_job(
            job_id="WistiaBronzeToSilverJob",
            job_name=bronze_to_silver_job_name,
            glue_role=glue_role,
            glue_version=glue_version,
            worker_type=glue_worker_type,
            number_of_workers=glue_number_of_workers,
            script_location=f"s3://{data_bucket.bucket_name}/{script_prefix}/transforms/bronze_to_silver.py",
            default_arguments=common_default_arguments,
        )

        silver_to_gold_job = self._create_glue_job(
            job_id="WistiaSilverToGoldJob",
            job_name=silver_to_gold_job_name,
            glue_role=glue_role,
            glue_version=glue_version,
            worker_type=glue_worker_type,
            number_of_workers=glue_number_of_workers,
            script_location=f"s3://{data_bucket.bucket_name}/{script_prefix}/transforms/silver_to_gold.py",
            default_arguments=common_default_arguments,
        )

        gold_quality_job = self._create_glue_job(
            job_id="WistiaGoldQualityJob",
            job_name=gold_quality_job_name,
            glue_role=glue_role,
            glue_version=glue_version,
            worker_type=glue_worker_type,
            number_of_workers=glue_number_of_workers,
            script_location=f"s3://{data_bucket.bucket_name}/{script_prefix}/quality/check_gold.py",
            default_arguments=common_default_arguments,
        )

        # Glue Workflow gives us a managed orchestration graph for the pipeline.
        # The workflow itself does not process data; it controls job order and state.
        workflow = glue.CfnWorkflow(
            self,
            "WistiaPipelineWorkflow",
            name=workflow_name,
            description="Runs Wistia ingestion, silver/gold transforms, and gold quality checks.",
            max_concurrent_runs=1,
        )

        # Scheduled trigger is disabled by default to avoid surprise runs on deploy.
        # Set PIPELINE_SCHEDULE_ENABLED=true when you are ready for automatic daily runs.
        scheduled_ingestion_trigger = glue.CfnTrigger(
            self,
            "WistiaScheduledIngestionTrigger",
            name=scheduled_ingestion_trigger_name,
            type="SCHEDULED",
            workflow_name=workflow_name,
            schedule=pipeline_schedule,
            start_on_creation=schedule_enabled,
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=ingestion_job.name,
                )
            ],
        )

        bronze_to_silver_trigger = self._create_success_trigger(
            trigger_id="WistiaBronzeToSilverWorkflowTrigger",
            trigger_name=bronze_to_silver_trigger_name,
            workflow_name=workflow_name,
            previous_job=ingestion_job,
            next_job=bronze_to_silver_job,
        )

        silver_to_gold_trigger = self._create_success_trigger(
            trigger_id="WistiaSilverToGoldWorkflowTrigger",
            trigger_name=silver_to_gold_trigger_name,
            workflow_name=workflow_name,
            previous_job=bronze_to_silver_job,
            next_job=silver_to_gold_job,
        )

        gold_quality_trigger = self._create_success_trigger(
            trigger_id="WistiaGoldQualityWorkflowTrigger",
            trigger_name=gold_quality_trigger_name,
            workflow_name=workflow_name,
            previous_job=silver_to_gold_job,
            next_job=gold_quality_job,
        )

        for trigger in (
            scheduled_ingestion_trigger,
            bronze_to_silver_trigger,
            silver_to_gold_trigger,
            gold_quality_trigger,
        ):
            trigger.add_dependency(workflow)

        scheduled_ingestion_trigger.add_dependency(ingestion_job)


        CfnOutput(self, "WistiaIngestionJobName", value=ingestion_job.name)
        CfnOutput(self, "WistiaBronzeToSilverJobName", value=bronze_to_silver_job.name)
        CfnOutput(self, "WistiaSilverToGoldJobName", value=silver_to_gold_job.name)
        CfnOutput(self, "WistiaGoldQualityJobName", value=gold_quality_job.name)
        CfnOutput(self, "WistiaGlueWorkflowName", value=workflow_name)
        CfnOutput(self, "WistiaScheduledIngestionTriggerName", value=scheduled_ingestion_trigger_name)
        CfnOutput(self, "WistiaBronzeToSilverTriggerName", value=bronze_to_silver_trigger_name)
        CfnOutput(self, "WistiaSilverToGoldTriggerName", value=silver_to_gold_trigger_name)
        CfnOutput(self, "WistiaGoldQualityTriggerName", value=gold_quality_trigger_name)


    def _deploy_glue_sources(
        self,
        bucket: s3.IBucket,
        project_root: Path,
        script_prefix: str,
        config_prefix: str,
    ) -> None:
        s3deploy.BucketDeployment(
            self,
            "WistiaGlueIngestionScripts",
            sources=[s3deploy.Source.asset(str(project_root / "src" / "ingestion"))],
            destination_bucket=bucket,
            destination_key_prefix=f"{script_prefix}/ingestion",
            prune=False,
        )

        s3deploy.BucketDeployment(
            self,
            "WistiaGlueTransformScripts",
            sources=[s3deploy.Source.asset(str(project_root / "src" / "transforms"))],
            destination_bucket=bucket,
            destination_key_prefix=f"{script_prefix}/transforms",
            prune=False,
        )

        s3deploy.BucketDeployment(
            self,
            "WistiaGlueQualityScripts",
            sources=[s3deploy.Source.asset(str(project_root / "src" / "quality"))],
            destination_bucket=bucket,
            destination_key_prefix=f"{script_prefix}/quality",
            prune=False,
        )

        s3deploy.BucketDeployment(
            self,
            "WistiaPipelineConfigDeployment",
            sources=[s3deploy.Source.asset(str(project_root / "config"))],
            destination_bucket=bucket,
            destination_key_prefix=config_prefix,
            prune=False,
        )

    # Helper methods
    def _create_glue_job(
        self,
        *,
        job_id: str,
        job_name: str,
        glue_role: iam.IRole,
        glue_version: str,
        worker_type: str,
        number_of_workers: int,
        script_location: str,
        default_arguments: dict[str, str],
    ) -> glue.CfnJob:
        return glue.CfnJob(
            self,
            job_id,
            name=job_name,
            role=glue_role.role_arn,
            glue_version=glue_version,
            worker_type=worker_type,
            number_of_workers=number_of_workers,
            max_retries=0,
            timeout=120,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1,
            ),
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=script_location,
            ),
            default_arguments=default_arguments,
        )

    def _create_success_trigger(
        self,
        *,
        trigger_id: str,
        trigger_name: str,
        workflow_name: str,
        previous_job: glue.CfnJob,
        next_job: glue.CfnJob,
    ) -> glue.CfnTrigger:
        """Create a conditional trigger that runs next_job after previous_job succeeds.

        This is the Glue Workflow equivalent of a simple DAG edge:
        previous job SUCCEEDED -> start next job.
        """
        trigger = glue.CfnTrigger(
            self,
            trigger_id,
            name=trigger_name,
            type="CONDITIONAL",
            workflow_name=workflow_name,
            start_on_creation=True,
            predicate=glue.CfnTrigger.PredicateProperty(
                logical="AND",
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        job_name=previous_job.name,
                        logical_operator="EQUALS",
                        state="SUCCEEDED",
                    )
                ],
            ),
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=next_job.name,
                )
            ],
        )

        trigger.add_dependency(previous_job)
        trigger.add_dependency(next_job)

        return trigger


    def _create_gold_tables(
        self,
        *,
        database: glue.CfnDatabase,
        database_name: str,
        bucket: s3.IBucket,
        gold_prefix: str,
        dim_media_table_name: str,
        dim_visitor_table_name: str,
        fact_media_engagement_table_name: str,
    ) -> list[glue.CfnTable]:
        """Create explicit Glue tables for the gold analytics model.

        Dimension tables are unpartitioned. The fact table is partitioned by
        date and media_id, matching the Spark write layout in silver_to_gold.py.
        """
        
        dim_media_table = self._create_external_parquet_table(
            table_id="WistiaDimMediaTable",
            database_name=database_name,
            table_name=dim_media_table_name,
            location=f"s3://{bucket.bucket_name}/{gold_prefix}/dim_media/",
            columns=[
                ("media_id", "string"),
                ("wistia_numeric_id", "bigint"),
                ("title", "string"),
                ("description", "string"),
                ("channel", "string"),
                ("duration_seconds", "double"),
                ("media_type", "string"),
                ("status", "string"),
                ("archived", "boolean"),
                ("created_at", "timestamp"),
                ("updated_at", "timestamp"),
                ("gold_loaded_at", "timestamp"),
            ],
        )

        dim_visitor_table = self._create_external_parquet_table(
            table_id="WistiaDimVisitorTable",
            database_name=database_name,
            table_name=dim_visitor_table_name,
            location=f"s3://{bucket.bucket_name}/{gold_prefix}/dim_visitor/",
            columns=[
                ("visitor_id", "string"),
                ("country", "string"),
                ("region", "string"),
                ("city", "string"),
                ("browser", "string"),
                ("browser_version", "string"),
                ("platform", "string"),
                ("mobile", "boolean"),
                ("first_seen_at", "timestamp"),
                ("last_seen_at", "timestamp"),
                ("event_count", "bigint"),
                ("distinct_media_count", "bigint"),
                ("avg_percent_viewed", "double"),
                ("max_percent_viewed", "double"),
                ("gold_loaded_at", "timestamp"),
            ],
        )

        fact_media_engagement_table = self._create_external_parquet_table(
            table_id="WistiaFactMediaEngagementTable",
            database_name=database_name,
            table_name=fact_media_engagement_table_name,
            location=f"s3://{bucket.bucket_name}/{gold_prefix}/fact_media_engagement/",
            columns=[
                ("visitor_id", "string"),
                ("play_count", "bigint"),
                ("play_rate", "double"),
                ("total_watch_time_seconds", "double"),
                ("total_watch_time_hours", "double"),
                ("watched_percent", "double"),
                ("media_load_count", "bigint"),
                ("media_play_count", "bigint"),
                ("media_hours_watched", "double"),
                ("gold_loaded_at", "timestamp"),
            ],
            partition_keys=[
                ("date", "date"),
                ("media_id", "string"),
            ],

            # Athena partition projection lets Athena calculate partition
            # locations from query predicates instead of requiring a crawler
            # or MSCK REPAIR TABLE after every pipeline run.
            parameters={
                "projection.enabled": "true",
                "projection.date.type": "date",
                "projection.date.format": "yyyy-MM-dd",
                "projection.date.range": "2024-01-01,NOW",
                "projection.date.interval": "1",
                "projection.date.interval.unit": "DAYS",
                "projection.media_id.type": "enum",
                "projection.media_id.values": "gskhw4w4lm,v08dlrgr7v",
                "storage.location.template": (
                    f"s3://{bucket.bucket_name}/{gold_prefix}/fact_media_engagement/"
                    "date=${date}/media_id=${media_id}/"
                ),
            },
        )

        tables = [dim_media_table, dim_visitor_table, fact_media_engagement_table]
        for table in tables:
            table.add_dependency(database)

        return tables

    def _create_external_parquet_table(
        self,
        *,
        table_id: str,
        database_name: str,
        table_name: str,
        location: str,
        columns: list[tuple[str, str]],
        partition_keys: list[tuple[str, str]] | None = None,
        parameters: dict[str, str] | None = None,
    ) -> glue.CfnTable:
        """Create a Glue external table over Parquet files in S3.

        The table stores metadata only. The actual gold data remains in S3,
        and Athena uses this schema plus the S3 location to query it.
        """        
        
        table_parameters = {
            "classification": "parquet",
            "EXTERNAL": "TRUE",
        }
        if parameters:
            table_parameters.update(parameters)

        return glue.CfnTable(
            self,
            table_id,
            catalog_id=self.account,
            database_name=database_name,
            table_input=glue.CfnTable.TableInputProperty(
                name=table_name,
                table_type="EXTERNAL_TABLE",
                parameters=table_parameters,
                partition_keys=[
                    glue.CfnTable.ColumnProperty(name=name, type=column_type)
                    for name, column_type in (partition_keys or [])
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    location=location,
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    ),
                    columns=[
                        glue.CfnTable.ColumnProperty(name=name, type=column_type)
                        for name, column_type in columns
                    ],
                ),
            ),
        )


    def _deploy_prefix_placeholders(self, bucket: s3.IBucket, prefixes: list[str]) -> None:
        temp_dir = tempfile.mkdtemp()

        for prefix in prefixes:
            safe_prefix = prefix.strip("/")
            folder_path = os.path.join(temp_dir, safe_prefix)
            os.makedirs(folder_path, exist_ok=True)
            with open(os.path.join(folder_path, ".keep"), "w", encoding="utf-8") as file:
                file.write("")

        s3deploy.BucketDeployment(
            self,
            "WistiaDataLakePrefixPlaceholders",
            sources=[s3deploy.Source.asset(temp_dir)],
            destination_bucket=bucket,
            prune=False,
        )
