from __future__ import annotations

import os
import tempfile

from aws_cdk import (
    CfnOutput,
    RemovalPolicy,
    Stack,
    Tags,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
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

        glue_database_name = os.getenv("GLUE_DATABASE_NAME", "wistia_video_analytics")
        glue_role_name = os.getenv(
            "GLUE_ROLE_NAME",
            f"{project_name}-{environment_name}-glue-role",
        )
        wistia_secret_name = os.getenv(
            "WISTIA_SECRET_NAME",
            f"/{project_name}/{environment_name}/wistia-api-token",
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

        glue.CfnDatabase(
            self,
            "WistiaGlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=glue_database_name,
                description="Glue database for Wistia video analytics bronze, silver, and gold tables.",
            ),
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
        CfnOutput(self, "WistiaApiTokenSecretName", value=wistia_api_secret.secret_name)

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
