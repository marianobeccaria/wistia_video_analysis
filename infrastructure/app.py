#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path

import aws_cdk as cdk
from dotenv import load_dotenv

from wistia_video_analysis_stack import WistiaVideoAnalysisStack


PROJECT_ROOT = Path(__file__).resolve().parents[1]

load_dotenv(PROJECT_ROOT / "infrastructure" / ".env")

app = cdk.App()

environment_name = os.getenv("ENVIRONMENT", "dev")
stack_name = f"wistia-video-analysis-{environment_name}"

WistiaVideoAnalysisStack(
    app,
    stack_name,
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT") or os.getenv("AWS_ACCOUNT_ID"),
        region=os.getenv("CDK_DEFAULT_REGION") or os.getenv("AWS_REGION", "us-east-1"),
    ),
)

app.synth()
