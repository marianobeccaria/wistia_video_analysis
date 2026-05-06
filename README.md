# Wistia Video Analysis

## Recommended Architecture

AWS is recommended for this project:

### Ingestion

* Python ingestion app calls Wistia APIs.
* Target media IDs: gskhw4w4lm, v08dlrgr7v.
* Pull:
  * media metadata
  * media stats
  * media engagement
  * visitor-level stats
  * date-range analytics where useful
* Store raw JSON responses in S3 bronze storage.

### Storage

* S3 data lake:
  * `bronze/wistia/...`
  * `silver/wistia/...`
  * `gold/wistia/...`

* Use Parquet for silver/gold layers.
* Use AWS Glue Data Catalog + Athena for query access.

### Transformation

* Use PySpark, likely AWS Glue Spark jobs.
* No dbt, per requirement.
* Transform raw API responses into:
  * `dim_media`
  * `dim_visitor`
  * `fact_media_engagement`
* Add audit fields like ingested_at, source_file, pipeline_run_id.

### Orchestration

* Use AWS Glue Workflow to coordinate the pipeline.
* Use a Glue scheduled trigger for automatic daily runs when enabled.
* The workflow runs:
  * ingest raw bronze data
  * transform bronze to silver
  * transform silver to gold
  * validate gold tables
* Run daily for 7 consecutive days.

### CI/CD

* Initialize GitHub repo. I checked locally and this folder is not currently a Git repository.
* GitHub Actions should run:
  * Python linting
  * unit tests
  * security checks for secrets
  * package validation
  * optional Terraform/IaC validation
* Deploy AWS resources and jobs through GitHub Actions.

### Reporting

* Use Athena + QuickSight for dashboarding.
* Optional local/dashboard folder can hold dashboard notes, screenshots, or export definitions.
* Main KPIs:
  * plays by media/channel/date
  * play rate
  * total watch time
  * average watched percent
  * unique visitors
  * visitor country/IP distribution
  * Facebook vs YouTube comparison

## Local Runbook

### Prerequisites

Use the `dea-cdk` conda environment with Python 3.11.

```bash
conda activate dea-cdk
python --version
```

Install project dependencies if needed:

```bash
python -m pip install -r requirements.txt
```

Create a local `.env` or `infrastructure/.env` file with the Wistia API token. Do not commit this file.

```env
WISTIA_API_TOKEN=replace_with_real_token
```

### Run The Local Pipeline

Run raw bronze ingestion for the default daily window:

```bash
python -m src.ingestion.ingest_wistia_raw
```

Run raw bronze ingestion for a specific date window:

```bash
WISTIA_START_DATE=2026-05-03 WISTIA_END_DATE=2026-05-04 python -m src.ingestion.ingest_wistia_raw
```

For historical exploration only, use a wider date window. This can take several minutes because visitor-level event pagination may return many pages.

```bash
WISTIA_START_DATE=2024-01-01 WISTIA_END_DATE=2026-05-04 python -m src.ingestion.ingest_wistia_raw
```

Transform bronze JSON to silver Parquet:

```bash
python -m src.transforms.bronze_to_silver
```

Transform silver Parquet to gold dimensional tables:

```bash
python -m src.transforms.silver_to_gold
```

Run gold data quality checks:

```bash
python -m src.quality.check_gold
```

### Local Data Outputs

The local pipeline writes data to these ignored folders:

```txt
data/bronze/wistia/
data/silver/wistia/
data/gold/wistia/
```

Bronze contains raw JSON API payloads wrapped with ingestion metadata. Silver contains cleaned source-aligned Parquet tables. Gold contains dimensional tables for analytics:

```txt
data/gold/wistia/dim_media/
data/gold/wistia/dim_visitor/
data/gold/wistia/fact_media_engagement/
```

### Validation Commands

Check local data sizes:

```bash
du -sh data/bronze/wistia data/silver/wistia data/gold/wistia
```

Check row counts and schemas:

```bash
python - <<'PY'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("wistia-local-check").getOrCreate()

for layer, tables in {
    "silver": ["media_metadata", "media_stats", "media_stats_by_date", "media_engagement", "events", "visitor_stats"],
    "gold": ["dim_media", "dim_visitor", "fact_media_engagement"],
}.items():
    for table in tables:
        path = f"data/{layer}/wistia/{table}"
        df = spark.read.parquet(path)
        print(layer, table, df.count())
        df.printSchema()

spark.stop()
PY
```

## AWS Runbook

### Deploy Infrastructure

Use the `dea-cdk` conda environment and deploy from the `infrastructure` folder.

```bash
conda activate dea-cdk
cd infrastructure
```

Create `infrastructure/.env` from `infrastructure/.env.example`, then fill in the AWS account, region, bucket name, and other environment-specific values. Do not commit `infrastructure/.env`.

```bash
cp .env.example .env
```

Synthesize and deploy the stack:

```bash
cdk synth
cdk deploy
```

The deployment creates or references the S3 data lake bucket, creates the Wistia API token secret, uploads the Glue scripts/config, creates Glue jobs, creates the Glue workflow, and registers explicit Glue catalog tables for the gold layer.

### Set Wistia Secret

After the stack creates the secret, update it with the real Wistia API token. `read -s` lets you type the token without echoing it to the terminal, and `unset` removes the shell variable after the update.

```bash
read -s -p "Wistia token: " WISTIA_TOKEN
echo
aws secretsmanager put-secret-value \
  --secret-id /wistia-video-analysis/dev/wistia-api-token \
  --secret-string "{\"token\":\"$WISTIA_TOKEN\"}"
unset WISTIA_TOKEN
```

Verify that the secret exists without printing the token value:

```bash
aws secretsmanager describe-secret \
  --secret-id /wistia-video-analysis/dev/wistia-api-token
```

### Start Workflow

Run the full pipeline manually through Glue Workflow:

```bash
aws glue start-workflow-run \
  --name wistia-video-analysis-dev-workflow
```

The workflow runs the jobs in this order:

```txt
wistia-video-analysis-dev-ingest-bronze
wistia-video-analysis-dev-bronze-to-silver
wistia-video-analysis-dev-silver-to-gold
wistia-video-analysis-dev-gold-quality
```

Automatic scheduling is controlled by `PIPELINE_SCHEDULE` and `PIPELINE_SCHEDULE_ENABLED` in `infrastructure/.env`. Keep `PIPELINE_SCHEDULE_ENABLED=false` for manual development runs.

### Monitor Workflow

Check the latest workflow run:

```bash
aws glue get-workflow-runs \
  --name wistia-video-analysis-dev-workflow \
  --max-results 1 \
  --include-graph \
  --query 'Runs[0].{RunId:WorkflowRunId,Status:Status,Started:StartedOn,Completed:CompletedOn}' \
  --output table
```

Inspect the job-level status for a specific workflow run:

```bash
RUN_ID=$(aws glue get-workflow-runs \
  --name wistia-video-analysis-dev-workflow \
  --max-results 1 \
  --query 'Runs[0].WorkflowRunId' \
  --output text)

aws glue get-workflow-run \
  --name wistia-video-analysis-dev-workflow \
  --run-id "$RUN_ID" \
  --include-graph \
  --query 'Run.Graph.Nodes[?Type==`JOB`].{Job:Name,Status:JobDetails.JobRuns[0].JobRunState,Started:JobDetails.JobRuns[0].StartedOn,Completed:JobDetails.JobRuns[0].CompletedOn,Error:JobDetails.JobRuns[0].ErrorMessage}' \
  --output table
```

The workflow is successful when all four jobs show `SUCCEEDED` and the workflow status is `COMPLETED`.

### Verify S3 Outputs

Set the bucket name for the current shell:

```bash
BUCKET_NAME=<your-s3-bucket-name>
```

Check bronze, silver, and gold outputs:

```bash
aws s3 ls "s3://${BUCKET_NAME}/bronze/wistia/" --recursive | head
aws s3 ls "s3://${BUCKET_NAME}/silver/wistia/" --recursive | head
aws s3 ls "s3://${BUCKET_NAME}/gold/wistia/" --recursive | head
```

Expected gold folders:

```txt
gold/wistia/dim_media/
gold/wistia/dim_visitor/
gold/wistia/fact_media_engagement/
```

`fact_media_engagement` is partitioned by `date` and `media_id`, so S3 paths look like:

```txt
gold/wistia/fact_media_engagement/date=YYYY-MM-DD/media_id=<wistia-media-id>/
```

### Query Athena

Use the Athena console and select the Glue database:

```txt
wistia_video_analytics
```

Smoke test the gold tables:

```sql
SELECT *
FROM wistia_video_analytics.dim_media
LIMIT 10;
```

```sql
SELECT *
FROM wistia_video_analytics.dim_visitor
LIMIT 10;
```

```sql
SELECT *
FROM wistia_video_analytics.fact_media_engagement
LIMIT 10;
```

Example KPI query:

```sql
SELECT
    f.date,
    m.channel,
    f.media_id,
    SUM(f.play_count) AS plays,
    AVG(f.watched_percent) AS avg_watched_percent,
    SUM(f.total_watch_time_hours) AS total_watch_time_hours
FROM wistia_video_analytics.fact_media_engagement f
JOIN wistia_video_analytics.dim_media m
    ON f.media_id = m.media_id
GROUP BY
    f.date,
    m.channel,
    f.media_id
ORDER BY
    f.date DESC,
    m.channel,
    f.media_id;
```

The explicit Glue tables point Athena directly at the gold Parquet data in S3. After each successful workflow run, Athena reads the newly written gold files without requiring a crawler run.

## CI/CD

The repository includes GitHub Actions workflows under `.github/workflows`.

`ci.yml` runs on pushes and pull requests to validate:

- Python syntax compilation
- optional pytest tests when present
- CDK synth

`deploy.yml` is a manual deployment workflow. It uses GitHub Actions OIDC to assume an AWS deployment role, then runs `cdk synth` and `cdk deploy`.

Required GitHub secret for deployment:

```txt
AWS_DEPLOY_ROLE_ARN
```

## Assumptions

* Channel mapping is inferred from Wistia media names and should be confirmed with the SME/business owner.
