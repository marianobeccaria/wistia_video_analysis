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
  * `bronze/wistia/raw/...`
  * `silver/wistia/cleaned/...`
  * `gold/wistia/dwh/...`

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

* Use EventBridge Scheduler to trigger daily.
* Use Step Functions to coordinate:
  * ingest media metadata
  * ingest stats/engagement
  * ingest visitor details
  * run PySpark transforms
  * validate/load gold tables
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

## Assumptions

* Channel mapping is inferred from Wistia media names and should be confirmed with the SME/business owner.
