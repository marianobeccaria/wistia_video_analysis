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

## Assumptions

* Channel mapping is inferred from Wistia media names and should be confirmed with the SME/business owner.
