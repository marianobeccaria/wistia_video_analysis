# Requirements Traceability

This document maps the Wistia Video Analytics project requirements to the implemented code, infrastructure, documentation, and AWS artifacts.

## Summary

| ID | Requirement | Status | Implementation / Evidence |
|---|---|---|---|
| FR1 | Design your own architecture for ingestion, processing, and storage | Complete | Architecture documented in `README.md`. Implemented with S3 bronze/silver/gold layers, Glue jobs, Glue Workflow, Secrets Manager, Glue Catalog, and Athena. |
| FR2 | Authenticate to Wistia Stats API using token-based Basic Auth | Complete | `src/ingestion/wistia_client.py` supports configurable auth and uses Basic Auth from `config/pipeline.yml`. Token is read from local env for local runs or AWS Secrets Manager for Glue runs. |
| FR3 | Extract media metadata such as title, ID, hashed ID, created_at | Complete | `src/ingestion/ingest_wistia_raw.py` extracts media metadata from `/medias`. `src/transforms/bronze_to_silver.py` maps metadata to silver columns such as `media_id`, `wistia_numeric_id`, `title`, `created_at`, and `updated_at`. |
| FR4 | Extract engagement metrics such as plays, play rate, watch time | Complete | Ingestion extracts media stats, media stats by date, and engagement arrays. Silver/gold transforms expose `play_count`, `play_rate`, `hours_watched`, `total_watch_time_hours`, `watched_percent`, and related metrics. |
| FR5 | Extract visitor-level data such as IP and engagement events | Complete with privacy handling | Bronze stores raw event payloads from `/stats/events` and visitor stats from `/stats/visitors/{visitor_key}`. Silver/gold expose visitor engagement, location, browser, platform, and hashed visitor IDs while intentionally excluding raw IP, email, and visitor identity fields. |
| FR6 | Implement pagination to fetch all pages of results | Complete | `src/ingestion/ingest_wistia_raw.py` paginates event extraction with `page` and `per_page` until Wistia returns an empty or short page. |
| FR7 | Implement incremental ingestion based on created_at/updated_at | Complete | `src/ingestion/watermark.py` persists an incremental watermark to local/S3 state. `src/ingestion/ingest_wistia_raw.py` resolves the next incremental date window and updates the watermark only after successful ingestion. |
| FR8 | Run this pipeline in production mode for 7 consecutive days | In progress | AWS Glue scheduled trigger is enabled for daily runs. Evidence should be recorded in `docs/production_run_log.md` for 7 consecutive successful workflow runs. |
| FR9 | Implement a CI/CD pipeline using GitHub Actions or equivalent | Complete | `.github/workflows/ci.yml` validates Python and CDK. `.github/workflows/deploy.yml` provides manual CDK deploy through GitHub OIDC. Setup is documented in `docs/github_actions_manual_deploy_setup.md`. |
| FR10 | Store results in a structured data model, DWH, or cloud database | Complete | Gold dimensional model is written to S3 as Parquet and registered in Glue Catalog for Athena: `dim_media`, `dim_visitor`, and `fact_media_engagement`. |
| FR11 | Create final reports or dashboards for insights | Complete as Athena reporting queries | `docs/athena_reporting_queries.md` contains Athena queries for plays, watch time, watched percent, unique visitors, Facebook vs YouTube comparison, location, and device/platform breakdown. |
| FR12 | Submit a GitHub repo with documentation, pipeline code, CI/CD setup, and instructions | Complete pending final push/review | Repository contains pipeline code, CDK infrastructure, CI/CD workflows, README runbooks, reporting queries, deployment setup docs, and this requirements traceability document. |

## Architecture Artifacts

- `README.md`: recommended architecture, local runbook, AWS runbook, and operational commands.
- `infrastructure/wistia_video_analysis_stack.py`: CDK stack for S3, Secrets Manager, Glue role, Glue database, Glue jobs, Glue Workflow, triggers, and explicit Glue tables.
- `config/pipeline.yml`: pipeline configuration for Wistia API endpoints, media/channel mapping, ingestion settings, storage prefixes, and table names.

## Pipeline Code Artifacts

- `src/ingestion/wistia_client.py`: Wistia API client with authentication, timeout handling, retries, status-code handling, and logging.
- `src/ingestion/ingest_wistia_raw.py`: bronze ingestion for media metadata, media stats, engagement, date-based stats, events, and visitor stats.
- `src/ingestion/watermark.py`: persisted incremental watermark state and incremental window resolution.
- `src/transforms/bronze_to_silver.py`: Spark transform from raw bronze JSON to cleaned silver Parquet tables.
- `src/transforms/silver_to_gold.py`: Spark transform from silver tables to gold dimensional model.
- `src/quality/check_gold.py`: gold data quality checks.

## Data Model Artifacts

Gold tables:

```txt
dim_media
dim_visitor
fact_media_engagement
```

The gold layer is stored in:

```txt
s3://mbeccaria-dea-wistia-analytics/gold/wistia/
```

Athena queries these tables through the Glue database:

```txt
wistia_video_analytics
```

## CI/CD Artifacts

- `.github/workflows/ci.yml`: validates Python compilation, optional tests, and CDK synth.
- `.github/workflows/deploy.yml`: manually deploys the CDK stack through GitHub Actions OIDC.
- `docs/github_actions_manual_deploy_setup.md`: documents reusable setup steps for GitHub OIDC and the AWS deploy role.

## Reporting Artifacts

- `docs/athena_reporting_queries.md`: Athena SQL reporting queries for project insights.
- Athena console: used to validate the explicit Glue gold tables and run business queries.

## Production Run Evidence

FR8 requires 7 consecutive successful production runs. Track those runs in:

```txt
docs/production_run_log.md
```

For each run, capture:

- workflow run ID
- workflow status
- start and completion timestamps
- job-level status for all Glue jobs
- latest watermark value
- optional S3/Athena evidence

## Known Assumptions

- Channel mapping is configured in `config/pipeline.yml` based on inferred Wistia media names and should be confirmed with the SME/business owner.
- Gold visitor IDs are hashed. Raw visitor keys, IP addresses, emails, and visitor identity payloads are intentionally not exposed in silver/gold analytics tables.
- The CDK deploy workflow is manual by design so infrastructure changes are reviewed before deployment.
