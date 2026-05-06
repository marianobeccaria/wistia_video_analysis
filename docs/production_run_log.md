# Wistia Production Run Log

This document records evidence for FR8: run the Wistia analytics pipeline in production mode for 7 consecutive days.

Workflow name:

```txt
wistia-video-analysis-dev-workflow
```

### Collect daily logs

Evidence to capture: 

- Workflow run ID
- Workflow status: COMPLETED
- Started timestamp
- Completed timestamp
- All four jobs succeeded:
    ingest-bronze
    bronze-to-silver
    silver-to-gold
    gold-quality
- Watermark after the run
- (Optional) S3 gold evidence
- (Optional) Athena row count query result

#### Run the following command to collect evidence:

Latest workflow run

```bash
aws glue get-workflow-runs \
  --name wistia-video-analysis-dev-workflow \
  --max-results 10 \
  --query 'Runs[].{RunId:WorkflowRunId,Status:Status,Started:StartedOn,Completed:CompletedOn}' \
  --output table
```

Job-level details for a run:

```bash
aws glue get-workflow-run \
  --name wistia-video-analysis-dev-workflow \
  --run-id "<workflow-run-id>" \
  --include-graph \
  --query 'Run.Graph.Nodes[?Type==`JOB`].{Job:Name,Status:JobDetails.JobRuns[0].JobRunState,Started:JobDetails.JobRuns[0].StartedOn,Completed:JobDetails.JobRuns[0].CompletedOn,Error:JobDetails.JobRuns[0].ErrorMessage}' \
  --output table
```

Watermark evidence:

```bash
aws s3 cp \
  s3://mbeccaria-dea-wistia-analytics/state/wistia/wistia_ingestion_watermark.json \
  -
```
Gold output evidence:

```bash
aws s3 ls s3://mbeccaria-dea-wistia-analytics/gold/wistia/ --recursive | head
```