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

## Completed Scheduled Run Summary

The scheduled Glue Workflow ran successfully for 10 consecutive days from 2026-05-10 through 2026-05-19. This satisfies FR8, which requires at least 7 consecutive production-mode runs.

Schedule:

```txt
cron(0 2 * * ? *) = daily at 02:00 UTC = 10:00 PM EDT during daylight saving time
```

| Day | Run Date | Workflow Run ID | Status | Started | Completed | Notes |
|---|---|---|---|---|---|---|
| 1 | 2026-05-10 | `wr_eb2b481f39779870c99dc7e6bc4431bb02962188df3de52b6ed2d6c2e0d3cb39` | COMPLETED | 2026-05-10T22:00:01.192000-04:00 | 2026-05-10T22:07:25.556000-04:00 | Scheduled run |
| 2 | 2026-05-11 | `wr_d672addf3926625cb593d1ced70f679e7517761a43d05bedde5743a06a25efd5` | COMPLETED | 2026-05-11T22:00:01.284000-04:00 | 2026-05-11T22:07:20.090000-04:00 | Scheduled run |
| 3 | 2026-05-12 | `wr_f342a1f3b04a1c5716c95935e7cdc9056ff6c3d66ec2be96c7385dd1e627873d` | COMPLETED | 2026-05-12T22:00:01.298000-04:00 | 2026-05-12T22:08:06.069000-04:00 | Scheduled run |
| 4 | 2026-05-13 | `wr_e2e7be342b809bfb5ab20da15820d753cc0fd5d05f934fb9c33178f73a4a81e6` | COMPLETED | 2026-05-13T22:00:01.104000-04:00 | 2026-05-13T22:07:10.111000-04:00 | Scheduled run |
| 5 | 2026-05-14 | `wr_ccad1c5fd32f6a773fae0c3ff710734c144e362730456566707bc899dbba8992` | COMPLETED | 2026-05-14T22:00:01.452000-04:00 | 2026-05-14T22:07:20.833000-04:00 | Scheduled run |
| 6 | 2026-05-15 | `wr_7c32a27b4f79b0a228541967b89e596a71b6f8c86de02011c7e963eeb846ccec` | COMPLETED | 2026-05-15T22:00:01.332000-04:00 | 2026-05-15T22:07:35.358000-04:00 | Scheduled run |
| 7 | 2026-05-16 | `wr_926f437ea81aaaab67dd879380115d41d41dff9a76b74bfc9f035e4e4b5c8c98` | COMPLETED | 2026-05-16T22:00:01.433000-04:00 | 2026-05-16T22:07:31.852000-04:00 | Scheduled run; FR8 7-day requirement met |
| 8 | 2026-05-17 | `wr_c1fa9a371db55cd139addbdf1d23b707430f36c6f46d02b018d33eaa65e9da7e` | COMPLETED | 2026-05-17T22:00:01.093000-04:00 | 2026-05-17T22:07:37.755000-04:00 | Additional successful scheduled run |
| 9 | 2026-05-18 | `wr_b60ce6661da2cfa08d0b7cb85431143f7cf9d9b933f7970c460271b50ae1dd3f` | COMPLETED | 2026-05-18T22:00:01.461000-04:00 | 2026-05-18T22:07:40.194000-04:00 | Additional successful scheduled run |
| 10 | 2026-05-19 | `wr_4b26b61b19351956e4280c004dfce474e830f89fbd99c9bdc3285c9fe4dcd23b` | COMPLETED | 2026-05-19T22:00:01.437000-04:00 | 2026-05-19T22:07:53.104000-04:00 | Additional successful scheduled run |

## Raw AWS CLI Evidence

```txt
--------------------------------------------------------------------------------------------------------------------------------------------------------------
|                                                                       GetWorkflowRuns                                                                      |
+----------------------------------+-----------------------------------------------------------------------+-----------------------------------+-------------+
|             Completed            |                                 RunId                                 |              Started              |   Status    |
+----------------------------------+-----------------------------------------------------------------------+-----------------------------------+-------------+
|  2026-05-19T22:07:53.104000-04:00|  wr_4b26b61b19351956e4280c004dfce474e830f89fbd99c9bdc3285c9fe4dcd23b  |  2026-05-19T22:00:01.437000-04:00 |  COMPLETED  |
|  2026-05-18T22:07:40.194000-04:00|  wr_b60ce6661da2cfa08d0b7cb85431143f7cf9d9b933f7970c460271b50ae1dd3f  |  2026-05-18T22:00:01.461000-04:00 |  COMPLETED  |
|  2026-05-17T22:07:37.755000-04:00|  wr_c1fa9a371db55cd139addbdf1d23b707430f36c6f46d02b018d33eaa65e9da7e  |  2026-05-17T22:00:01.093000-04:00 |  COMPLETED  |
|  2026-05-16T22:07:31.852000-04:00|  wr_926f437ea81aaaab67dd879380115d41d41dff9a76b74bfc9f035e4e4b5c8c98  |  2026-05-16T22:00:01.433000-04:00 |  COMPLETED  |
|  2026-05-15T22:07:35.358000-04:00|  wr_7c32a27b4f79b0a228541967b89e596a71b6f8c86de02011c7e963eeb846ccec  |  2026-05-15T22:00:01.332000-04:00 |  COMPLETED  |
|  2026-05-14T22:07:20.833000-04:00|  wr_ccad1c5fd32f6a773fae0c3ff710734c144e362730456566707bc899dbba8992  |  2026-05-14T22:00:01.452000-04:00 |  COMPLETED  |
|  2026-05-13T22:07:10.111000-04:00|  wr_e2e7be342b809bfb5ab20da15820d753cc0fd5d05f934fb9c33178f73a4a81e6  |  2026-05-13T22:00:01.104000-04:00 |  COMPLETED  |
|  2026-05-12T22:08:06.069000-04:00|  wr_f342a1f3b04a1c5716c95935e7cdc9056ff6c3d66ec2be96c7385dd1e627873d  |  2026-05-12T22:00:01.298000-04:00 |  COMPLETED  |
|  2026-05-11T22:07:20.090000-04:00|  wr_d672addf3926625cb593d1ced70f679e7517761a43d05bedde5743a06a25efd5  |  2026-05-11T22:00:01.284000-04:00 |  COMPLETED  |
|  2026-05-10T22:07:25.556000-04:00|  wr_eb2b481f39779870c99dc7e6bc4431bb02962188df3de52b6ed2d6c2e0d3cb39  |  2026-05-10T22:00:01.192000-04:00 |  COMPLETED  |
+----------------------------------+-----------------------------------------------------------------------+-----------------------------------+-------------+
```
