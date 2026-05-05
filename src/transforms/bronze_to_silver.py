from __future__ import annotations

import argparse
import logging
from typing import Any
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from src.common.paths import is_s3_uri, join_path, load_yaml, resolve_layer_path

LOGGER = logging.getLogger(__name__)


def load_config(config_path: str) -> dict[str, Any]:
    return load_yaml(config_path)

def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("wistia-bronze-to-silver")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

# Reads all JSON files under a bronze dataset folder recursively. 
# Adds source_file and ingest_date. 
# 'source_file' helps trace a silver row back to its original raw JSON file.
def read_bronze_dataset(spark: SparkSession, bronze_dir: str, dataset: str) -> DataFrame | None:
    dataset_path = join_path(bronze_dir, dataset)
    if not is_s3_uri(dataset_path) and not Path(dataset_path).exists():
        LOGGER.warning("Skipping missing bronze dataset: %s", dataset_path)
        return None

    return (
        spark.read.option("multiLine", True)
        .option("recursiveFileLookup", True)
        .json(dataset_path)
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingest_date", F.to_date("ingested_at"))
    )

# Writes each cleaned DataFrame as Parquet. 
# It uses overwrite mode and partitions by columns like ingest_date and media_id
def write_silver(df: DataFrame, silver_dir: str, dataset: str, partition_cols: list[str]) -> None:
    output_path = join_path(silver_dir, dataset)
    LOGGER.info("Writing silver dataset=%s path=%s", dataset, output_path)

    (
        df.write.mode("overwrite")
        .partitionBy(*partition_cols)
        .parquet(output_path)
    )

#  Explodes the metadata payload array and creates clean media columns like media_id, title, duration_seconds, status, created_at, and updated_at
def transform_media_metadata(df: DataFrame) -> DataFrame:
    media = df.select(
        "pipeline_run_id",
        "ingested_at",
        "source_file",
        F.explode_outer("payload").alias("media"),
    )

    return media.select(
        F.col("media.hashed_id").alias("media_id"),
        F.col("media.id").alias("wistia_numeric_id"),
        F.col("media.name").alias("title"),
        F.col("media.description").alias("description"),
        F.col("media.duration").cast("double").alias("duration_seconds"),
        F.col("media.type").alias("media_type"),
        F.col("media.status").alias("status"),
        F.col("media.archived").cast("boolean").alias("archived"),
        F.to_timestamp("media.created").alias("created_at"),
        F.to_timestamp("media.updated").alias("updated_at"),
        "pipeline_run_id",
        F.to_timestamp("ingested_at").alias("ingested_at"),
        "source_file",
        F.to_date("ingested_at").alias("ingest_date"),
    ).dropDuplicates(["media_id", "pipeline_run_id"])

# Flattens aggregate media metrics like load_count, play_count, play_rate, hours_watched, engagement, and visitor_count
def transform_media_stats(df: DataFrame) -> DataFrame:
    return df.select(
        "media_id",
        "channel",
        F.col("payload.load_count").cast("long").alias("load_count"),
        F.col("payload.play_count").cast("long").alias("play_count"),
        F.col("payload.play_rate").cast("double").alias("play_rate"),
        F.col("payload.hours_watched").cast("double").alias("hours_watched"),
        F.col("payload.engagement").cast("double").alias("engagement"),
        F.col("payload.visitors").cast("long").alias("visitor_count"),
        "pipeline_run_id",
        F.to_timestamp("ingested_at").alias("ingested_at"),
        "source_file",
        F.to_date("ingested_at").alias("ingest_date"),
    ).dropDuplicates(["media_id", "pipeline_run_id"])

# Explodes the daily metrics array into one row per media_id per metric_date
def transform_media_stats_by_date(df: DataFrame) -> DataFrame:
    daily = df.select(
        "media_id",
        "channel",
        "pipeline_run_id",
        "ingested_at",
        "source_file",
        F.explode_outer("payload").alias("daily"),
    )

    return daily.select(
        "media_id",
        "channel",
        F.to_date("daily.date").alias("metric_date"),
        F.col("daily.load_count").cast("long").alias("load_count"),
        F.col("daily.play_count").cast("long").alias("play_count"),
        F.col("daily.hours_watched").cast("double").alias("hours_watched"),
        "pipeline_run_id",
        F.to_timestamp("ingested_at").alias("ingested_at"),
        "source_file",
        F.to_date("ingested_at").alias("ingest_date"),
    ).dropDuplicates(["media_id", "metric_date", "pipeline_run_id"])

# Turns the engagement and rewatch arrays into timeline rows. 
# posexplode_outer gives each point a timeline_index, and arrays_zip keeps engagement and rewatch values aligned.
def transform_media_engagement(df: DataFrame) -> DataFrame:
    zipped = df.select(
        "media_id",
        "channel",
        "pipeline_run_id",
        "ingested_at",
        "source_file",
        F.col("payload.engagement").cast("double").alias("overall_engagement"),
        F.posexplode_outer(
            F.arrays_zip("payload.engagement_data", "payload.rewatch_data")
        ).alias("timeline_index", "point"),
    )

    return zipped.select(
        "media_id",
        "channel",
        F.col("timeline_index").cast("int").alias("timeline_index"),
        F.col("point.engagement_data").cast("long").alias("engagement_value"),
        F.col("point.rewatch_data").cast("long").alias("rewatch_value"),
        "overall_engagement",
        "pipeline_run_id",
        F.to_timestamp("ingested_at").alias("ingested_at"),
        "source_file",
        F.to_date("ingested_at").alias("ingest_date"),
    ).dropDuplicates(["media_id", "timeline_index", "pipeline_run_id"])

# This is the main 'visitor-level' table. It explodes event payload arrays into one row per event and keeps fields like event_key, visitor_key, media_id, received_at, percent_viewed, location, organization, and browser/device details. 
# It intentionally excludes raw ip and email
def transform_events(df: DataFrame) -> DataFrame:
    events = df.select(
        "pipeline_run_id",
        "ingested_at",
        "source_file",
        F.explode_outer("payload").alias("event"),
    )

    return events.select(
        F.col("event.event_key").alias("event_key"),
        F.col("event.visitor_key").alias("visitor_key"),
        F.col("event.media_id").alias("media_id"),
        F.col("event.media_name").alias("media_name"),
        F.to_timestamp("event.received_at", "yyyy-MM-dd'T'HH:mm:ss.SSSX").alias("received_at"),
        F.to_date(F.to_timestamp("event.received_at", "yyyy-MM-dd'T'HH:mm:ss.SSSX")).alias("event_date"),
        F.col("event.percent_viewed").cast("double").alias("percent_viewed"),
        F.col("event.country").alias("country"),
        F.col("event.region").alias("region"),
        F.col("event.city").alias("city"),
        F.col("event.lat").cast("double").alias("latitude"),
        F.col("event.lon").cast("double").alias("longitude"),
        F.col("event.org").alias("organization"),
        F.col("event.embed_url").alias("embed_url"),
        F.col("event.media_url").alias("media_url"),
        F.col("event.user_agent_details.browser").alias("browser"),
        F.col("event.user_agent_details.browser_version").alias("browser_version"),
        F.col("event.user_agent_details.platform").alias("platform"),
        F.col("event.user_agent_details.mobile").cast("boolean").alias("mobile"),
        "pipeline_run_id",
        F.to_timestamp("ingested_at").alias("ingested_at"),
        "source_file",
        F.to_date("ingested_at").alias("ingest_date"),
    ).dropDuplicates(["event_key"])

# Flattens visitor rollup data like created_at, last_active_at, load_count, play_count, and user agent fields.
# It intentionally excludes 'visitor_identity'
def transform_visitor_stats(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("payload.visitor_key").alias("visitor_key"),
        F.to_timestamp("payload.created_at").alias("created_at"),
        F.to_timestamp("payload.last_active_at").alias("last_active_at"),
        F.col("payload.last_event_key").alias("last_event_key"),
        F.col("payload.load_count").cast("long").alias("load_count"),
        F.col("payload.play_count").cast("long").alias("play_count"),
        F.col("payload.user_agent_details.browser").alias("browser"),
        F.col("payload.user_agent_details.browser_version").alias("browser_version"),
        F.col("payload.user_agent_details.platform").alias("platform"),
        F.col("payload.user_agent_details.mobile").cast("boolean").alias("mobile"),
        "pipeline_run_id",
        F.to_timestamp("ingested_at").alias("ingested_at"),
        "source_file",
        F.to_date("ingested_at").alias("ingest_date"),
    ).dropDuplicates(["visitor_key", "pipeline_run_id"])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/pipeline.yml")
    parser.add_argument("--storage-mode", choices=["local", "s3"], default="local")
    args, _ = parser.parse_known_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    config = load_config(args.config)
    bronze_dir = resolve_layer_path(config, "bronze", args.storage_mode, args.config)
    silver_dir = resolve_layer_path(config, "silver", args.storage_mode, args.config)

    spark = create_spark()

    transforms = {
        "media_metadata": (transform_media_metadata, ["ingest_date"]),
        "media_stats": (transform_media_stats, ["ingest_date", "media_id"]),
        "media_stats_by_date": (transform_media_stats_by_date, ["ingest_date", "media_id"]),
        "media_engagement": (transform_media_engagement, ["ingest_date", "media_id"]),
        "events": (transform_events, ["ingest_date", "media_id"]),
        "visitor_stats": (transform_visitor_stats, ["ingest_date"]),
    }

    # loops through each dataset, reads bronze, transforms it, writes silver, and logs row counts.
    for dataset, (transform_func, partition_cols) in transforms.items():
        bronze_df = read_bronze_dataset(spark, bronze_dir, dataset)
        if bronze_df is None:
            continue

        silver_df = transform_func(bronze_df)
        write_silver(silver_df, silver_dir, dataset, partition_cols)
        LOGGER.info("Completed dataset=%s rows=%s", dataset, silver_df.count())

    spark.stop()


if __name__ == "__main__":
    main()
