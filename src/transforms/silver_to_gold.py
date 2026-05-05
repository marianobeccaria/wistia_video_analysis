from __future__ import annotations

import argparse
import logging
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from src.common.paths import join_path, load_yaml, resolve_layer_path


LOGGER = logging.getLogger(__name__)

# reads config/pipeline.yml
def load_config(config_path: str) -> dict[str, Any]:
    return load_yaml(config_path)

# starts Spark and sets the timezone to UTC.
def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("wistia-silver-to-gold")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

# keeps only the newest row per key. 
# latest_by(df, ["media_id"]) keeps the most
def latest_by(df: DataFrame, keys: list[str], order_col: str = "ingested_at") -> DataFrame:
    window = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")

# Writes a DataFrame as Parquet under data/gold/wistia/<table>. 
# It can also partition tables, like the fact table by date and media_id.
def write_gold(df: DataFrame, gold_dir: str, table: str, partition_cols: list[str] | None = None) -> None:
    output_path = join_path(gold_dir, table)

    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(output_path)
    LOGGER.info("Wrote gold table=%s rows=%s path=%s", table, df.count(), output_path)

# builds one row per video/media.
# It keeps the latest metadata per media_id, joins the latest channel value from media_stats, 
# and outputs several fields (media_id, title, channel, duration_seconds, status, created_at, updated_at)
def build_dim_media(silver_dir: str) -> DataFrame:
    media = latest_by(
        spark.read.parquet(join_path(silver_dir, "media_metadata")),

        ["media_id"],
        "ingested_at",
    )

    stats = latest_by(
        spark.read.parquet(join_path(silver_dir, "media_stats")),

        ["media_id"],
        "ingested_at",
    ).select("media_id", "channel")

    return (
        media.join(stats, "media_id", "left")
        .select(
            "media_id",
            "wistia_numeric_id",
            "title",
            "description",
            "channel",
            "duration_seconds",
            "media_type",
            "status",
            "archived",
            "created_at",
            "updated_at",
            F.current_timestamp().alias("gold_loaded_at"),
        )
        .dropDuplicates(["media_id"])
    )

# Builds one row per visitor using the silver events table.
# It groups by raw visitor_key. Then it picks the visitor’s latest known attributes.
# It does not expose raw visitor_key in gold
def build_dim_visitor(silver_dir: str) -> DataFrame:
    events = spark.read.parquet(join_path(silver_dir, "events")).filter(F.col("visitor_key").isNotNull())

    visitor_rollup = events.groupBy("visitor_key").agg(
        F.min("received_at").alias("first_seen_at"),
        F.max("received_at").alias("last_seen_at"),
        F.countDistinct("event_key").alias("event_count"),
        F.countDistinct("media_id").alias("distinct_media_count"),
        F.avg("percent_viewed").alias("avg_percent_viewed"),
        F.max("percent_viewed").alias("max_percent_viewed"),
    )

    latest_window = Window.partitionBy("visitor_key").orderBy(F.col("received_at").desc_nulls_last())
    latest_attrs = (
        events.withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .select(
            "visitor_key",
            "country",
            "region",
            "city",
            "browser",
            "browser_version",
            "platform",
            "mobile",
        )
    )

    return (
        visitor_rollup.join(latest_attrs, "visitor_key", "left")
        .select(
            F.sha2("visitor_key", 256).alias("visitor_id"),
            "country",
            "region",
            "city",
            "browser",
            "browser_version",
            "platform",
            "mobile",
            "first_seen_at",
            "last_seen_at",
            "event_count",
            "distinct_media_count",
            F.round("avg_percent_viewed", 4).alias("avg_percent_viewed"),
            F.round("max_percent_viewed", 4).alias("max_percent_viewed"),
            F.current_timestamp().alias("gold_loaded_at"),
        )
        .dropDuplicates(["visitor_id"])
    )

# builds the main fact table
# reads silver/events, joins dim_media to get video duration, and estimates watch time
# 
def build_fact_media_engagement(silver_dir: str, dim_media: DataFrame) -> DataFrame:
    events = spark.read.parquet(join_path(silver_dir, "events")).filter(F.col("visitor_key").isNotNull())
    media_duration = dim_media.select("media_id", "duration_seconds")
    event_facts = events.join(media_duration, "media_id", "left").withColumn(
        "estimated_watch_seconds",
        F.col("percent_viewed") * F.col("duration_seconds"),
    )

    visitor_daily = event_facts.groupBy(
        "media_id",
        F.sha2("visitor_key", 256).alias("visitor_id"),
        F.col("event_date").alias("date"),
    ).agg(
        F.countDistinct("event_key").alias("play_count"),
        F.avg("percent_viewed").alias("watched_percent"),
        F.sum("estimated_watch_seconds").alias("total_watch_time_seconds"),
    )

    daily_stats = spark.read.parquet(join_path(silver_dir, "media_stats_by_date")).select(
        "media_id",
        F.col("metric_date").alias("date"),
        F.when(F.col("load_count") > 0, F.col("play_count") / F.col("load_count")).alias("play_rate"),
        F.col("load_count").alias("media_load_count"),
        F.col("play_count").alias("media_play_count"),
        F.col("hours_watched").alias("media_hours_watched"),
    )

    return (
        visitor_daily.join(daily_stats, ["media_id", "date"], "left")
        .select(
            "media_id",
            "visitor_id",
            "date",
            "play_count",
            F.round("play_rate", 6).alias("play_rate"),
            F.round("total_watch_time_seconds", 2).alias("total_watch_time_seconds"),
            F.round(F.col("total_watch_time_seconds") / F.lit(3600), 6).alias("total_watch_time_hours"),
            F.round("watched_percent", 4).alias("watched_percent"),
            "media_load_count",
            "media_play_count",
            "media_hours_watched",
            F.current_timestamp().alias("gold_loaded_at"),
        )
        .dropDuplicates(["media_id", "visitor_id", "date"])
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/pipeline.yml")
    parser.add_argument("--storage-mode", choices=["local", "s3"], default="local")
    args, _ = parser.parse_known_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    config = load_config(args.config)
    silver_dir = resolve_layer_path(config, "silver", args.storage_mode, args.config)
    gold_dir = resolve_layer_path(config, "gold", args.storage_mode, args.config)


    global spark
    spark = create_spark()

    dim_media = build_dim_media(silver_dir)
    dim_visitor = build_dim_visitor(silver_dir)
    fact_media_engagement = build_fact_media_engagement(silver_dir, dim_media)

    write_gold(dim_media, gold_dir, "dim_media")
    write_gold(dim_visitor, gold_dir, "dim_visitor")
    write_gold(fact_media_engagement, gold_dir, "fact_media_engagement", ["date", "media_id"])

    spark.stop()


if __name__ == "__main__":
    main()
