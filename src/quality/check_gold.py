from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    details: str


def load_config(config_path: str) -> dict[str, Any]:
    with Path(config_path).open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def config_path(config: dict[str, Any], key: str, default: str) -> str:
    storage = config.get("storage", {})
    return storage.get(key) or config.get(key) or default


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("wistia-gold-quality-checks")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_gold_table(spark: SparkSession, gold_dir: str, table_name: str) -> DataFrame:
    table_path = Path(gold_dir) / table_name
    if not table_path.exists():
        raise FileNotFoundError(f"Missing gold table path: {table_path}")

    return spark.read.parquet(str(table_path))


def add_result(results: list[CheckResult], name: str, passed: bool, details: str) -> None:
    results.append(CheckResult(name=name, passed=passed, details=details))


def count_nulls(df: DataFrame, column_name: str) -> int:
    return df.filter(F.col(column_name).isNull()).count()


def count_duplicate_keys(df: DataFrame, key_columns: list[str]) -> int:
    return df.groupBy(*key_columns).count().filter(F.col("count") > 1).count()


def count_outside_range(
    df: DataFrame,
    column_name: str,
    min_value: float,
    max_value: float,
    allow_null: bool = True,
) -> int:
    invalid_condition = (F.col(column_name) < F.lit(min_value)) | (F.col(column_name) > F.lit(max_value))
    if not allow_null:
        invalid_condition = invalid_condition | F.col(column_name).isNull()

    return df.filter(invalid_condition).count()


def count_negative_values(df: DataFrame, column_name: str, allow_null: bool = True) -> int:
    invalid_condition = F.col(column_name) < F.lit(0)
    if not allow_null:
        invalid_condition = invalid_condition | F.col(column_name).isNull()

    return df.filter(invalid_condition).count()


def count_left_anti(left_df: DataFrame, right_df: DataFrame, key_columns: list[str]) -> int:
    return left_df.select(*key_columns).dropDuplicates().join(
        right_df.select(*key_columns).dropDuplicates(),
        key_columns,
        "left_anti",
    ).count()


def check_dim_media(
    dim_media: DataFrame,
    expected_media_count: int,
    expected_channels: set[str],
) -> list[CheckResult]:
    results: list[CheckResult] = []

    row_count = dim_media.count()
    add_result(
        results,
        "dim_media.row_count",
        row_count == expected_media_count,
        f"expected={expected_media_count}, actual={row_count}",
    )

    for column_name in ("media_id", "title", "channel"):
        null_count = count_nulls(dim_media, column_name)
        add_result(
            results,
            f"dim_media.{column_name}.not_null",
            null_count == 0,
            f"null_count={null_count}",
        )

    duplicate_count = count_duplicate_keys(dim_media, ["media_id"])
    add_result(
        results,
        "dim_media.media_id.unique",
        duplicate_count == 0,
        f"duplicate_key_count={duplicate_count}",
    )

    invalid_channel_count = dim_media.filter(~F.col("channel").isin(sorted(expected_channels))).count()
    add_result(
        results,
        "dim_media.channel.expected_values",
        invalid_channel_count == 0,
        f"expected={sorted(expected_channels)}, invalid_count={invalid_channel_count}",
    )

    return results


def check_dim_visitor(dim_visitor: DataFrame) -> list[CheckResult]:
    results: list[CheckResult] = []

    row_count = dim_visitor.count()
    add_result(results, "dim_visitor.row_count_positive", row_count > 0, f"actual={row_count}")

    null_count = count_nulls(dim_visitor, "visitor_id")
    add_result(results, "dim_visitor.visitor_id.not_null", null_count == 0, f"null_count={null_count}")

    duplicate_count = count_duplicate_keys(dim_visitor, ["visitor_id"])
    add_result(
        results,
        "dim_visitor.visitor_id.unique",
        duplicate_count == 0,
        f"duplicate_key_count={duplicate_count}",
    )

    non_positive_event_count = dim_visitor.filter(F.col("event_count") <= 0).count()
    add_result(
        results,
        "dim_visitor.event_count.positive",
        non_positive_event_count == 0,
        f"invalid_count={non_positive_event_count}",
    )

    for column_name in ("avg_percent_viewed", "max_percent_viewed"):
        invalid_count = count_outside_range(dim_visitor, column_name, 0, 1)
        add_result(
            results,
            f"dim_visitor.{column_name}.between_0_and_1",
            invalid_count == 0,
            f"invalid_count={invalid_count}",
        )

    return results


def check_fact_media_engagement(
    fact_media_engagement: DataFrame,
    dim_media: DataFrame,
    dim_visitor: DataFrame,
) -> list[CheckResult]:
    results: list[CheckResult] = []

    row_count = fact_media_engagement.count()
    add_result(results, "fact_media_engagement.row_count_positive", row_count > 0, f"actual={row_count}")

    for column_name in ("media_id", "visitor_id", "date"):
        null_count = count_nulls(fact_media_engagement, column_name)
        add_result(
            results,
            f"fact_media_engagement.{column_name}.not_null",
            null_count == 0,
            f"null_count={null_count}",
        )

    duplicate_count = count_duplicate_keys(fact_media_engagement, ["media_id", "visitor_id", "date"])
    add_result(
        results,
        "fact_media_engagement.grain.unique",
        duplicate_count == 0,
        f"duplicate_key_count={duplicate_count}",
    )

    for column_name in (
        "play_count",
        "total_watch_time_seconds",
        "total_watch_time_hours",
        "media_load_count",
        "media_play_count",
        "media_hours_watched",
    ):
        invalid_count = count_negative_values(fact_media_engagement, column_name)
        add_result(
            results,
            f"fact_media_engagement.{column_name}.non_negative",
            invalid_count == 0,
            f"invalid_count={invalid_count}",
        )

    for column_name in ("play_rate", "watched_percent"):
        invalid_count = count_outside_range(fact_media_engagement, column_name, 0, 1)
        add_result(
            results,
            f"fact_media_engagement.{column_name}.between_0_and_1",
            invalid_count == 0,
            f"invalid_count={invalid_count}",
        )

    missing_media_count = count_left_anti(fact_media_engagement, dim_media, ["media_id"])
    add_result(
        results,
        "fact_media_engagement.media_id.exists_in_dim_media",
        missing_media_count == 0,
        f"missing_media_count={missing_media_count}",
    )

    missing_visitor_count = count_left_anti(fact_media_engagement, dim_visitor, ["visitor_id"])
    add_result(
        results,
        "fact_media_engagement.visitor_id.exists_in_dim_visitor",
        missing_visitor_count == 0,
        f"missing_visitor_count={missing_visitor_count}",
    )

    return results


def log_results(results: list[CheckResult]) -> None:
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        log_func = LOGGER.info if result.passed else LOGGER.error
        log_func("[%s] %s - %s", status, result.name, result.details)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/pipeline.yml")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    config = load_config(args.config)
    gold_dir = config_path(config, "local_gold_dir", "data/gold/wistia")

    expected_media = config.get("wistia", {}).get("media", [])
    expected_media_count = len(expected_media)
    expected_channels = {media["channel"] for media in expected_media if media.get("channel")}

    spark = create_spark()

    try:
        dim_media = read_gold_table(spark, gold_dir, "dim_media")
        dim_visitor = read_gold_table(spark, gold_dir, "dim_visitor")
        fact_media_engagement = read_gold_table(spark, gold_dir, "fact_media_engagement")

        results: list[CheckResult] = []
        results.extend(check_dim_media(dim_media, expected_media_count, expected_channels))
        results.extend(check_dim_visitor(dim_visitor))
        results.extend(check_fact_media_engagement(fact_media_engagement, dim_media, dim_visitor))

        log_results(results)

        failed_results = [result for result in results if not result.passed]
        if failed_results:
            raise SystemExit(f"Gold quality checks failed: {len(failed_results)} failure(s)")

        LOGGER.info("All gold quality checks passed: %s check(s)", len(results))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
