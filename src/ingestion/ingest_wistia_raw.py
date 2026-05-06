from __future__ import annotations

import argparse
import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import boto3
from dotenv import load_dotenv

from src.common.paths import join_path, load_yaml, resolve_layer_path, write_text
from src.ingestion.wistia_client import WistiaClient, WistiaClientConfig
from src.ingestion.watermark import resolve_incremental_window, write_success_watermark



LOGGER = logging.getLogger(__name__)
VISITOR_KEY_NAMES = {"visitor_key", "visitorKey"}

def load_pipeline_config(config_path: str = "config/pipeline.yml") -> dict[str, Any]:
    return load_yaml(config_path)

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def default_date_window() -> tuple[str, str]:
    today = date.today()
    yesterday = today - timedelta(days=1)
    return yesterday.isoformat(), today.isoformat()


def collect_values_by_key(value: Any, key_names: set[str]) -> set[str]:
    values: set[str] = set()

    if isinstance(value, dict):
        for key, item in value.items():
            if key in key_names and isinstance(item, str):
                values.add(item)
            values.update(collect_values_by_key(item, key_names))

    if isinstance(value, list):
        for item in value:
            values.update(collect_values_by_key(item, key_names))

    return values

def get_wistia_api_token(secret_name: str | None = None) -> str:
    api_token = os.getenv("WISTIA_API_TOKEN")
    if api_token:
        return api_token

    if not secret_name:
        raise RuntimeError("Missing WISTIA_API_TOKEN and Wistia secret name.")

    secret_value = boto3.client("secretsmanager").get_secret_value(SecretId=secret_name)
    secret_string = secret_value.get("SecretString")
    if not secret_string:
        raise RuntimeError(f"Secret {secret_name} does not contain a SecretString.")

    try:
        parsed_secret = json.loads(secret_string)
    except json.JSONDecodeError:
        return secret_string

    token = parsed_secret.get("token")
    if not token:
        raise RuntimeError(f"Secret {secret_name} must contain a 'token' key.")

    return token


def sanitize_file_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", value)

# accepts output_base_uri: str instead of output_base_dir: Path,
# builds paths with join_path, and writes with write_text
def write_raw_json(
    *,
    output_base_uri: str,
    dataset: str,
    run_id: str,
    payload: Any,
    endpoint: str,
    request_params: dict[str, Any] | None = None,
    media_id: str | None = None,
    channel: str | None = None,
    page: int | None = None,
    record_key: str | None = None,
) -> str:
    ingested_at = utc_now()
    ingest_date = ingested_at.date().isoformat()

    path_parts = [
        dataset,
        f"ingest_date={ingest_date}",
        f"run_id={run_id}",
    ]

    if media_id:
        path_parts.append(f"media_id={media_id}")

    file_name_parts = [dataset]
    if media_id:
        file_name_parts.append(media_id)
    if page is not None:
        file_name_parts.append(f"page_{page:05d}")
    if record_key:
        file_name_parts.append(sanitize_file_part(record_key))

    output_location = join_path(output_base_uri, *path_parts, "_".join(file_name_parts) + ".json")

    envelope = {
        "pipeline_run_id": run_id,
        "ingested_at": ingested_at.isoformat(),
        "source": "wistia",
        "dataset": dataset,
        "media_id": media_id,
        "channel": channel,
        "endpoint": endpoint,
        "request_params": request_params or {},
        "payload": payload,
    }

    write_text(output_location, json.dumps(envelope, indent=2))
    LOGGER.info("Wrote raw %s payload to %s", dataset, output_location)

    return output_location


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/pipeline.yml")
    parser.add_argument("--storage-mode", choices=["local", "s3"], default=os.getenv("PIPELINE_STORAGE_MODE", "local"))
    parser.add_argument("--wistia-secret-name", default=os.getenv("WISTIA_SECRET_NAME"))
    parser.add_argument("--start-date", default=os.getenv("WISTIA_START_DATE"))
    parser.add_argument("--end-date", default=os.getenv("WISTIA_END_DATE"))
    parser.add_argument("--watermark-path", default=os.getenv("WISTIA_WATERMARK_PATH"))
    parser.add_argument("--disable-watermark", action="store_true")
    parser.add_argument("--update-watermark-on-explicit-window", action="store_true")

    args, _ = parser.parse_known_args()

    load_dotenv()
    load_dotenv("infrastructure/.env", override=False)

    pipeline_config = load_pipeline_config(args.config)
    wistia_config = pipeline_config["wistia"]
    ingestion_config = pipeline_config.get("ingestion", {})
    auth_config = wistia_config.get("auth", {})

    run_id = utc_now().strftime("%Y%m%dT%H%M%SZ") + f"-{uuid4().hex[:8]}"
    output_base_uri = resolve_layer_path(pipeline_config, "bronze", args.storage_mode, args.config)

    api_token = get_wistia_api_token(args.wistia_secret_name)

    incremental_window = resolve_incremental_window(
        config=pipeline_config,
        storage_mode=args.storage_mode,
        config_path=args.config,
        explicit_start_date=args.start_date,
        explicit_end_date=args.end_date,
        explicit_watermark_path=args.watermark_path,
        disable_watermark=args.disable_watermark,
        update_watermark_on_explicit_window=args.update_watermark_on_explicit_window,
    )

    start_date = incremental_window.start_date
    end_date = incremental_window.end_date

    LOGGER.info(
        "Using Wistia incremental window start_date=%s end_date=%s watermark_path=%s previous_watermark=%s should_update_watermark=%s",
        start_date,
        end_date,
        incremental_window.watermark_path,
        incremental_window.previous_watermark,
        incremental_window.should_update_watermark,
    )



    client = WistiaClient(
        WistiaClientConfig(
            api_token=api_token,
            base_url=wistia_config["base_url"],
            api_version=wistia_config.get("api_version", "2026-03"),
            auth_scheme=auth_config.get("scheme", "basic"),
            basic_auth_username=auth_config.get("basic_username", "api"),
            basic_auth_token_position=auth_config.get("basic_token_position", "password"),
            timeout_seconds=int(ingestion_config.get("request_timeout_seconds", 30)),
            max_retries=int(ingestion_config.get("max_retries", 5)),
        )
    )

    endpoints = wistia_config["endpoints"]
    media_items = wistia_config["media"]
    media_ids = [media["media_id"] for media in media_items]
    per_page = int(ingestion_config.get("page_size", 100))

    medias_payload = client.get_json(
        endpoints["medias"],
        params={"hashed_ids[]": media_ids, "per_page": per_page},
    )

    visitor_keys: set[str] = set()
    
    write_raw_json(
        output_base_uri=output_base_uri,
        dataset="media_metadata",
        run_id=run_id,
        payload=medias_payload,
        endpoint=endpoints["medias"],
        request_params={"hashed_ids[]": media_ids, "per_page": per_page},
    )


    for media in media_items:
        media_id = media["media_id"]
        channel = media["channel"]

        media_stats_endpoint = endpoints["media_stats"].format(media_id=media_id)
        media_stats_payload = client.get_json(media_stats_endpoint)
        write_raw_json(
            output_base_uri=output_base_uri,
            dataset="media_stats",
            run_id=run_id,
            payload=media_stats_payload,
            endpoint=media_stats_endpoint,
            media_id=media_id,
            channel=channel,
        )

        media_engagement_endpoint = endpoints["media_engagement"].format(media_id=media_id)
        media_engagement_payload = client.get_json(media_engagement_endpoint)
        write_raw_json(
            output_base_uri=output_base_uri,
            dataset="media_engagement",
            run_id=run_id,
            payload=media_engagement_payload,
            endpoint=media_engagement_endpoint,
            media_id=media_id,
            channel=channel,
        )

        media_stats_by_date_endpoint = endpoints["media_stats_by_date"].format(media_id=media_id)
        stats_by_date_params = {"start_date": start_date, "end_date": end_date}
        media_stats_by_date_payload = client.get_json(media_stats_by_date_endpoint, params=stats_by_date_params)
        write_raw_json(
            output_base_uri=output_base_uri,
            dataset="media_stats_by_date",
            run_id=run_id,
            payload=media_stats_by_date_payload,
            endpoint=media_stats_by_date_endpoint,
            request_params=stats_by_date_params,
            media_id=media_id,
            channel=channel,
        )

        page = 1
        while True:
            event_params = {
                "media_id": media_id,
                "start_date": start_date,
                "end_date": end_date,
                "page": page,
                "per_page": per_page,
            }

            events_payload = client.get_json(endpoints["events"], params=event_params)
            if not events_payload:
                LOGGER.info("No more events for media_id=%s page=%s", media_id, page)
                break

            write_raw_json(
                output_base_uri=output_base_uri,
                dataset="events",
                run_id=run_id,
                payload=events_payload,
                endpoint=endpoints["events"],
                request_params=event_params,
                media_id=media_id,
                channel=channel,
                page=page,
            )

            visitor_keys.update(collect_values_by_key(events_payload, VISITOR_KEY_NAMES))

            if not isinstance(events_payload, list) or len(events_payload) < per_page:
                break

            page += 1

    for visitor_key in sorted(visitor_keys):
        visitor_endpoint = endpoints["visitor_stats"].format(visitor_key=visitor_key)
        visitor_payload = client.get_json(visitor_endpoint)
        visitor_safe_key = visitor_key.replace("/", "_")

        write_raw_json(
            output_base_uri=output_base_uri,
            dataset="visitor_stats",
            run_id=run_id,
            payload=visitor_payload,
            endpoint=visitor_endpoint,
            request_params={"visitor_key": visitor_key},
            record_key=visitor_safe_key,
        )

    if incremental_window.should_update_watermark:
        write_success_watermark(
            path=incremental_window.watermark_path,
            config=pipeline_config,
            run_id=run_id,
            window=incremental_window,
            storage_mode=args.storage_mode,
            visitor_count=len(visitor_keys),
            media_count=len(media_items),
        )
    else:
        LOGGER.info("Skipping watermark update for this run.")


    LOGGER.info("Raw Wistia ingestion complete run_id=%s visitor_count=%s", run_id, len(visitor_keys))


if __name__ == "__main__":
    main()
