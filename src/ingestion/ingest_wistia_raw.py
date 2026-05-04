from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import yaml
from dotenv import load_dotenv

from src.ingestion.wistia_client import WistiaClient, WistiaClientConfig

LOGGER = logging.getLogger(__name__)
VISITOR_KEY_NAMES = {"visitor_key", "visitorKey"}


def load_pipeline_config(config_path: str = "config/pipeline.yml") -> dict[str, Any]:
    with Path(config_path).open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


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


def write_raw_json(
    *,
    output_base_dir: Path,
    dataset: str,
    run_id: str,
    payload: Any,
    endpoint: str,
    request_params: dict[str, Any] | None = None,
    media_id: str | None = None,
    channel: str | None = None,
    page: int | None = None,
    record_key: str | None = None,
) -> Path:
    ingested_at = utc_now()
    ingest_date = ingested_at.date().isoformat()

    path_parts = [
        output_base_dir,
        dataset,
        f"ingest_date={ingest_date}",
        f"run_id={run_id}",
    ]

    if media_id:
        path_parts.append(f"media_id={media_id}")

    output_dir = Path(*path_parts)
    output_dir.mkdir(parents=True, exist_ok=True)

    file_name_parts = [dataset]
    if media_id:
        file_name_parts.append(media_id)
    if page is not None:
        file_name_parts.append(f"page_{page:05d}")
    if record_key:
        file_name_parts.append(record_key)

    output_path = output_dir / ("_".join(file_name_parts) + ".json")

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

    output_path.write_text(json.dumps(envelope, indent=2), encoding="utf-8")
    LOGGER.info("Wrote raw %s payload to %s", dataset, output_path)

    return output_path


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    load_dotenv()
    load_dotenv("infrastructure/.env", override=False)

    api_token = os.getenv("WISTIA_API_TOKEN")
    if not api_token:
        raise RuntimeError("Missing WISTIA_API_TOKEN. Add it to .env or infrastructure/.env.")

    pipeline_config = load_pipeline_config()
    wistia_config = pipeline_config["wistia"]
    ingestion_config = pipeline_config.get("ingestion", {})
    storage_config = pipeline_config.get("storage", {})

    run_id = utc_now().strftime("%Y%m%dT%H%M%SZ") + f"-{uuid4().hex[:8]}"
    output_base_dir = Path(storage_config.get("local_bronze_dir", "data/bronze/wistia"))

    start_date = os.getenv("WISTIA_START_DATE")
    end_date = os.getenv("WISTIA_END_DATE")
    if not start_date or not end_date:
        start_date, end_date = default_date_window()

    client = WistiaClient(
        WistiaClientConfig(
            api_token=api_token,
            base_url=wistia_config["base_url"],
            api_version=wistia_config.get("api_version", "2026-03"),
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
        output_base_dir=output_base_dir,
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
            output_base_dir=output_base_dir,
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
            output_base_dir=output_base_dir,
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
            output_base_dir=output_base_dir,
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
                output_base_dir=output_base_dir,
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
            output_base_dir=output_base_dir,
            dataset="visitor_stats",
            run_id=run_id,
            payload=visitor_payload,
            endpoint=visitor_endpoint,
            request_params={"visitor_key": visitor_key},
            record_key=visitor_safe_key,
        )


    LOGGER.info("Raw Wistia ingestion complete run_id=%s visitor_count=%s", run_id, len(visitor_keys))


if __name__ == "__main__":
    main()
