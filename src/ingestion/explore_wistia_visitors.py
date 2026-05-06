from __future__ import annotations

import json
import logging
import os
from typing import Any

from dotenv import load_dotenv

from src.ingestion.explore_wistia_api import describe_json_shape, load_pipeline_config
from src.ingestion.wistia_client import WistiaClient, WistiaClientConfig

LOGGER = logging.getLogger(__name__)
VISITOR_KEY_NAMES = {"visitor_key", "visitorKey"}
EVENT_KEY_NAMES = {"event_key", "eventKey"}


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


def extract_records(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        for key in ("events", "data", "items", "results"):
            records = payload.get(key)
            if isinstance(records, list):
                return records

    return []


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
    auth_config = wistia_config.get("auth", {})

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

    events_endpoint = wistia_config["endpoints"].get("events", "/stats/events")
    visitor_endpoint_template = wistia_config["endpoints"]["visitor_stats"]
    event_endpoint_template = wistia_config["endpoints"].get("event_detail", "/stats/events/{event_key}")

    per_page = min(int(ingestion_config.get("page_size", 100)), 5)

    for media in wistia_config["media"]:
        media_id = media["media_id"]
        channel = media["channel"]

        events_payload = client.get_json(
            events_endpoint,
            params={
                "media_id": media_id,
                "page": 1,
                "per_page": per_page,
            },
        )

        LOGGER.info(
            "%s events sanitized response shape:\n%s",
            channel,
            json.dumps(describe_json_shape(events_payload), indent=2),
        )

        records = extract_records(events_payload)
        visitor_keys = sorted(collect_values_by_key(records, VISITOR_KEY_NAMES))
        event_keys = sorted(collect_values_by_key(records, EVENT_KEY_NAMES))

        LOGGER.info(
            "%s events record_count=%s visitor_key_count=%s event_key_count=%s",
            channel,
            len(records),
            len(visitor_keys),
            len(event_keys),
        )

        for event_key in event_keys[:1]:
            event_payload = client.get_json(event_endpoint_template.format(event_key=event_key))
            LOGGER.info(
                "%s event_detail sanitized response shape:\n%s",
                channel,
                json.dumps(describe_json_shape(event_payload), indent=2),
            )

        for visitor_key in visitor_keys[:3]:
            visitor_payload = client.get_json(visitor_endpoint_template.format(visitor_key=visitor_key))
            LOGGER.info(
                "%s visitor_stats sanitized response shape:\n%s",
                channel,
                json.dumps(describe_json_shape(visitor_payload), indent=2),
            )


if __name__ == "__main__":
    main()
