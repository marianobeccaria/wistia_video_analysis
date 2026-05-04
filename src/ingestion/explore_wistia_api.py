from __future__ import annotations

import logging
import os
from pathlib import Path
import json
from datetime import date, datetime
from typing import Any

import yaml
from dotenv import load_dotenv

from src.ingestion.wistia_client import WistiaClient, WistiaClientConfig

SENSITIVE_KEY_PARTS = (
    "token",
    "authorization",
    "api_key",
    "password",
    "secret",
    "email",
    "ip",
    "visitor_key",
    "visitor_id",
)

LOGGER = logging.getLogger(__name__)

def is_sensitive_key(key: str) -> bool:
    key_lower = key.lower()
    return any(part in key_lower for part in SENSITIVE_KEY_PARTS)


def describe_json_shape(value: Any, key: str = "", max_list_samples: int = 1) -> Any:
    if is_sensitive_key(key):
        return f"<redacted:{type(value).__name__}>"

    if isinstance(value, dict):
        return {
            item_key: describe_json_shape(item_value, item_key, max_list_samples)
            for item_key, item_value in sorted(value.items())
        }

    if isinstance(value, list):
        return {
            "_type": "list",
            "_length": len(value),
            "_sample": [
                describe_json_shape(item, key, max_list_samples)
                for item in value[:max_list_samples]
            ],
        }

    if isinstance(value, (datetime, date)):
        return "<datetime>"

    if value is None:
        return "<null>"

    return f"<{type(value).__name__}>"


def load_pipeline_config(config_path: str = "config/pipeline.yml") -> dict:
    with Path(config_path).open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


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

    client = WistiaClient(
        WistiaClientConfig(
            api_token=api_token,
            base_url=wistia_config["base_url"],
            api_version=wistia_config.get("api_version", "2026-03"),
            timeout_seconds=ingestion_config.get("request_timeout_seconds", 30),
            max_retries=ingestion_config.get("max_retries", 5),
        )
    )

    for media in wistia_config["media"]:
        media_id = media["media_id"]
        channel = media["channel"]

        for endpoint_name in ("media_stats", "media_engagement"):
            endpoint = wistia_config["endpoints"][endpoint_name].format(media_id=media_id)
            payload = client.get_json(endpoint)

            shape = describe_json_shape(payload)

            LOGGER.info(
                "%s %s sanitized response shape:\n%s",
                channel,
                endpoint_name,
                json.dumps(shape, indent=2),
            )

            if isinstance(payload, dict):
                LOGGER.info("%s %s returned keys=%s", channel, endpoint_name, sorted(payload.keys()))
            else:
                LOGGER.info("%s %s returned list_length=%s", channel, endpoint_name, len(payload))


if __name__ == "__main__":
    main()
