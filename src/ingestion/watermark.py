from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from botocore.exceptions import ClientError

from src.common.paths import join_path, read_text, resolve_layer_path, write_text

LOGGER = logging.getLogger(__name__)

# Makes object immutable after creation
@dataclass(frozen=True)
class IncrementalWindow:
    """Represents the date range selected for one ingestion run.

    This object is returned by resolve_incremental_window() after the ingestion
    code decides whether to use an explicit user-provided date range or a
    persisted watermark. frozen=True makes the object immutable so the selected
    window cannot be accidentally changed later in the pipeline.
    """

    start_date: str
    end_date: str
    watermark_path: str
    previous_watermark: str | None
    next_watermark: str
    explicit_window: bool
    should_update_watermark: bool


def utc_now() -> datetime:
    """Return the current timestamp in UTC.
    """
    return datetime.now(timezone.utc)


def parse_watermark_datetime(value: str) -> datetime:
    """Parse a stored watermark timestamp into a timezone-aware UTC datetime.

    Watermarks may be stored with a trailing Z or with a +00:00 offset. This
    helper normalizes both formats to a Python datetime in UTC so watermark
    comparisons are reliable.
    """

    cleaned_value = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(cleaned_value)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def read_watermark_state(path: str) -> dict[str, Any]:
    """Read the persisted watermark JSON from local disk or S3.

    If the state file does not exist yet, this returns an empty dictionary. That
    lets the first pipeline run bootstrap itself without requiring a manually
    created watermark file.
    """

    try:
        return json.loads(read_text(path))
    except FileNotFoundError:
        return {}
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in {"NoSuchKey", "404", "NotFound"}:
            return {}
        raise


def resolve_watermark_path(
    *,
    config: dict[str, Any],
    storage_mode: str,
    config_path: str,
    explicit_watermark_path: str | None = None,
) -> str:
    """Determine where the watermark state file should live.

    If the caller passes an explicit path, that path wins. Otherwise the path is
    built from the configured state layer, so local runs write under
    data/state/wistia and AWS runs write under s3://<bucket>/state/wistia.
    """

    if explicit_watermark_path:
        return explicit_watermark_path

    ingestion_config = config.get("ingestion", {})
    state_dir = resolve_layer_path(config, "state", storage_mode, config_path)
    state_file = ingestion_config.get("watermark_state_file", "wistia_ingestion_watermark.json")

    return join_path(state_dir, state_file)


def resolve_incremental_window(
    *,
    config: dict[str, Any],
    storage_mode: str,
    config_path: str,
    explicit_start_date: str | None,
    explicit_end_date: str | None,
    explicit_watermark_path: str | None = None,
    disable_watermark: bool = False,
    update_watermark_on_explicit_window: bool = False,
) -> IncrementalWindow:
    """Choose the Wistia date window for the current ingestion run.

    If the user provides --start-date and --end-date, the function uses that
    explicit window. By default, explicit backfills do not update the persisted
    watermark.

    If no explicit dates are provided, the function reads the previous
    watermark. When a previous watermark exists, ingestion starts from the
    watermark date minus the configured lookback window. The lookback helps
    capture late-arriving events. When no watermark exists, ingestion starts from
    the configured initial_start_date or defaults to yesterday.

    The returned IncrementalWindow also tells the caller whether the watermark
    should be updated after the ingestion run succeeds.
    """

    ingestion_config = config.get("ingestion", {})
    now = utc_now()
    watermark_path = resolve_watermark_path(
        config=config,
        storage_mode=storage_mode,
        config_path=config_path,
        explicit_watermark_path=explicit_watermark_path,
    )

    if bool(explicit_start_date) != bool(explicit_end_date):
        raise ValueError("Provide both --start-date and --end-date, or neither.")

    if explicit_start_date and explicit_end_date:
        return IncrementalWindow(
            start_date=explicit_start_date,
            end_date=explicit_end_date,
            watermark_path=watermark_path,
            previous_watermark=None,
            next_watermark=now.isoformat(),
            explicit_window=True,
            should_update_watermark=update_watermark_on_explicit_window and not disable_watermark,
        )

    if disable_watermark:
        yesterday = date.today() - timedelta(days=1)
        today = date.today()

        return IncrementalWindow(
            start_date=yesterday.isoformat(),
            end_date=today.isoformat(),
            watermark_path=watermark_path,
            previous_watermark=None,
            next_watermark=now.isoformat(),
            explicit_window=False,
            should_update_watermark=False,
        )

    state = read_watermark_state(watermark_path)
    previous_watermark = state.get("watermark_value")
    lookback_days = int(ingestion_config.get("watermark_lookback_days", 1))

    if previous_watermark:
        previous_dt = parse_watermark_datetime(previous_watermark)
        start_dt = previous_dt - timedelta(days=lookback_days)
        start_date = start_dt.date().isoformat()
    else:
        configured_initial_start = ingestion_config.get("initial_start_date")
        if configured_initial_start:
            start_date = configured_initial_start
        else:
            start_date = (date.today() - timedelta(days=1)).isoformat()

    return IncrementalWindow(
        start_date=start_date,
        end_date=date.today().isoformat(),
        watermark_path=watermark_path,
        previous_watermark=previous_watermark,
        next_watermark=now.isoformat(),
        explicit_window=False,
        should_update_watermark=True,
    )


def write_success_watermark(
    *,
    path: str,
    config: dict[str, Any],
    run_id: str,
    window: IncrementalWindow,
    storage_mode: str,
    visitor_count: int,
    media_count: int,
) -> None:
    """Persist the new watermark after a successful ingestion run.

    This function writes a JSON state file with the new watermark timestamp and
    summary metadata about the completed run. It only advances the watermark if
    the new timestamp is newer than the existing one, which prevents accidental
    rollback of incremental state.
    """

    existing_state = read_watermark_state(path)
    existing_watermark = existing_state.get("watermark_value")

    if existing_watermark:
        existing_dt = parse_watermark_datetime(existing_watermark)
        next_dt = parse_watermark_datetime(window.next_watermark)
        if next_dt <= existing_dt:
            LOGGER.warning(
                "Skipping watermark update because next watermark is not newer. existing=%s next=%s",
                existing_watermark,
                window.next_watermark,
            )
            return

    completed_at = utc_now().isoformat()
    ingestion_config = config.get("ingestion", {})

    state = {
        "source": "wistia",
        "watermark_field": ingestion_config.get("incremental_watermark_field", "updated_at"),
        "watermark_value": window.next_watermark,
        "updated_at": completed_at,
        "last_successful_run": {
            "pipeline_run_id": run_id,
            "start_date": window.start_date,
            "end_date": window.end_date,
            "completed_at": completed_at,
            "storage_mode": storage_mode,
            "visitor_count": visitor_count,
            "media_count": media_count,
        },
    }

    write_text(path, json.dumps(state, indent=2, sort_keys=True))
    LOGGER.info("Updated Wistia ingestion watermark path=%s value=%s", path, window.next_watermark)
