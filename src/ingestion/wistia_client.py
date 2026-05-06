from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Literal

from requests.auth import HTTPBasicAuth


from urllib.parse import urljoin

import requests

LOGGER = logging.getLogger(__name__)
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class WistiaAPIError(Exception):
    pass


class WistiaAuthError(WistiaAPIError):
    pass


class WistiaNotFoundError(WistiaAPIError):
    pass

AuthScheme = Literal["basic", "bearer"]
BasicAuthTokenPosition = Literal["username", "password"]

@dataclass(frozen=True)
class WistiaClientConfig:
    api_token: str
    base_url: str
    api_version: str = "2026-03"
    auth_scheme: AuthScheme = "basic"
    basic_auth_username: str = "api"
    basic_auth_token_position: BasicAuthTokenPosition = "password"
    timeout_seconds: int = 30
    max_retries: int = 5


class WistiaClient:
    def __init__(self, config: WistiaClientConfig) -> None:
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "X-Wistia-API-Version": config.api_version,
            }
        )

        if config.auth_scheme == "basic":
            if config.basic_auth_token_position == "username":
                self.session.auth = HTTPBasicAuth(config.api_token, "")
            else:
                self.session.auth = HTTPBasicAuth(config.basic_auth_username, config.api_token)
        elif config.auth_scheme == "bearer":
            self.session.headers["Authorization"] = f"Bearer {config.api_token}"
        else:
            raise ValueError(f"Unsupported Wistia auth scheme: {config.auth_scheme}")


    def get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any] | list[Any]:
        url = urljoin(f"{self.config.base_url.rstrip('/')}/", path.lstrip("/"))

        for attempt in range(1, self.config.max_retries + 1):
            try:
                LOGGER.info("Calling Wistia API path=%s attempt=%s", path, attempt)
                response = self.session.get(url, params=params, timeout=self.config.timeout_seconds)
            except requests.RequestException as exc:
                if attempt == self.config.max_retries:
                    raise WistiaAPIError(f"Wistia request failed after retries: {exc}") from exc
                self._sleep_before_retry(attempt)
                continue

            if response.status_code == 200:
                return response.json()

            if response.status_code == 401:
                raise WistiaAuthError("Unauthorized: invalid or missing Wistia API token.")

            if response.status_code == 403:
                raise WistiaAuthError("Forbidden: token lacks required Wistia permissions.")

            if response.status_code == 404:
                raise WistiaNotFoundError(f"Not found: {path}")

            if response.status_code in RETRYABLE_STATUS_CODES and attempt < self.config.max_retries:
                self._sleep_before_retry(attempt, response)
                continue

            raise WistiaAPIError(
                f"Wistia API error status={response.status_code} path={path} body={response.text[:500]}"
            )

        raise WistiaAPIError(f"Wistia request exhausted retries for path={path}")

    def _sleep_before_retry(self, attempt: int, response: requests.Response | None = None) -> None:
        retry_after = response.headers.get("Retry-After") if response is not None else None

        if retry_after:
            delay_seconds = float(retry_after)
        else:
            delay_seconds = min(2**attempt, 60)

        LOGGER.warning("Retrying Wistia request after %.1f seconds", delay_seconds)
        time.sleep(delay_seconds)
