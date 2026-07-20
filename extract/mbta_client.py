"""Thin client for the MBTA V3 API.

Its ONLY job is to talk HTTP to api-v3.mbta.com: auth, pagination, retries,
rate limits, timeouts. It knows nothing about Snowflake or dbt. That
separation of concerns means we can test it in isolation and reuse it anywhere.
"""
from __future__ import annotations

import logging
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from extract import config

logger = logging.getLogger(__name__)


class MBTAClient:
    def __init__(self, api_key: str | None = None) -> None:
        self.base_url = config.MBTA_BASE_URL
        self.api_key = api_key or config.MBTA_API_KEY
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        """A Session with connection pooling + automatic retry/backoff."""
        session = requests.Session()
        if self.api_key:
            session.headers.update({"x-api-key": self.api_key})
        session.headers.update({"accept": "application/vnd.api+json"})

        retry = Retry(
            total=config.MAX_RETRIES,
            backoff_factor=config.BACKOFF_FACTOR,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=True,
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        return session

    def _get(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}/{endpoint}"
        response = self.session.get(url, params=params, timeout=config.REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _flatten(record: dict[str, Any]) -> dict[str, Any]:
        """Unwrap one JSON:API record into a flat dict.

        JSON:API nests fields under 'attributes' and references under
        'relationships'. We hoist id, spread attributes to the top level,
        and pull each relationship's id into a <name>_id column.
        """
        flat: dict[str, Any] = {"id": record.get("id")}
        flat.update(record.get("attributes", {}))
        for rel_name, rel_body in (record.get("relationships") or {}).items():
            rel_data = (rel_body or {}).get("data")
            flat[f"{rel_name}_id"] = rel_data.get("id") if rel_data else None
        return flat

    def get_all(self, endpoint: str, filters: dict[str, str] | None = None) -> list[dict[str, Any]]:
        """Fetch EVERY page of an endpoint, returning flattened records."""
        params: dict[str, Any] = {"page[limit]": config.PAGE_LIMIT, "page[offset]": 0}
        if filters:
            params.update(filters)

        records: list[dict[str, Any]] = []
        while True:
            payload = self._get(endpoint, params)
            page = payload.get("data", [])
            records.extend(self._flatten(r) for r in page)
            logger.info("Fetched %d from /%s (offset=%s)", len(page), endpoint, params["page[offset]"])

            if not (payload.get("links") or {}).get("next"):
                break
            params["page[offset]"] += config.PAGE_LIMIT
        return records

    # --- convenience wrappers per endpoint ---
    def get_routes(self) -> list[dict[str, Any]]:
        return self.get_all("routes", {"filter[id]": ",".join(config.TARGET_ROUTES)})

    def get_stops(self) -> list[dict[str, Any]]:
        return self.get_all("stops", {"filter[route]": ",".join(config.TARGET_ROUTES)})

    def get_predictions(self) -> list[dict[str, Any]]:
        return self.get_all("predictions", {"filter[route]": ",".join(config.TARGET_ROUTES)})

    def get_schedules(self) -> list[dict[str, Any]]:
        return self.get_all("schedules", {"filter[route]": ",".join(config.TARGET_ROUTES)})
