"""Polite HTTP client: descriptive User-Agent, per-host pacing, retries with backoff.

* MusicBrainz asks for at most one request per second and a meaningful User-Agent.
* ListenBrainz publishes ``X-RateLimit-Remaining`` / ``X-RateLimit-Reset-In`` headers; when the
  budget is exhausted we sleep until the window resets.
* 429 and 5xx responses are retried with exponential backoff (honouring ``Retry-After``).
"""

from __future__ import annotations

import contextlib
import logging
import time
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

import requests

log = logging.getLogger(__name__)

RETRY_STATUSES = {429, 500, 502, 503, 504}


class HttpError(RuntimeError):
    def __init__(self, url: str, status: int | None, detail: str):
        super().__init__(f"GET {url} failed ({status}): {detail}")
        self.url = url
        self.status = status
        self.detail = detail


class HttpClient:
    def __init__(
        self,
        user_agent: str,
        timeout_s: float = 30.0,
        retries: int = 4,
        min_interval_s: Mapping[str, float] | None = None,
        session: requests.Session | None = None,
        sleep=time.sleep,
    ):
        self.session = session or requests.Session()
        self.session.headers["User-Agent"] = user_agent
        self.timeout_s = timeout_s
        self.retries = retries
        self.min_interval_s = dict(min_interval_s or {})
        self._last_call: dict[str, float] = {}
        self._sleep = sleep
        self.requests_made = 0

    def _pace(self, host: str) -> None:
        interval = self.min_interval_s.get(host, 0.0)
        last = self._last_call.get(host)
        if interval and last is not None:
            wait = interval - (time.monotonic() - last)
            if wait > 0:
                self._sleep(wait)
        self._last_call[host] = time.monotonic()

    def get(
        self,
        url: str,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        allow_404: bool = False,
        max_bytes: int | None = None,
    ) -> requests.Response | None:
        host = urlsplit(url).netloc
        attempt = 0
        while True:
            self._pace(host)
            try:
                resp = self.session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout_s,
                    stream=max_bytes is not None,
                )
                self.requests_made += 1
            except requests.RequestException as exc:
                if attempt >= self.retries:
                    raise HttpError(url, None, str(exc)) from exc
                self._backoff(attempt, None)
                attempt += 1
                continue
            self._respect_rate_limit(resp)
            if resp.status_code == 404 and allow_404:
                return None
            if resp.status_code in RETRY_STATUSES and attempt < self.retries:
                self._backoff(attempt, resp.headers.get("Retry-After"))
                attempt += 1
                continue
            if resp.status_code >= 400:
                raise HttpError(url, resp.status_code, resp.text[:300])
            if max_bytes is not None:
                body = bytearray()
                for chunk in resp.iter_content(64 * 1024):
                    body.extend(chunk)
                    if len(body) > max_bytes:
                        resp.close()
                        raise HttpError(url, resp.status_code, f"body exceeds {max_bytes} bytes")
                resp._content = bytes(body)
            return resp

    def get_json(self, url: str, params: Mapping[str, Any] | None = None, **kw) -> Any:
        resp = self.get(url, params=params, **kw)
        if resp is None or resp.status_code == 204 or not resp.content:
            return None
        return resp.json()

    def _backoff(self, attempt: int, retry_after: str | None) -> None:
        delay = min(60.0, 2.0**attempt)
        if retry_after:
            with contextlib.suppress(ValueError):
                delay = max(delay, float(retry_after))
        log.warning("retrying after %.1fs (attempt %d)", delay, attempt + 1)
        self._sleep(delay)

    def _respect_rate_limit(self, resp: requests.Response) -> None:
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset_in = resp.headers.get("X-RateLimit-Reset-In")
        if remaining is None or reset_in is None:
            return
        try:
            if int(remaining) <= 1:
                self._sleep(float(reset_in) + 0.5)
        except ValueError:
            return
