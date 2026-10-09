from __future__ import annotations

import gzip
import json
from datetime import date

import pytest
import requests
import responses

from musicpods.bronze import Bronze, BronzeConflictError
from musicpods.config import ConfigError, Settings
from musicpods.http import HttpClient, HttpError


# ----------------------------------------------------------------------------- config
def test_defaults_are_safe():
    s = Settings.from_env({})
    assert s.aws_mode == "moto"
    assert s.iceberg_catalog == "sql"
    assert s.mb_min_interval_s >= 1.0
    assert s.lastfm_api_key is None
    assert s.dq_fail_on_error is True


def test_endpoint_switches_to_endpoint_mode():
    s = Settings.from_env({"MUSICPODS_AWS_ENDPOINT_URL": "http://localhost:4566"})
    assert s.aws_mode == "endpoint"
    assert s.lake_in_s3


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("MUSICPODS_LB_USERS", "many"),
        ("MUSICPODS_LB_USERS", "0"),
        ("MUSICPODS_DQ_FAIL_ON_ERROR", "maybe"),
        ("MUSICPODS_LOG_FORMAT", "xml"),
        ("MUSICPODS_MB_MIN_INTERVAL_SECONDS", "0.2"),
        ("MUSICPODS_AWS_MODE", "azure"),
    ],
)
def test_invalid_values_raise(name, value):
    with pytest.raises(ConfigError, match=name):
        Settings.from_env({name: value})


def test_secrets_are_not_in_repr():
    s = Settings.from_env({"LASTFM_API_KEY": "abc123", "SPOTIFY_CLIENT_SECRET": "xyz"})
    assert "abc123" not in repr(s)
    assert "xyz" not in repr(s)


# ----------------------------------------------------------------------------- http
def _client(sleeps):
    return HttpClient("test-agent/1.0", retries=2, sleep=sleeps.append)


@responses.activate
def test_retries_5xx_then_succeeds():
    responses.get("https://example.org/x", status=503)
    responses.get("https://example.org/x", json={"ok": True})
    sleeps: list[float] = []
    assert _client(sleeps).get_json("https://example.org/x") == {"ok": True}
    assert sleeps == [1.0]
    assert responses.calls[0].request.headers["User-Agent"] == "test-agent/1.0"


@responses.activate
def test_honours_retry_after():
    responses.get("https://example.org/x", status=429, headers={"Retry-After": "7"})
    responses.get("https://example.org/x", json={})
    sleeps: list[float] = []
    _client(sleeps).get_json("https://example.org/x")
    assert sleeps == [7.0]


@responses.activate
def test_gives_up_after_retries():
    for _ in range(3):
        responses.get("https://example.org/x", status=500)
    with pytest.raises(HttpError) as err:
        _client([]).get("https://example.org/x")
    assert err.value.status == 500


@responses.activate
def test_404_allowed_returns_none_and_4xx_raises():
    responses.get("https://example.org/missing", status=404)
    responses.get("https://example.org/bad", status=400)
    c = _client([])
    assert c.get("https://example.org/missing", allow_404=True) is None
    with pytest.raises(HttpError):
        c.get("https://example.org/bad")


@responses.activate
def test_connection_errors_are_retried():
    responses.get("https://example.org/x", body=requests.ConnectionError("dns"))
    responses.get("https://example.org/x", json={"n": 1})
    assert _client([]).get_json("https://example.org/x") == {"n": 1}


@responses.activate
def test_body_size_cap():
    responses.get("https://example.org/big", body=b"x" * 5000)
    with pytest.raises(HttpError, match="exceeds"):
        _client([]).get("https://example.org/big", max_bytes=1000)


@responses.activate
def test_listenbrainz_rate_limit_headers_pause():
    responses.get(
        "https://api.example/x",
        json={},
        headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset-In": "3"},
    )
    sleeps: list[float] = []
    _client(sleeps).get_json("https://api.example/x")
    assert sleeps == [3.5]


def test_per_host_pacing():
    sleeps: list[float] = []
    c = HttpClient("ua", min_interval_s={"musicbrainz.org": 1.0}, sleep=sleeps.append)
    c._pace("musicbrainz.org")
    c._pace("musicbrainz.org")
    c._pace("other.org")
    assert len(sleeps) == 1
    assert 0 < sleeps[0] <= 1.0


# ----------------------------------------------------------------------------- bronze
def test_bronze_write_is_idempotent_and_immutable(tmp_path, caplog):
    b = Bronze(tmp_path)
    d = date(2026, 10, 9)
    p1 = b.write("src", "ds", d, "a", {"x": 1})
    p2 = b.write("src", "ds", d, "a", {"x": 1})
    assert p1 == p2
    assert len((tmp_path / "_manifest.jsonl").read_text().splitlines()) == 1
    # A different payload under the same name keeps the first snapshot of the day...
    with caplog.at_level("WARNING", logger="musicpods.bronze"):
        b.write("src", "ds", d, "a", {"x": 2})
    assert b.read(p1) == {"x": 1}
    assert "different content" in caplog.text  # never silently
    # ...or is refused when asked to.
    with pytest.raises(BronzeConflictError):
        b.write("src", "ds", d, "a", {"x": 2}, on_conflict="error")
    raw = p1.read_bytes()
    assert raw[:2] == b"\x1f\x8b"
    assert json.loads(gzip.decompress(raw)) == {"x": 1}


def test_bronze_latest_and_import_tree(tmp_path):
    src = tmp_path / "fixtures"
    for dt, n in (("2026-10-01", 1), ("2026-10-02", 2)):
        path = src / "apple" / "charts" / f"dt={dt}" / "us.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({"n": n}))
    (src / "README.txt").write_text("ignored")
    b = Bronze(tmp_path / "bronze")
    assert b.import_tree(src) == 2
    assert b.import_tree(src) == 2  # same content again: no-op writes
    latest = b.latest("apple", "charts")
    assert [f.dt for f in latest] == [date(2026, 10, 2)]
    assert b.read(latest[0]) == {"n": 2}
    assert b.import_tree(src, sources=("rss",)) == 0
