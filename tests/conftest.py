"""Shared fixtures. Tests never touch the network or a real AWS account: HTTP is mocked with
``responses`` and AWS runs on moto in-process (or on an emulator endpoint for -m integration)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from musicpods.config import Settings

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "bronze"
FIXTURE_DATE = "2026-10-09"


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch):
    """No developer .env or AWS profile may leak into a test."""
    for key in list(os.environ):
        if key.startswith(("MUSICPODS_", "LASTFM_", "PODCASTINDEX_", "SPOTIFY_")):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
    monkeypatch.delenv("AWS_PROFILE", raising=False)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(data_dir=tmp_path / "data", pseudonym_salt="test-salt")
