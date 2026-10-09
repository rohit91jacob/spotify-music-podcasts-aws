"""Environment-driven settings. Every knob is a ``MUSICPODS_*`` variable; see .env.example."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path


class ConfigError(ValueError):
    """Raised when an environment variable holds an invalid value (exit code 2)."""


_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}

DEFAULT_COUNTRIES = ("us", "gb", "ca", "au", "in", "ie", "de", "fr", "br", "jp")


class _Env:
    def __init__(self, env: Mapping[str, str]):
        self._env = env

    def text(self, name: str, default: str) -> str:
        value = self._env.get(name, "").strip()
        return value or default

    def optional(self, name: str) -> str | None:
        value = self._env.get(name, "").strip()
        return value or None

    def flag(self, name: str, default: bool) -> bool:
        raw = self._env.get(name, "").strip().lower()
        if not raw:
            return default
        if raw in _TRUE:
            return True
        if raw in _FALSE:
            return False
        raise ConfigError(f"{name} must be a boolean (true/false), got {raw!r}")

    def integer(self, name: str, default: int, minimum: int | None = None) -> int:
        raw = self._env.get(name, "").strip()
        try:
            value = int(raw) if raw else default
        except ValueError as exc:
            raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc
        if minimum is not None and value < minimum:
            raise ConfigError(f"{name} must be >= {minimum}, got {value}")
        return value

    def number(self, name: str, default: float, minimum: float | None = None) -> float:
        raw = self._env.get(name, "").strip()
        try:
            value = float(raw) if raw else default
        except ValueError as exc:
            raise ConfigError(f"{name} must be a number, got {raw!r}") from exc
        if minimum is not None and value < minimum:
            raise ConfigError(f"{name} must be >= {minimum}, got {value}")
        return value

    def choice(self, name: str, default: str, choices: set[str]) -> str:
        value = self.text(name, default)
        if value not in choices:
            raise ConfigError(f"{name} must be one of {sorted(choices)}, got {value!r}")
        return value

    def csv(self, name: str, default: tuple[str, ...]) -> tuple[str, ...]:
        raw = self._env.get(name, "").strip()
        if not raw:
            return default
        return tuple(p.strip().lower() for p in raw.split(",") if p.strip())


@dataclass(frozen=True)
class Settings:
    data_dir: Path = Path("data")

    # HTTP
    user_agent: str = (
        "spotify-music-podcasts-aws/0.1 "
        "(+https://github.com/rohit91jacob/spotify-music-podcasts-aws)"
    )
    http_timeout_s: float = 60.0
    http_retries: int = 4

    # ListenBrainz sample
    lb_seed_artists: int = 30
    lb_users: int = 40
    lb_window_days: int = 28
    lb_max_listens_per_user: int = 4000
    lb_max_daily_listens: int = 480
    lb_fresh_release_days: int = 30
    pseudonym_salt: str | None = field(default=None, repr=False)

    # MusicBrainz
    mb_min_interval_s: float = 1.1
    mb_max_artists: int = 600
    mb_max_recordings: int = 3000
    mb_max_release_groups: int = 2000
    mb_fresh_release_groups: int = 200
    mb_refresh_days: int = 30

    # Apple Podcasts / RSS
    chart_countries: tuple[str, ...] = DEFAULT_COUNTRIES
    chart_size: int = 100
    rss_top_n_per_country: int = 10
    rss_max_feeds: int = 60
    rss_max_bytes: int = 25_000_000

    # Optional key-gated sources (enabled only when set)
    lastfm_api_key: str | None = field(default=None, repr=False)
    podcastindex_api_key: str | None = field(default=None, repr=False)
    podcastindex_api_secret: str | None = field(default=None, repr=False)
    spotify_client_id: str | None = field(default=None, repr=False)
    spotify_client_secret: str | None = field(default=None, repr=False)

    # Emulated AWS
    aws_region: str = "eu-west-1"
    aws_endpoint_url: str | None = None  # LocalStack in CI; None = moto in-process
    # moto: in-process emulator (default); endpoint: MUSICPODS_AWS_ENDPOINT_URL (LocalStack);
    # aws: the ambient AWS credentials. Only the Glue entrypoint selects "aws".
    aws_mode: str = "moto"
    # sql: SQLite-backed Iceberg catalog (local, CI); glue: AWS Glue Data Catalog (deployed).
    iceberg_catalog: str = "sql"
    glue_raw_database: str = "musicpods_raw"
    lake_bucket: str = "musicpods-lake"
    firehose_stream: str = "musicpods-listen-events"
    firehose_prefix: str = "firehose/listen-events/"
    firehose_error_prefix: str = "firehose/errors/"
    replay_speedup: float = 0.0
    replay_batch_size: int = 500

    # Batch / quality
    session_gap_minutes: int = 30
    dq_fail_on_error: bool = True
    snapshot_retention_days: int = 7

    # Observability
    log_level: str = "INFO"
    log_format: str = "text"

    # ------------------------------------------------------------------ derived paths
    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def s3_mirror_dir(self) -> Path:
        """Local mirror of the emulated S3 lake bucket (moto mode)."""
        return self.data_dir / "s3" / self.lake_bucket

    @property
    def iceberg_dir(self) -> Path:
        return self.data_dir / "iceberg"

    @property
    def catalog_db(self) -> Path:
        return self.data_dir / "iceberg" / "catalog.db"

    @property
    def warehouse_path(self) -> Path:
        return self.data_dir / "warehouse" / "musicpods.duckdb"

    @property
    def ops_dir(self) -> Path:
        return self.data_dir / "ops"

    @property
    def uses_localstack(self) -> bool:
        return self.aws_mode == "endpoint"

    @property
    def lake_in_s3(self) -> bool:
        """True when the lake bucket is a real S3 API (LocalStack or AWS), not the moto mirror."""
        return self.aws_mode in {"endpoint", "aws"}

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        e = _Env(os.environ if env is None else env)
        d = cls()
        return cls(
            data_dir=Path(e.text("MUSICPODS_DATA_DIR", str(d.data_dir))),
            user_agent=e.text("MUSICPODS_HTTP_USER_AGENT", d.user_agent),
            http_timeout_s=e.number("MUSICPODS_HTTP_TIMEOUT_SECONDS", d.http_timeout_s, 1),
            http_retries=e.integer("MUSICPODS_HTTP_RETRIES", d.http_retries, 0),
            lb_seed_artists=e.integer("MUSICPODS_LB_SEED_ARTISTS", d.lb_seed_artists, 1),
            lb_users=e.integer("MUSICPODS_LB_USERS", d.lb_users, 1),
            lb_window_days=e.integer("MUSICPODS_LB_WINDOW_DAYS", d.lb_window_days, 1),
            lb_max_listens_per_user=e.integer(
                "MUSICPODS_LB_MAX_LISTENS_PER_USER", d.lb_max_listens_per_user, 100
            ),
            lb_max_daily_listens=e.integer(
                "MUSICPODS_LB_MAX_DAILY_LISTENS", d.lb_max_daily_listens, 1
            ),
            lb_fresh_release_days=e.integer(
                "MUSICPODS_LB_FRESH_RELEASE_DAYS", d.lb_fresh_release_days, 1
            ),
            pseudonym_salt=e.optional("MUSICPODS_PSEUDONYM_SALT"),
            mb_min_interval_s=e.number("MUSICPODS_MB_MIN_INTERVAL_SECONDS", d.mb_min_interval_s, 1),
            mb_max_artists=e.integer("MUSICPODS_MB_MAX_ARTISTS", d.mb_max_artists, 0),
            mb_max_recordings=e.integer("MUSICPODS_MB_MAX_RECORDINGS", d.mb_max_recordings, 0),
            mb_max_release_groups=e.integer(
                "MUSICPODS_MB_MAX_RELEASE_GROUPS", d.mb_max_release_groups, 0
            ),
            mb_fresh_release_groups=e.integer(
                "MUSICPODS_MB_FRESH_RELEASE_GROUPS", d.mb_fresh_release_groups, 0
            ),
            mb_refresh_days=e.integer("MUSICPODS_MB_REFRESH_DAYS", d.mb_refresh_days, 0),
            chart_countries=e.csv("MUSICPODS_CHART_COUNTRIES", d.chart_countries),
            chart_size=e.integer("MUSICPODS_CHART_SIZE", d.chart_size, 10),
            rss_top_n_per_country=e.integer(
                "MUSICPODS_RSS_TOP_N_PER_COUNTRY", d.rss_top_n_per_country, 0
            ),
            rss_max_feeds=e.integer("MUSICPODS_RSS_MAX_FEEDS", d.rss_max_feeds, 0),
            rss_max_bytes=e.integer("MUSICPODS_RSS_MAX_BYTES", d.rss_max_bytes, 100_000),
            lastfm_api_key=e.optional("LASTFM_API_KEY"),
            podcastindex_api_key=e.optional("PODCASTINDEX_API_KEY"),
            podcastindex_api_secret=e.optional("PODCASTINDEX_API_SECRET"),
            spotify_client_id=e.optional("SPOTIFY_CLIENT_ID"),
            spotify_client_secret=e.optional("SPOTIFY_CLIENT_SECRET"),
            aws_region=e.text("MUSICPODS_AWS_REGION", d.aws_region),
            aws_endpoint_url=e.optional("MUSICPODS_AWS_ENDPOINT_URL"),
            aws_mode=e.choice(
                "MUSICPODS_AWS_MODE",
                "endpoint" if e.optional("MUSICPODS_AWS_ENDPOINT_URL") else "moto",
                {"moto", "endpoint", "aws"},
            ),
            iceberg_catalog=e.choice(
                "MUSICPODS_ICEBERG_CATALOG", d.iceberg_catalog, {"sql", "glue"}
            ),
            glue_raw_database=e.text("MUSICPODS_GLUE_RAW_DATABASE", d.glue_raw_database),
            lake_bucket=e.text("MUSICPODS_LAKE_BUCKET", d.lake_bucket),
            firehose_stream=e.text("MUSICPODS_FIREHOSE_STREAM", d.firehose_stream),
            replay_speedup=e.number("MUSICPODS_REPLAY_SPEEDUP", d.replay_speedup, 0),
            replay_batch_size=e.integer("MUSICPODS_REPLAY_BATCH_SIZE", d.replay_batch_size, 1),
            session_gap_minutes=e.integer(
                "MUSICPODS_SESSION_GAP_MINUTES", d.session_gap_minutes, 1
            ),
            dq_fail_on_error=e.flag("MUSICPODS_DQ_FAIL_ON_ERROR", d.dq_fail_on_error),
            snapshot_retention_days=e.integer(
                "MUSICPODS_SNAPSHOT_RETENTION_DAYS", d.snapshot_retention_days, 0
            ),
            log_level=e.choice(
                "MUSICPODS_LOG_LEVEL", d.log_level, {"DEBUG", "INFO", "WARNING", "ERROR"}
            ),
            log_format=e.choice("MUSICPODS_LOG_FORMAT", d.log_format, {"text", "json"}),
        )

    def with_data_dir(self, data_dir: Path) -> Settings:
        from dataclasses import replace

        return replace(self, data_dir=data_dir)
