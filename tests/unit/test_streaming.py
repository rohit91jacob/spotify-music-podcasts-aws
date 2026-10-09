from __future__ import annotations

import json
from datetime import date

import pytest

from musicpods import lakehouse
from musicpods.aws import LocalStore, aws_clients
from musicpods.bronze import Bronze
from musicpods.quality.checks import DataQualityError
from musicpods.streaming import events
from musicpods.streaming.replay import _batches, put_batch, replay
from musicpods.streaming.sink import run_sink

TS = 1_791_500_000


def _listen(i: int, user: str = "u_1", **info) -> dict:
    return {
        "user_key": user,
        "listened_at": TS + 60 * i,
        "inserted_at": TS + 60 * i + 3,
        "recording_msid": f"msid-{i}",
        "track_metadata": {
            "artist_name": "Artist",
            "track_name": f"Track {i}",
            "additional_info": {"duration_ms": 200_000, **info},
            "mbid_mapping": {
                "recording_mbid": "0f9f2c76-6cb2-4d6a-a7a2-2a4e5b6c7d8e",
                "artist_mbids": ["a1b2c3d4-0000-4000-8000-000000000001"],
            },
        },
    }


# ----------------------------------------------------------------------------- events
def test_event_id_is_deterministic():
    e1 = events.to_event(_listen(1))
    e2 = events.to_event(_listen(1))
    assert e1["event_id"] == e2["event_id"]
    assert e1["event_id"] != events.to_event(_listen(2))["event_id"]
    assert events.validate(e1) is None


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda e: e.pop("user_key"), "missing:user_key"),
        (lambda e: e.update(schema_version=2), "unsupported_schema_version:2"),
        (lambda e: e.update(listened_at=-5), "bad_listened_at"),
        (lambda e: e.update(listened_at="2026"), "bad_listened_at"),
        (lambda e: e.update(track_metadata={"x": 1}), "empty_track_metadata"),
        (lambda e: e.update(event_id="forged"), "event_id_mismatch"),
    ],
)
def test_validate_reasons(mutate, reason):
    e = events.to_event(_listen(1))
    mutate(e)
    assert events.validate(e) == reason
    assert events.validate("not json") == "not_an_object"


def test_flatten_durations_and_playback():
    row = events.flatten(events.to_event(_listen(1, ms_played=0)))
    assert row["duration_ms"] == 200_000
    assert row["played_ms"] == 0  # an immediate skip is a real observation, not "unknown"
    assert row["primary_artist_mbid"] == "a1b2c3d4-0000-4000-8000-000000000001"
    seconds = _listen(2)
    seconds["track_metadata"]["additional_info"] = {"duration": 180}
    assert events.flatten(events.to_event(seconds))["duration_ms"] == 180_000
    assert events.flatten(events.to_event(seconds))["played_ms"] is None


def test_encode_is_json_lines():
    body = events.encode(events.to_event(_listen(1)))
    assert body.endswith(b"\n")
    assert json.loads(body)["event_type"] == "listen"


# ----------------------------------------------------------------------------- replay
def test_batches_respect_firehose_limits():
    records = [b"x" * 10] * 1201
    sizes = [len(b) for b in _batches(records)]
    assert sizes == [500, 500, 201]
    big = [b"y" * (3 * 1024 * 1024)] * 3
    assert [len(b) for b in _batches(big)] == [1, 1, 1]


class FlakyFirehose:
    """Fails the first record of the first call, as Firehose does under throttling."""

    def __init__(self):
        self.calls: list[int] = []

    def put_record_batch(self, DeliveryStreamName, Records):
        self.calls.append(len(Records))
        if len(self.calls) == 1:
            responses = [{"ErrorCode": "ServiceUnavailableException"}] + [
                {"RecordId": "ok"} for _ in Records[1:]
            ]
            return {"FailedPutCount": 1, "RequestResponses": responses}
        return {"FailedPutCount": 0, "RequestResponses": [{"RecordId": "ok"} for _ in Records]}


def test_put_batch_resends_only_failed_records():
    fh = FlakyFirehose()
    assert put_batch(fh, "s", [b"a", b"b", b"c"], sleep=lambda _s: None) == 1
    assert fh.calls == [3, 1]


def _land(settings, n: int = 30, bad: bool = False) -> Bronze:
    bronze = Bronze(settings.bronze_dir)
    listens = [_listen(i) for i in range(n)]
    bronze.write("listenbrainz", "listens", date(2026, 10, 9), "u_1-page", {"listens": listens})
    return bronze


def test_replay_sink_roundtrip_is_exactly_once(settings):
    bronze = _land(settings)
    with aws_clients(settings) as aws:
        first = replay(aws, settings, bronze, duplicate_every=5)
    assert first.events == 36  # 30 events + every 5th sent twice
    assert first.mirrored_objects >= 1
    store = LocalStore(settings.s3_mirror_dir)
    cat = lakehouse.catalog(settings)
    sink1 = run_sink(settings, store, cat)
    assert sink1.records == 36
    assert sink1.duplicates_in_batch == 6
    assert sink1.appended == 30
    assert sink1.table_rows == 30

    # Nothing new: replay sends nothing, the sink reads nothing.
    with aws_clients(settings) as aws:
        assert replay(aws, settings, bronze).events == 0
    assert run_sink(settings, store, cat).objects == 0

    # A full re-replay (at-least-once redelivery) leaves the table unchanged.
    with aws_clients(settings) as aws:
        again = replay(aws, settings, bronze, full=True)
    assert again.events == 30
    sink3 = run_sink(settings, store, cat)
    assert sink3.already_in_table == 30
    assert sink3.appended == 0
    assert sink3.table_rows == 30
    snapshots = list(cat.load_table("raw.listen_events").snapshots())
    assert len(snapshots) == 1  # no-op batches do not create snapshots


def test_sink_dead_letters_bad_records(settings):
    store = LocalStore(settings.s3_mirror_dir)
    good = events.encode(events.to_event(_listen(1)))
    forged = events.to_event(_listen(2))
    forged["event_id"] = "x"
    body = good + events.encode(forged) + b"{not json\n"
    store.put(settings.firehose_prefix + "2026/10/09/00/part-1", body)
    result = run_sink(settings, store)
    assert (result.records, result.dead_letters, result.appended) == (3, 2, 1)
    dlq = (settings.ops_dir / "dlq" / "listen_events.jsonl").read_text().splitlines()
    assert sorted(json.loads(x)["reason"] for x in dlq) == ["event_id_mismatch", "unparseable_json"]


def test_sink_dq_gate_blocks_commit(settings):
    store = LocalStore(settings.s3_mirror_dir)
    bad = _listen(1)
    bad["track_metadata"]["mbid_mapping"]["recording_mbid"] = "not-a-uuid"
    store.put(settings.firehose_prefix + "k1", events.encode(events.to_event(bad)))
    cat = lakehouse.catalog(settings)
    with pytest.raises(DataQualityError, match="recording_mbid_is_uuid"):
        run_sink(settings, store, cat)
    # Nothing committed and the object is not marked as done: the next run re-reads it.
    assert cat.load_table("raw.listen_events").current_snapshot() is None
    assert not (settings.ops_dir / "sink_state.json").exists()
