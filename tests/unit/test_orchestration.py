from __future__ import annotations

import json
import re
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from musicpods import cli
from musicpods.glue import entrypoint
from musicpods.orchestration import runner
from musicpods.orchestration.steps import STEPS
from musicpods.quality.checks import DataQualityError

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures" / "bronze"


def test_definition_is_valid_and_complete():
    defn = runner.render(runner.definition_text())
    assert runner.validate(defn) == []
    text = json.dumps(defn)
    jobs = set(re.findall(r'"JobName": "musicpods-local-([a-z-]+)"', text))
    assert jobs == set(STEPS), "every step is in the state machine and vice versa"


def test_terraform_creates_one_glue_job_per_step():
    main_tf = (ROOT / "infra" / "main.tf").read_text()
    block = main_tf[
        main_tf.index("glue_steps = [") : main_tf.index("]", main_tf.index("glue_steps"))
    ]
    assert set(re.findall(r'"([a-z-]+)"', block)) == set(STEPS)


def test_validate_reports_broken_definitions():
    bad = {
        "StartAt": "A",
        "States": {
            "A": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {"JobName": "musicpods-local-nope"},
                "Next": "Missing",
            },
            "B": {"Type": "Map"},
        },
    }
    problems = runner.validate(bad)
    assert any("unknown target 'Missing'" in p for p in problems)
    assert any("unsupported Type 'Map'" in p for p in problems)
    assert any("no local step" in p for p in problems)


def _machine(task_resource="arn:aws:states:::glue:startJobRun.sync"):
    return {
        "StartAt": "Work",
        "States": {
            "Work": {
                "Type": "Task",
                "Resource": task_resource,
                "Parameters": {"JobName": "musicpods-local-maintenance"},
                "Retry": [{"ErrorEquals": ["States.TaskFailed"], "MaxAttempts": 2}],
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Notify"}],
                "Next": "Done",
            },
            "Done": {"Type": "Succeed"},
            "Notify": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {"TopicArn": "arn:local", "Subject": "failed"},
                "Next": "Failed",
            },
            "Failed": {"Type": "Fail", "Error": "PipelineFailed"},
        },
    }


def test_retry_then_success(settings, monkeypatch):
    calls = []

    def flaky(_s, _c):
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError("transient")
        return {"ok": True}

    monkeypatch.setitem(STEPS, "maintenance", flaky)
    ex = runner.execute(settings, {"run_date": "2026-10-09"}, _machine())
    assert len(calls) == 3
    assert ex.outputs["maintenance"] == {"ok": True}
    assert [h["state"] for h in ex.history] == ["Work", "Done"]


def test_dq_failure_is_not_retried_and_alerts(settings, monkeypatch):
    calls = []

    def gate(_s, _c):
        calls.append(1)
        raise DataQualityError([])

    monkeypatch.setitem(STEPS, "maintenance", gate)
    with pytest.raises(runner.PipelineFailed) as err:
        runner.execute(settings, {"run_date": "2026-10-09"}, _machine())
    assert len(calls) == 1  # Retry matches States.TaskFailed only
    assert isinstance(err.value.cause, DataQualityError)
    alert = json.loads((settings.ops_dir / "alerts.jsonl").read_text())
    assert alert["error"]["Error"] == "DataQualityError"
    [execution] = list((settings.ops_dir / "executions").glob("*.json"))
    assert json.loads(execution.read_text())["status"] == "FAILED"


def test_cli_exit_codes(settings, monkeypatch, capsys):
    monkeypatch.setenv("MUSICPODS_LB_USERS", "lots")
    assert cli.main(["asl", "validate"]) == cli.EXIT_CONFIG
    monkeypatch.delenv("MUSICPODS_LB_USERS")
    assert cli.main(["--data-dir", str(settings.data_dir), "asl", "validate"]) == 0
    assert "valid" in capsys.readouterr().out

    def gate(_s, _c):
        raise DataQualityError([])

    monkeypatch.setitem(STEPS, "ingest-listenbrainz", gate)
    code = cli.main(["--data-dir", str(settings.data_dir), "run", "--from-dir", str(FIXTURES)])
    assert code == cli.EXIT_DQ


@mock_aws
def test_glue_entrypoint_syncs_state_around_a_step(tmp_path, monkeypatch):
    s3 = boto3.client("s3", region_name="eu-west-1")
    s3.create_bucket(Bucket="lake", CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})
    for path in (FIXTURES / "apple" / "charts").rglob("*.json"):
        key = "bronze/" + path.relative_to(FIXTURES).as_posix()
        s3.put_object(Bucket="lake", Key=key, Body=path.read_bytes())
    s3.put_object(Bucket="lake", Key="state/ops/keep.txt", Body=b"unchanged")
    env = {
        # The test keeps moto + the SQLite catalog; a deployed job would use AWS + Glue.
        "MUSICPODS_AWS_MODE": "aws",
        "MUSICPODS_ICEBERG_CATALOG": "sql",
        "MUSICPODS_DQ_FAIL_ON_ERROR": "true",
    }
    monkeypatch.setitem(STEPS, "build-raw-tables", _only_charts)
    code = entrypoint.main(
        [
            "--step",
            "build-raw-tables",
            "--lake_bucket",
            "lake",
            "--run_date",
            "2026-10-09T05:30:00Z",
            "--data_dir",
            str(tmp_path / "scratch"),
            "--JOB_ID",
            "j_123",
        ],
        environ=env,
    )
    assert code == 0
    keys = {o["Key"] for o in s3.list_objects_v2(Bucket="lake")["Contents"]}
    assert "state/ops/glue_last_run.json" in keys
    assert "state/ops/dq_results.jsonl" in keys
    body = s3.get_object(Bucket="lake", Key="state/ops/glue_last_run.json")["Body"].read()
    assert json.loads(body) == {"podcast_chart_entries": 450}
    # Bronze objects were downloaded, not re-uploaded.
    uploads = [k for k in keys if k.startswith("bronze/")]
    assert len(uploads) == len(list((FIXTURES / "apple" / "charts").rglob("*.json")))


def _only_charts(settings, _ctx):
    from musicpods.glue.jobs import run_jobs

    return run_jobs(settings, ["podcast_chart_entries"])


def test_entrypoint_rejects_unknown_step(tmp_path):
    with mock_aws():
        code = entrypoint.main(
            ["--step", "nope", "--lake_bucket", "lake", "--data_dir", str(tmp_path)],
            environ={"MUSICPODS_AWS_MODE": "aws"},
        )
    assert code == 2


def test_definition_and_schedule_register_in_the_emulator(settings):
    from musicpods.aws import aws_clients

    defn = runner.render(runner.definition_text())
    with aws_clients(settings) as aws:
        first = runner.register(aws, defn)
        again = runner.register(aws, defn)  # idempotent: update, not a second machine
        assert first == again
        described = aws.stepfunctions.describe_state_machine(
            stateMachineArn=first["state_machine_arn"]
        )
        assert json.loads(described["definition"])["StartAt"] == "Ingest"
        schedule = aws.scheduler.get_schedule(Name=first["schedule"])
        assert schedule["ScheduleExpression"] == "cron(15 6 * * ? *)"
        assert schedule["Target"]["Arn"] == first["state_machine_arn"]
        assert len(aws.stepfunctions.list_state_machines()["stateMachines"]) == 1
