"""A small local interpreter for the pipeline's Amazon States Language definition.

It executes the *same* ``pipeline.asl.json`` that Terraform deploys to Step Functions, so the
control flow (Parallel branches, Retry, Catch -> NotifyFailure -> Fail) is exercised on every
local run and in CI. Supported: Task, Parallel, Wait (skipped locally), Pass, Succeed, Fail,
``Retry`` and ``Catch``. Task resources map to local steps by Glue job name.

Each execution writes its state history to ``ops/executions/<execution_id>.json``.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib import resources
from pathlib import Path
from typing import Any

from musicpods.config import Settings
from musicpods.orchestration.steps import STEPS, Context
from musicpods.quality.checks import DataQualityError
from musicpods.warehouse import DbtTestFailure

log = logging.getLogger(__name__)

LOCAL_JOB_PREFIX = "musicpods-local"
LOCAL_TOPIC = "arn:aws:sns:eu-west-1:000000000000:musicpods-local-alerts"
SUPPORTED = {"Task", "Parallel", "Wait", "Pass", "Succeed", "Fail"}


class PipelineFailed(RuntimeError):
    def __init__(self, error: str, cause: BaseException | None):
        super().__init__(f"pipeline failed: {error}: {cause}")
        self.error = error
        self.cause = cause


def definition_text() -> str:
    return (
        resources.files("musicpods.orchestration")
        .joinpath("pipeline.asl.json")
        .read_text(encoding="utf-8")
    )


def render(text: str, job_prefix: str = LOCAL_JOB_PREFIX, topic: str = LOCAL_TOPIC) -> dict:
    return json.loads(
        text.replace("${job_prefix}", job_prefix).replace("${alerts_topic_arn}", topic)
    )


def validate(defn: dict[str, Any], path: str = "$") -> list[str]:
    """Structural checks: every Next/Catch target exists, every state type is supported, every
    Task maps to a local step, and every branch terminates."""
    problems: list[str] = []
    states = defn.get("States", {})
    if defn.get("StartAt") not in states:
        problems.append(f"{path}: StartAt {defn.get('StartAt')!r} is not a state")
    for name, st in states.items():
        where = f"{path}.{name}"
        kind = st.get("Type")
        if kind not in SUPPORTED:
            problems.append(f"{where}: unsupported Type {kind!r}")
        targets = [st["Next"]] if "Next" in st else []
        targets += [c["Next"] for c in st.get("Catch", [])]
        problems += [f"{where}: unknown target {t!r}" for t in targets if t not in states]
        if kind not in {"Succeed", "Fail"} and "Next" not in st and not st.get("End"):
            problems.append(f"{where}: neither Next nor End")
        if kind == "Task" and st["Resource"].startswith("arn:aws:states:::glue"):
            job = st["Parameters"]["JobName"]
            if _step_name(job) not in STEPS:
                problems.append(f"{where}: no local step for job {job!r}")
        for i, branch in enumerate(st.get("Branches", [])):
            problems += validate(branch, f"{where}.Branches[{i}]")
    return problems


def _step_name(job_name: str) -> str:
    for prefix in (LOCAL_JOB_PREFIX + "-",):
        if job_name.startswith(prefix):
            return job_name[len(prefix) :]
    return job_name.split("-", 2)[-1]


def _error_name(exc: BaseException) -> str:
    if isinstance(exc, DataQualityError | DbtTestFailure):
        return "DataQualityError"
    return "States.TaskFailed"


def _matches(error: str, equals: list[str]) -> bool:
    return "States.ALL" in equals or error in equals


@dataclass
class Execution:
    settings: Settings
    ctx: Context
    retry_scale: float = 0.0  # local runs do not wait the full Retry interval
    execution_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    history: list[dict[str, Any]] = field(default_factory=list)
    outputs: dict[str, Any] = field(default_factory=dict)
    failure: BaseException | None = None

    def _record(self, state: str, status: str, started: float, **extra: Any) -> None:
        self.history.append(
            {
                "state": state,
                "status": status,
                "seconds": round(time.monotonic() - started, 2),
                **extra,
            }
        )

    def run_machine(self, defn: dict[str, Any], data: dict[str, Any]) -> dict[str, Any]:
        name = defn["StartAt"]
        states = defn["States"]
        while True:
            st = states[name]
            kind = st["Type"]
            started = time.monotonic()
            if kind == "Succeed":
                self._record(name, "succeeded", started)
                return data
            if kind == "Fail":
                self._record(name, "failed", started, error=st.get("Error"))
                raise PipelineFailed(st.get("Error", "Failed"), self.failure)
            if kind == "Pass":
                self._record(name, "passed", started)
            elif kind == "Wait":
                self._record(name, "skipped_wait", started, seconds_requested=st.get("Seconds"))
            else:
                try:
                    if kind == "Parallel":
                        for branch in st["Branches"]:
                            self.run_machine(branch, data)
                    else:
                        self._task(name, st, data)
                    self._record(name, "succeeded", started)
                except PipelineFailed:
                    raise
                except Exception as exc:
                    error = _error_name(exc)
                    self._record(name, "caught", started, error=error, cause=str(exc)[:500])
                    self.failure = exc
                    target = next(
                        (c for c in st.get("Catch", []) if _matches(error, c["ErrorEquals"])),
                        None,
                    )
                    if target is None:
                        raise PipelineFailed(error, exc) from exc
                    data = {**data, "error": {"Error": error, "Cause": str(exc)[:500]}}
                    name = target["Next"]
                    continue
            if st.get("End"):
                return data
            name = st["Next"]

    def _task(self, name: str, st: dict[str, Any], data: dict[str, Any]) -> None:
        resource = st["Resource"]
        if resource.startswith("arn:aws:states:::sns:publish"):
            alert = {
                "execution_id": self.execution_id,
                "topic": st["Parameters"]["TopicArn"],
                "subject": st["Parameters"].get("Subject"),
                "error": data.get("error"),
                "at": datetime.now(UTC).isoformat(timespec="seconds"),
            }
            log.error("ALERT %s", json.dumps(alert))
            path = self.settings.ops_dir / "alerts.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(alert) + "\n")
            return
        step_name = _step_name(st["Parameters"]["JobName"])
        step = STEPS[step_name]
        retries = st.get("Retry", [])
        attempt = 0
        while True:
            try:
                log.info("state %s -> step %s", name, step_name)
                self.outputs[step_name] = step(self.settings, self.ctx)
                return
            except Exception as exc:
                error = _error_name(exc)
                rule = next((r for r in retries if _matches(error, r["ErrorEquals"])), None)
                if rule is None or attempt >= rule.get("MaxAttempts", 3):
                    raise
                delay = rule.get("IntervalSeconds", 1) * rule.get("BackoffRate", 2.0) ** attempt
                log.warning("step %s failed (%s); retry %d", step_name, exc, attempt + 1)
                time.sleep(delay * self.retry_scale)
                attempt += 1


def register(
    aws,
    definition: dict[str, Any],
    name: str = "musicpods-local-pipeline",
    schedule: str = "cron(15 6 * * ? *)",
) -> dict[str, str]:
    """Create (or update) the state machine and its daily EventBridge Scheduler schedule in the
    emulator, the way infra/modules/orchestration does in AWS. Returns the ARNs."""
    sfn, scheduler = aws.stepfunctions, aws.scheduler
    existing = {m["name"]: m["stateMachineArn"] for m in sfn.list_state_machines()["stateMachines"]}
    body = json.dumps(definition)
    if name in existing:
        arn = existing[name]
        sfn.update_state_machine(stateMachineArn=arn, definition=body)
    else:
        arn = sfn.create_state_machine(name=name, definition=body, roleArn=aws.iam_role_arn)[
            "stateMachineArn"
        ]
    schedules = {x["Name"] for x in scheduler.list_schedules().get("Schedules", [])}
    target = {
        "Arn": arn,
        "RoleArn": aws.iam_role_arn,
        "Input": json.dumps({"run_date": "<aws.scheduler.scheduled-time>"}),
    }
    kwargs = {
        "Name": f"{name}-daily",
        "ScheduleExpression": schedule,
        "ScheduleExpressionTimezone": "UTC",
        "FlexibleTimeWindow": {"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 15},
        "Target": target,
    }
    if kwargs["Name"] in schedules:
        scheduler.update_schedule(**kwargs)
    else:
        scheduler.create_schedule(**kwargs)
    return {"state_machine_arn": arn, "schedule": kwargs["Name"]}


def execute(
    settings: Settings,
    ctx: Context,
    defn: dict[str, Any] | None = None,
    retry_scale: float = 0.0,
) -> Execution:
    defn = defn or render(definition_text())
    problems = validate(defn)
    if problems:
        raise ValueError("invalid state machine: " + "; ".join(problems))
    ex = Execution(settings, ctx, retry_scale=retry_scale)
    started = datetime.now(UTC)
    status = "SUCCEEDED"
    try:
        ex.run_machine(defn, {"run_date": ctx.get("run_date")})
    except PipelineFailed:
        status = "FAILED"
        raise
    finally:
        out_dir: Path = settings.ops_dir / "executions"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / f"{ex.execution_id}.json").write_text(
            json.dumps(
                {
                    "execution_id": ex.execution_id,
                    "status": status,
                    "started_at": started.isoformat(timespec="seconds"),
                    "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
                    "input": {"run_date": ctx.get("run_date")},
                    "history": ex.history,
                    "outputs": ex.outputs,
                },
                indent=1,
                default=str,
            ),
            encoding="utf-8",
        )
    return ex
