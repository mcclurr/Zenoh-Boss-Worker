import json
import time
from pathlib import Path

import pytest

from chores import chores_pb2
from common import common_pb2

from bw.services.orchestrator.config import OrchestratorConfig
from bw.services.orchestrator.coordinator import BatchCoordinator


ROOT = Path(__file__).resolve().parents[3]
CASE_DIR = ROOT / "test_cases" / "orchestrator" / "coordinator"


class FakeBatchRunner:
    def __init__(self):
        self.calls = []

    def run_chore_filter(self, chores, person):
        self.calls.append(
            {
                "chores_id": chores.chores_id,
                "person_id": person.person_id,
                "available_minutes": person.available_minutes,
            }
        )


class FakeLogger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def exception(self, *args, **kwargs):
        pass


def load_cases():
    return sorted(CASE_DIR.glob("*.json"))


def make_context(case_name: str):
    return common_pb2.RequestContext(
        request_id=f"req-{case_name}",
        created_at_unix_ms=123,
        source="orchestrator-test",
    )


def make_chores(event: dict, case_name: str):
    msg = chores_pb2.Chores(
        chores_id=event["chores_id"],
        context=make_context(case_name),
    )

    for chore_data in event.get("chores", []):
        msg.chores.append(
            chores_pb2.Chore(
                chore_id=chore_data["chore_id"],
                name=chore_data["name"],
                estimated_minutes=chore_data["estimated_minutes"],
            )
        )

    return msg


def make_person(event: dict, case_name: str):
    return chores_pb2.PersonAvailability(
        cycle_id=event.get("cycle_id", f"cycle-{case_name}"),
        person_id=event["person_id"],
        available_minutes=event["available_minutes"],
        context=make_context(case_name),
    )


def make_config(case: dict) -> OrchestratorConfig:
    worker_count = case.get("worker_count", case.get("max_active_filters", 4))
    worker_max_concurrent_requests = case.get("worker_max_concurrent_requests", 1)

    return OrchestratorConfig(
        worker_ids=[
            f"worker-{i}"
            for i in range(1, worker_count + 1)
        ],
        worker_max_concurrent_requests=worker_max_concurrent_requests,
        person_gather_window_seconds=case.get("window_seconds", 0.05),
        person_last_success_ttl_seconds=case.get(
            "person_last_success_ttl_seconds",
            30,
        ),
        result_timeout_seconds=case.get("result_timeout_seconds", 1),
    )


@pytest.mark.parametrize("case_path", load_cases(), ids=lambda p: p.stem)
def test_orchestrator_scenario(case_path):
    case = json.loads(case_path.read_text())

    batch_runner = FakeBatchRunner()

    coordinator = BatchCoordinator(
        batch_runner=batch_runner,
        config=make_config(case),
        logger=FakeLogger(),
    )

    for event in case["events"]:
        delay_seconds = event.get("delay_seconds", 0)

        if delay_seconds:
            time.sleep(delay_seconds)

        if event["type"] == "chores":
            coordinator.receive_chores(
                make_chores(event, case["name"])
            )

        elif event["type"] == "person":
            coordinator.receive_person(
                make_person(event, case["name"])
            )

        else:
            raise ValueError(f"Unknown event type: {event['type']}")

    time.sleep(
        case.get(
            "final_sleep_seconds",
            case.get("window_seconds", 0.05) + 0.05,
        )
    )

    assert batch_runner.calls == case["expected_calls"]