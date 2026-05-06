import json
from pathlib import Path

import pytest

from bw.services.assignment.solver import AssignmentInput, solve_assignment


ROOT = Path(__file__).resolve().parents[2]
CASE_DIR = ROOT / "test_cases" / "assignment"


def load_cases():
    return sorted(CASE_DIR.glob("*.json"))


def normalize_assignments(assignments):
    return sorted(assignments, key=lambda x: (x[0], x[1]))


@pytest.mark.parametrize("case_path", load_cases(), ids=lambda p: p.stem)
def test_assignment_golden_case(case_path):
    case = json.loads(case_path.read_text())

    costs = {
        (entry["worker_id"], entry["job_id"]): entry["cost"]
        for entry in case["costs"]
    }

    result = solve_assignment(
        AssignmentInput(
            workers=case["workers"],
            jobs=case["jobs"],
            costs=costs,
        )
    )

    assert result.total_cost == case["expected"]["total_cost"]

    if "assignments" in case["expected"]:
        expected_assignments = [
            (a["worker_id"], a["job_id"], a["cost"])
            for a in case["expected"]["assignments"]
        ]

        assert normalize_assignments(result.assignments) == normalize_assignments(
            expected_assignments
        )