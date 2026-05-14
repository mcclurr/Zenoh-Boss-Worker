from assignment import assignment_pb2
from common import common_pb2

from bw.services.assignment.handler import handle_assignment_request


def test_handle_assignment_request_returns_proto_result():
    context = common_pb2.RequestContext(
        request_id="req-001",
        created_at_unix_ms=123,
        source="test",
        tags={"kind": "unit-test"},
    )

    request = assignment_pb2.AssignmentRequest(
        assignment_id="assignment-001",
        context=context,
    )

    request.workers.extend(
        [
            assignment_pb2.Worker(worker_id="alice", name="Alice"),
            assignment_pb2.Worker(worker_id="bob", name="Bob"),
        ]
    )

    request.jobs.extend(
        [
            assignment_pb2.Job(job_id="dishes", name="Wash dishes"),
            assignment_pb2.Job(job_id="laundry", name="Fold laundry"),
        ]
    )

    request.costs.extend(
        [
            assignment_pb2.AssignmentCost(worker_id="alice", job_id="dishes", cost=1),
            assignment_pb2.AssignmentCost(worker_id="alice", job_id="laundry", cost=10),
            assignment_pb2.AssignmentCost(worker_id="bob", job_id="dishes", cost=10),
            assignment_pb2.AssignmentCost(worker_id="bob", job_id="laundry", cost=1),
        ]
    )

    result = handle_assignment_request(request)

    assert result.assignment_id == "assignment-001"
    assert result.total_cost == 2

    pairs = [
        (assignment.worker_id, assignment.job_id, assignment.cost)
        for assignment in result.assignments
    ]

    assert pairs == [
        ("alice", "dishes", 1),
        ("bob", "laundry", 1),
    ]

    assert result.context.request_id == "req-001"