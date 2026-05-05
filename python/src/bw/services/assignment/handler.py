from assignment import assignment_pb2

from bw.services.assignment.solver import (
    AssignmentInput,
    AssignmentOutput,
    solve_assignment,
)


def handle_assignment_request(
    request: assignment_pb2.AssignmentRequest,
) -> assignment_pb2.AssignmentResult:
    worker_ids = [worker.worker_id for worker in request.workers]
    job_ids = [job.job_id for job in request.jobs]

    costs: dict[tuple[str, str], int] = {}

    for cost in request.costs:
        costs[(cost.worker_id, cost.job_id)] = cost.cost

    output = solve_assignment(
        AssignmentInput(
            workers=worker_ids,
            jobs=job_ids,
            costs=costs,
        )
    )

    return build_result(request, output)


def handle_assignment_bytes(payload: bytes) -> bytes:
    request = assignment_pb2.AssignmentRequest()
    request.ParseFromString(payload)

    result = handle_assignment_request(request)

    return result.SerializeToString()


def build_result(
    request: assignment_pb2.AssignmentRequest,
    output: AssignmentOutput,
) -> assignment_pb2.AssignmentResult:
    result = assignment_pb2.AssignmentResult(
        assignment_id=request.assignment_id,
        total_cost=output.total_cost,
        context=request.context,
    )

    for worker_id, job_id, cost in output.assignments:
        result.assignments.append(
            assignment_pb2.Assignment(
                worker_id=worker_id,
                job_id=job_id,
                cost=cost,
            )
        )

    return result