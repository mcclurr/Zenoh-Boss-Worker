import numpy as np
from scipy.optimize import linear_sum_assignment

from dataclasses import dataclass


@dataclass(frozen=True)
class AssignmentInput:
    workers: list[str]
    jobs: list[str]
    costs: dict[tuple[str, str], int]


@dataclass(frozen=True)
class AssignmentOutput:
    assignments: list[tuple[str, str, int]]
    total_cost: int


IMPOSSIBLE_COST = 1_000_000_000


def solve_assignment(data: AssignmentInput) -> AssignmentOutput:
    workers = data.workers
    jobs = data.jobs

    if not workers:
        return AssignmentOutput(assignments=[], total_cost=0)

    if not jobs:
        raise ValueError("Cannot assign workers because there are no jobs")

    if len(workers) > len(jobs):
        raise ValueError(
            f"More workers than jobs is not supported: "
            f"workers={len(workers)} jobs={len(jobs)}"
        )

    # Build dense cost matrix
    cost_matrix = np.full((len(workers), len(jobs)), IMPOSSIBLE_COST)

    for i, worker_id in enumerate(workers):
        for j, job_id in enumerate(jobs):
            if (worker_id, job_id) in data.costs:
                cost_matrix[i, j] = data.costs[(worker_id, job_id)]

    # Solve assignment
    row_ind, col_ind = linear_sum_assignment(cost_matrix)

    assignments = []
    total_cost = 0

    for i, j in zip(row_ind, col_ind):
        worker_id = workers[i]
        job_id = jobs[j]
        cost = int(cost_matrix[i, j])

        if cost >= IMPOSSIBLE_COST:
            raise ValueError("No valid assignment found")

        assignments.append((worker_id, job_id, cost))
        total_cost += cost

    return AssignmentOutput(
        assignments=assignments,
        total_cost=total_cost,
    )