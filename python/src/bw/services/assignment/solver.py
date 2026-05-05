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
    """
    Solves the minimum-cost one-to-one assignment problem.

    This is intentionally dependency-free for now. It uses DP over job subsets,
    which is great for small/medium demos and unit testing.

    Later, this function can be replaced with scipy.optimize.linear_sum_assignment
    or a real Hungarian/Jonker-Volgenant implementation without changing the
    service, protobuf handler, or tests.
    """
    workers = data.workers
    jobs = data.jobs

    if not workers:
        return AssignmentOutput(assignments=[], total_cost=0)

    if not jobs:
        raise ValueError("Cannot assign workers because there are no jobs")

    if len(workers) > len(jobs):
        raise ValueError(
            f"More workers than jobs is not supported by this simple solver: "
            f"workers={len(workers)} jobs={len(jobs)}"
        )

    # dp(worker_index, used_jobs_mask) -> (cost, chosen_job_indices)
    memo: dict[tuple[int, int], tuple[int, list[int]]] = {}

    def dp(worker_index: int, used_mask: int) -> tuple[int, list[int]]:
        if worker_index == len(workers):
            return 0, []

        key = (worker_index, used_mask)
        if key in memo:
            return memo[key]

        worker_id = workers[worker_index]
        best_cost = IMPOSSIBLE_COST
        best_path: list[int] = []

        for job_index, job_id in enumerate(jobs):
            if used_mask & (1 << job_index):
                continue

            pair_cost = data.costs.get((worker_id, job_id), IMPOSSIBLE_COST)
            remaining_cost, remaining_path = dp(
                worker_index + 1,
                used_mask | (1 << job_index),
            )

            total = pair_cost + remaining_cost

            if total < best_cost:
                best_cost = total
                best_path = [job_index] + remaining_path

        memo[key] = (best_cost, best_path)
        return memo[key]

    total_cost, chosen_job_indices = dp(0, 0)

    if total_cost >= IMPOSSIBLE_COST:
        raise ValueError("No valid assignment found")

    assignments = []

    for worker_id, job_index in zip(workers, chosen_job_indices):
        job_id = jobs[job_index]
        cost = data.costs[(worker_id, job_id)]
        assignments.append((worker_id, job_id, cost))

    return AssignmentOutput(
        assignments=assignments,
        total_cost=total_cost,
    )