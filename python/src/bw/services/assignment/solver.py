IMPOSSIBLE_COST = -1
SOLVER_IMPOSSIBLE_COST = 1_000_000_000


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

    score_matrix = np.full((len(workers), len(jobs)), IMPOSSIBLE_COST, dtype=int)

    for i, worker_id in enumerate(workers):
        for j, job_id in enumerate(jobs):
            if (worker_id, job_id) in data.costs:
                score_matrix[i, j] = data.costs[(worker_id, job_id)]

    # Convert max problem into min problem.
    # Missing edges remain impossible.
    solver_matrix = np.full(
        (len(workers), len(jobs)),
        SOLVER_IMPOSSIBLE_COST,
        dtype=int,
    )

    valid_mask = score_matrix != IMPOSSIBLE_COST
    solver_matrix[valid_mask] = -score_matrix[valid_mask]

    row_ind, col_ind = linear_sum_assignment(solver_matrix)

    assignments = []
    total_cost = 0

    for i, j in zip(row_ind, col_ind):
        if solver_matrix[i, j] >= SOLVER_IMPOSSIBLE_COST:
            raise ValueError("No valid assignment found")

        worker_id = workers[i]
        job_id = jobs[j]
        cost = int(score_matrix[i, j])

        assignments.append((worker_id, job_id, cost))
        total_cost += cost

    return AssignmentOutput(assignments=assignments, total_cost=total_cost)