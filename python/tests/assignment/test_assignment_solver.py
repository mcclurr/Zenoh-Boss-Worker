from bw.services.assignment.solver import AssignmentInput, solve_assignment


def test_solve_assignment_picks_minimum_total_cost():
    result = solve_assignment(
        AssignmentInput(
            workers=["alice", "bob", "carol"],
            jobs=["dishes", "laundry", "vacuum"],
            costs={
                ("alice", "dishes"): 1,
                ("alice", "laundry"): 10,
                ("alice", "vacuum"): 10,
                ("bob", "dishes"): 10,
                ("bob", "laundry"): 1,
                ("bob", "vacuum"): 10,
                ("carol", "dishes"): 10,
                ("carol", "laundry"): 10,
                ("carol", "vacuum"): 1,
            },
        )
    )

    assert result.total_cost == 3
    assert result.assignments == [
        ("alice", "dishes", 1),
        ("bob", "laundry", 1),
        ("carol", "vacuum", 1),
    ]


def test_solve_assignment_is_global_not_greedy():
    result = solve_assignment(
        AssignmentInput(
            workers=["w1", "w2"],
            jobs=["j1", "j2"],
            costs={
                ("w1", "j1"): 1,
                ("w1", "j2"): 2,
                ("w2", "j1"): 1,
                ("w2", "j2"): 100,
            },
        )
    )

    assert result.total_cost == 3
    assert result.assignments == [
        ("w1", "j2", 2),
        ("w2", "j1", 1),
    ]