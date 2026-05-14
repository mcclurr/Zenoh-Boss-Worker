from chores import chores_pb2


def process_chore_filter_request(
    request: chores_pb2.ChoreFilterRequest,
) -> chores_pb2.ChoreFilterResult:
    person = request.person
    available_minutes = person.available_minutes

    accepted_chores: list[chores_pb2.Chore] = []
    rejected_chores: list[chores_pb2.Chore] = []

    used_minutes = 0

    for chore in request.chores.chores:
        if used_minutes + chore.estimated_minutes <= available_minutes:
            accepted_chores.append(chore)
            used_minutes += chore.estimated_minutes
        else:
            rejected_chores.append(chore)

    result = chores_pb2.ChoreFilterResult(
        filter_id=request.filter_id,
        chores_id=request.chores.chores_id,
        person=person,
        used_minutes=used_minutes,
        remaining_minutes=max(available_minutes - used_minutes, 0),
        context=request.context,
    )

    result.accepted_chores.extend(accepted_chores)
    result.rejected_chores.extend(rejected_chores)

    return result