import threading
import time

from chores import chores_pb2


LOCAL_WORKER_NAME = "local-orchestrator"


class BatchRunner:
    def __init__(
        self,
        consumer_pub,
        logger,
    ) -> None:
        self.consumer_pub = consumer_pub
        self.logger = logger
        self.consumer_pub_lock = threading.Lock()

    def run_chore_filter(
        self,
        chores: chores_pb2.Chores,
        person: chores_pb2.PersonAvailability,
    ) -> None:
        filter_id = f"{chores.chores_id}-{person.person_id}"

        self.logger.info(
            "[orchestrator] running local chore filter: "
            "chores_id=%s filter_id=%s person_id=%s chores=%s "
            "available_minutes=%s",
            chores.chores_id,
            filter_id,
            person.person_id,
            len(chores.chores),
            person.available_minutes,
        )

        request = self._build_chore_filter_request(
            filter_id=filter_id,
            chores=chores,
            person=person,
        )

        result = self._process_chore_filter_request(request)

        summary = self._build_summary(
            chores_id=chores.chores_id,
            context=chores.context,
            results=[result],
        )

        self._publish_summary(summary)

    def _build_chore_filter_request(
        self,
        filter_id: str,
        chores: chores_pb2.Chores,
        person: chores_pb2.PersonAvailability,
    ) -> chores_pb2.ChoreFilterRequest:
        request = chores_pb2.ChoreFilterRequest(
            filter_id=filter_id,
            context=chores.context,
        )

        request.chores.CopyFrom(chores)
        request.person.CopyFrom(person)

        return request

    def _process_chore_filter_request(
        self,
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

                self.logger.info(
                    "[orchestrator] accepted chore locally: "
                    "filter_id=%s person_id=%s chore_id=%s name=%s "
                    "estimated_minutes=%s used_minutes=%s",
                    request.filter_id,
                    person.person_id,
                    chore.chore_id,
                    chore.name,
                    chore.estimated_minutes,
                    used_minutes,
                )

            else:
                rejected_chores.append(chore)

                self.logger.info(
                    "[orchestrator] rejected chore locally: "
                    "filter_id=%s person_id=%s chore_id=%s name=%s "
                    "estimated_minutes=%s used_minutes=%s available_minutes=%s",
                    request.filter_id,
                    person.person_id,
                    chore.chore_id,
                    chore.name,
                    chore.estimated_minutes,
                    used_minutes,
                    available_minutes,
                )

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

    def _build_summary(
        self,
        chores_id: str,
        context,
        results: list[chores_pb2.ChoreFilterResult],
    ) -> chores_pb2.ChoreFilterSummary:
        total_chores_accepted = sum(
            len(result.accepted_chores)
            for result in results
        )

        summary = chores_pb2.ChoreFilterSummary(
            chores_id=chores_id,
            total_people_evaluated=len(results),
            total_chores_accepted=total_chores_accepted,
            context=context,
        )

        summary.results.extend(results)

        return summary

    def _publish_summary(
        self,
        summary: chores_pb2.ChoreFilterSummary,
    ) -> None:
        with self.consumer_pub_lock:
            self.consumer_pub.put(summary.SerializeToString())

        self.logger.info(
            "[orchestrator] published local chore filter summary: "
            "chores_id=%s people_evaluated=%s chores_accepted=%s",
            summary.chores_id,
            summary.total_people_evaluated,
            summary.total_chores_accepted,
        )