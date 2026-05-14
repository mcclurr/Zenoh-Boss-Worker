from bw.services.orchestrator.processor import process_chore_filter_request

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

        result = process_chore_filter_request(request)

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