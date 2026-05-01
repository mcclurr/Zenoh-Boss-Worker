import os
import queue
import threading
import time

from chores import chores_pb2

from bw.messaging.zenoh import worker_request_key
from bw.services.orchestrator.result_dispatcher import ZenohResultDispatcher


RESULT_TIMEOUT_SECONDS = float(os.getenv("RESULT_TIMEOUT_SECONDS", "30"))


class BatchRunner:
    def __init__(
        self,
        zenoh_session,
        result_dispatcher: ZenohResultDispatcher,
        consumer_pub,
        logger,
        worker_instance_ids: list[str],
        max_inflight_per_worker: int,
    ) -> None:
        self.zenoh_session = zenoh_session
        self.result_dispatcher = result_dispatcher
        self.consumer_pub = consumer_pub
        self.logger = logger
        self.worker_instance_ids = worker_instance_ids
        self.max_inflight_per_worker = max_inflight_per_worker

        self.consumer_pub_lock = threading.Lock()
        self.worker_lock = threading.Lock()

        self.worker_inflight: dict[str, int] = {
            worker_id: 0 for worker_id in worker_instance_ids
        }

        self.worker_publishers = {
            worker_id: self.zenoh_session.declare_publisher(
                worker_request_key(worker_id)
            )
            for worker_id in worker_instance_ids
        }

    def run_chore_filter(
        self,
        chores: chores_pb2.Chores,
        person: chores_pb2.PersonAvailability,
    ) -> None:
        filter_id = f"{chores.chores_id}-{person.person_id}"

        worker_instance_id = self._reserve_worker_instance(filter_id)

        self.logger.info(
            "[orchestrator] running chore filter: "
            "chores_id=%s filter_id=%s person_id=%s chores=%s "
            "available_minutes=%s worker_instance_id=%s",
            chores.chores_id,
            filter_id,
            person.person_id,
            len(chores.chores),
            person.available_minutes,
            worker_instance_id,
        )

        result_queue = self.result_dispatcher.register_filter(filter_id)

        try:
            request = self._build_chore_filter_request(
                filter_id=filter_id,
                chores=chores,
                person=person,
            )

            self._publish_chore_filter_request(
                worker_instance_id=worker_instance_id,
                request=request,
            )

            results = self._wait_for_results(
                filter_id=filter_id,
                expected_result_count=1,
                result_queue=result_queue,
            )

            summary = self._build_summary(
                chores_id=chores.chores_id,
                context=chores.context,
                results=results,
            )

            self._publish_summary(summary)

        finally:
            self.result_dispatcher.unregister_filter(filter_id)
            self._release_worker_instance(worker_instance_id)

    def _reserve_worker_instance(self, filter_id: str) -> str:
        with self.worker_lock:
            available_workers = [
                worker_id
                for worker_id, inflight in self.worker_inflight.items()
                if inflight < self.max_inflight_per_worker
            ]

            if not available_workers:
                raise RuntimeError(
                    f"No Zenoh worker instance capacity available for filter_id={filter_id}"
                )

            selected_worker = min(
                available_workers,
                key=lambda worker_id: self.worker_inflight[worker_id],
            )

            self.worker_inflight[selected_worker] += 1

            self.logger.info(
                "[orchestrator] reserved worker instance: "
                "filter_id=%s worker_instance_id=%s inflight=%s max_inflight=%s",
                filter_id,
                selected_worker,
                self.worker_inflight[selected_worker],
                self.max_inflight_per_worker,
            )

            return selected_worker

    def _release_worker_instance(self, worker_instance_id: str) -> None:
        with self.worker_lock:
            self.worker_inflight[worker_instance_id] -= 1

            if self.worker_inflight[worker_instance_id] < 0:
                self.logger.warning(
                    "[orchestrator] worker inflight count went negative: "
                    "worker_instance_id=%s",
                    worker_instance_id,
                )
                self.worker_inflight[worker_instance_id] = 0

            self.logger.info(
                "[orchestrator] released worker instance: "
                "worker_instance_id=%s inflight=%s",
                worker_instance_id,
                self.worker_inflight[worker_instance_id],
            )

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

    def _publish_chore_filter_request(
        self,
        worker_instance_id: str,
        request: chores_pb2.ChoreFilterRequest,
    ) -> None:
        publisher = self.worker_publishers[worker_instance_id]
        key = worker_request_key(worker_instance_id)

        publisher.put(request.SerializeToString())

        self.logger.info(
            "[orchestrator] published chore filter request to zenoh worker: "
            "key=%s worker_instance_id=%s filter_id=%s chores_id=%s "
            "person_id=%s chores=%s available_minutes=%s",
            key,
            worker_instance_id,
            request.filter_id,
            request.chores.chores_id,
            request.person.person_id,
            len(request.chores.chores),
            request.person.available_minutes,
        )

    def _wait_for_results(
        self,
        filter_id: str,
        expected_result_count: int,
        result_queue: queue.Queue[chores_pb2.ChoreFilterResult],
    ) -> list[chores_pb2.ChoreFilterResult]:
        self.logger.info(
            "[orchestrator] waiting for chore filter results: "
            "filter_id=%s expected=%s",
            filter_id,
            expected_result_count,
        )

        deadline = time.monotonic() + RESULT_TIMEOUT_SECONDS
        results: list[chores_pb2.ChoreFilterResult] = []
        seen_filter_ids: set[str] = set()

        while len(results) < expected_result_count:
            remaining = deadline - time.monotonic()

            if remaining <= 0:
                raise TimeoutError(
                    f"Timed out waiting for chore filter result: "
                    f"filter_id={filter_id} received={len(results)} "
                    f"expected={expected_result_count}"
                )

            try:
                result = result_queue.get(timeout=min(remaining, 0.25))
            except queue.Empty:
                continue

            if result.filter_id in seen_filter_ids:
                self.logger.warning(
                    "[orchestrator] ignoring duplicate chore filter result: "
                    "filter_id=%s",
                    result.filter_id,
                )
                continue

            seen_filter_ids.add(result.filter_id)
            results.append(result)

            self.logger.info(
                "[orchestrator] received chore filter result %s/%s: "
                "filter_id=%s chores_id=%s person_id=%s accepted=%s rejected=%s "
                "used_minutes=%s remaining_minutes=%s",
                len(results),
                expected_result_count,
                result.filter_id,
                result.chores_id,
                result.person.person_id,
                len(result.accepted_chores),
                len(result.rejected_chores),
                result.used_minutes,
                result.remaining_minutes,
            )

        return results

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

    def _publish_summary(self, summary: chores_pb2.ChoreFilterSummary) -> None:
        with self.consumer_pub_lock:
            self.consumer_pub.put(summary.SerializeToString())

        self.logger.info(
            "[orchestrator] published chore filter summary to zenoh: "
            "chores_id=%s people_evaluated=%s chores_accepted=%s",
            summary.chores_id,
            summary.total_people_evaluated,
            summary.total_chores_accepted,
        )