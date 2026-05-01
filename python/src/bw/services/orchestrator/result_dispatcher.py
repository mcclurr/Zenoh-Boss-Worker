import queue
import threading
from typing import Optional

from chores import chores_pb2

from bw.messaging.zenoh import CHORE_FILTER_RESULT_KEY


class ZenohResultDispatcher:
    def __init__(self, zenoh_session, logger) -> None:
        self.zenoh_session = zenoh_session
        self.logger = logger

        self.lock = threading.Lock()
        self.result_queues: dict[str, queue.Queue[chores_pb2.ChoreFilterResult]] = {}

        self.subscriber = None

    def start(self) -> None:
        if self.subscriber is not None:
            return

        self.subscriber = self.zenoh_session.declare_subscriber(
            CHORE_FILTER_RESULT_KEY,
            self._on_result,
        )

        self.logger.info(
            "[result-dispatcher] subscribed to chore filter results: key=%s",
            CHORE_FILTER_RESULT_KEY,
        )

    def register_filter(
        self,
        filter_id: str,
    ) -> queue.Queue[chores_pb2.ChoreFilterResult]:
        with self.lock:
            if filter_id in self.result_queues:
                raise RuntimeError(f"Filter is already registered: {filter_id}")

            result_queue: queue.Queue[chores_pb2.ChoreFilterResult] = queue.Queue()
            self.result_queues[filter_id] = result_queue
            return result_queue

    def unregister_filter(self, filter_id: str) -> None:
        with self.lock:
            self.result_queues.pop(filter_id, None)

    def _on_result(self, sample) -> None:
        try:
            result = chores_pb2.ChoreFilterResult()
            result.ParseFromString(sample.payload.to_bytes())

            with self.lock:
                result_queue = self.result_queues.get(result.filter_id)

            if result_queue is None:
                self.logger.warning(
                    "[result-dispatcher] no waiter for chore filter result: "
                    "filter_id=%s chores_id=%s person_id=%s",
                    result.filter_id,
                    result.chores_id,
                    result.person.person_id,
                )
                return

            result_queue.put(result)

            self.logger.info(
                "[result-dispatcher] routed chore filter result: "
                "filter_id=%s chores_id=%s person_id=%s "
                "accepted=%s rejected=%s used_minutes=%s remaining_minutes=%s",
                result.filter_id,
                result.chores_id,
                result.person.person_id,
                len(result.accepted_chores),
                len(result.rejected_chores),
                result.used_minutes,
                result.remaining_minutes,
            )

        except Exception:
            self.logger.exception("[result-dispatcher] failed to process result")