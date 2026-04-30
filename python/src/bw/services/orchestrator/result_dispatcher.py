import queue
import threading
from typing import Optional

from chores import chores_pb2

from bw.messaging.rabbitmq import (
    RESULTS_QUEUE,
    connect_with_retry,
    declare_queues,
)


class RabbitResultDispatcher:
    def __init__(self, logger) -> None:
        self.logger = logger
        self.lock = threading.Lock()
        self.ready = threading.Event()
        self.startup_error: Exception | None = None
        self.result_queues: dict[str, queue.Queue[chores_pb2.ChoreFilterResult]] = {}
        self.thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self.thread is not None:
            return

        self.thread = threading.Thread(
            target=self._run,
            daemon=True,
        )
        self.thread.start()

        self.logger.info("[result-dispatcher] waiting for RabbitMQ connection")

        self.ready.wait()

        if self.startup_error is not None:
            raise RuntimeError(
                "Result dispatcher failed to start"
            ) from self.startup_error

        self.logger.info("[result-dispatcher] ready")

    def register_batch(
        self,
        filter_id: str,
    ) -> queue.Queue[chores_pb2.ChoreFilterResult]:
        with self.lock:
            if filter_id in self.result_queues:
                raise RuntimeError(f"Filter is already registered: {filter_id}")

            result_queue: queue.Queue[chores_pb2.ChoreFilterResult] = queue.Queue()
            self.result_queues[filter_id] = result_queue
            return result_queue

    def unregister_batch(self, filter_id: str) -> None:
        with self.lock:
            self.result_queues.pop(filter_id, None)

    def _run(self) -> None:
        try:
            connection = connect_with_retry(logger=self.logger)
            channel = connection.channel()
            declare_queues(channel)

            self.logger.info("[result-dispatcher] connected to RabbitMQ")
            self.ready.set()

            channel.basic_qos(prefetch_count=10)

            def callback(ch, method, properties, body) -> None:
                try:
                    result = chores_pb2.ChoreFilterResult()
                    result.ParseFromString(body)

                    with self.lock:
                        result_queue = self.result_queues.get(result.filter_id)

                    if result_queue is None:
                        self.logger.warning(
                            "[result-dispatcher] no waiter for result: "
                            "filter_id=%s chores_id=%s person_id=%s",
                            result.filter_id,
                            result.chores_id,
                            result.person.person_id,
                        )
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    result_queue.put(result)

                    self.logger.info(
                        "[result-dispatcher] routed result: "
                        "filter_id=%s chores_id=%s person_id=%s accepted=%s rejected=%s",
                        result.filter_id,
                        result.chores_id,
                        result.person.person_id,
                        len(result.accepted_chores),
                        len(result.rejected_chores),
                    )

                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception:
                    self.logger.exception("[result-dispatcher] failed to process result")
                    ch.basic_nack(
                        delivery_tag=method.delivery_tag,
                        requeue=True,
                    )

            channel.basic_consume(
                queue=RESULTS_QUEUE,
                on_message_callback=callback,
                auto_ack=False,
            )

            channel.start_consuming()

        except Exception as exc:
            self.startup_error = exc
            self.ready.set()
            self.logger.exception("[result-dispatcher] failed to start")