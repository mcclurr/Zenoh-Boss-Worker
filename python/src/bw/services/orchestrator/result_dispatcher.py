import queue
import threading
from typing import Optional

from example1 import result_pb2

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
        self.result_queues: dict[str, queue.Queue[result_pb2.JobResult]] = {}
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

    def register_batch(self, batch_id: str) -> queue.Queue[result_pb2.JobResult]:
        with self.lock:
            if batch_id in self.result_queues:
                raise RuntimeError(f"Batch is already registered: {batch_id}")

            result_queue: queue.Queue[result_pb2.JobResult] = queue.Queue()
            self.result_queues[batch_id] = result_queue
            return result_queue

    def unregister_batch(self, batch_id: str) -> None:
        with self.lock:
            self.result_queues.pop(batch_id, None)

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
                    result = result_pb2.JobResult()
                    result.ParseFromString(body)

                    with self.lock:
                        result_queue = self.result_queues.get(result.batch_id)

                    if result_queue is None:
                        self.logger.warning(
                            "[result-dispatcher] no waiter for result: "
                            "batch_id=%s job_id=%s worker=%s",
                            result.batch_id,
                            result.job_id,
                            result.worker,
                        )
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    result_queue.put(result)

                    self.logger.info(
                        "[result-dispatcher] routed result: "
                        "batch_id=%s job_id=%s worker=%s",
                        result.batch_id,
                        result.job_id,
                        result.worker,
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