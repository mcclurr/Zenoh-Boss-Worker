import os
import threading
import time
from dataclasses import dataclass
from typing import Optional

from example1 import batch_pb2, input_pair_pb2, job_pb2, result_pb2

from bw.common.log import init_logging
from bw.messaging.rabbitmq import (
    JOBS_QUEUE,
    RESULTS_QUEUE,
    connect_with_retry,
    declare_queues,
    publish_bytes,
)

from bw.messaging.zenoh import (
    ORCHESTRATOR_TO_CONSUMER_KEY,
    TOPIC_A_KEY,
    TOPIC_B_KEY,
    open_zenoh_session,
)


MATCH_WINDOW_SECONDS = float(os.getenv("MATCH_WINDOW_SECONDS", ""))


@dataclass
class PendingA:
    message: input_pair_pb2.TopicAMessage
    received_monotonic: float


@dataclass
class PendingB:
    message: input_pair_pb2.TopicBMessage
    received_monotonic: float


class BatchCoordinator:
    def __init__(self, rabbit_channel, zenoh_pub) -> None:
        self.logger = init_logging("orchestrator-python")
        self.rabbit_channel = rabbit_channel
        self.zenoh_pub = zenoh_pub
        self.lock = threading.Lock()
        self.pending_a: Optional[PendingA] = None
        self.pending_b: Optional[PendingB] = None
        self.active = False
        self.job_number = 1

    def on_topic_a(self, sample) -> None:
        message = input_pair_pb2.TopicAMessage()
        message.ParseFromString(sample.payload.to_bytes())
        now = time.monotonic()

        with self.lock:
            if self.active:
                self.logger.info(
                    "[orchestrator] dropping topic A while active: cycle_id=%s",
                    message.cycle_id,
                )
                return

            self.pending_a = PendingA(message=message, received_monotonic=now)
            self.logger.info(
                "[orchestrator] received topic A: cycle_id=%s text=%s",
                message.cycle_id,
                message.text,
            )
            self._maybe_start_batch_locked(now)

    def on_topic_b(self, sample) -> None:
        message = input_pair_pb2.TopicBMessage()
        message.ParseFromString(sample.payload.to_bytes())
        now = time.monotonic()

        with self.lock:
            if self.active:
                self.logger.info(
                    "[orchestrator] dropping topic B while active: cycle_id=%s",
                    message.cycle_id,
                )
                return

            self.pending_b = PendingB(message=message, received_monotonic=now)
            self.logger.info(
                "[orchestrator] received topic B: cycle_id=%s value=%s",
                message.cycle_id,
                message.value,
            )
            self._maybe_start_batch_locked(now)

    def _maybe_start_batch_locked(self, now: float) -> None:
        self._expire_stale_locked(now)

        if self.pending_a is None or self.pending_b is None:
            return

        delta = abs(
            self.pending_a.received_monotonic - self.pending_b.received_monotonic
        )

        if delta > MATCH_WINDOW_SECONDS:
            self.logger.info(
                "[orchestrator] messages too far apart: "
                "a_cycle_id=%s b_cycle_id=%s delta=%.3fs window=%.3fs",
                self.pending_a.message.cycle_id,
                self.pending_b.message.cycle_id,
                delta,
                MATCH_WINDOW_SECONDS,
            )

            if self.pending_a.received_monotonic < self.pending_b.received_monotonic:
                self.pending_a = None
            else:
                self.pending_b = None
            return

        a_msg = self.pending_a.message
        b_msg = self.pending_b.message

        self.pending_a = None
        self.pending_b = None
        self.active = True

        thread = threading.Thread(
            target=self._run_batch,
            args=(a_msg, b_msg),
            daemon=True,
        )
        thread.start()

    def _expire_stale_locked(self, now: float) -> None:
        if (
            self.pending_a is not None
            and (now - self.pending_a.received_monotonic) > MATCH_WINDOW_SECONDS
        ):
            age = now - self.pending_a.received_monotonic
            self.logger.info(
                "[orchestrator] expiring stale topic A: cycle_id=%s age=%.3fs",
                self.pending_a.message.cycle_id,
                age,
            )
            self.pending_a = None

        if (
            self.pending_b is not None
            and (now - self.pending_b.received_monotonic) > MATCH_WINDOW_SECONDS
        ):
            age = now - self.pending_b.received_monotonic
            self.logger.info(
                "[orchestrator] expiring stale topic B: cycle_id=%s age=%.3fs",
                self.pending_b.message.cycle_id,
                age,
            )
            self.pending_b = None

    def _run_batch(
        self,
        a_msg: input_pair_pb2.TopicAMessage,
        b_msg: input_pair_pb2.TopicBMessage,
    ) -> None:
        try:
            combined_id = f"{a_msg.cycle_id}-{b_msg.cycle_id}"

            combined = input_pair_pb2.CombinedInput(
                combined_id=combined_id,
                topic_a=a_msg,
                topic_b=b_msg,
                context=a_msg.context,
            )

            job = job_pb2.Job(
                batch_id=combined_id,
                job_id=self.job_number,
                payload=job_pb2.WorkPayload(
                    raw_bytes=combined.SerializeToString()
                ),
                context=a_msg.context,
                steps=["combine-topic-a-topic-b", "process-combined-input"],
            )

            self.job_number += 1

            publish_bytes(
                self.rabbit_channel,
                JOBS_QUEUE,
                job.SerializeToString(),
            )

            self.logger.info(
                "[orchestrator] queued combined job: combined_id=%s "
                "job_id=%s topic_a_text=%s topic_b_value=%s",
                combined_id,
                job.job_id,
                a_msg.text,
                b_msg.value,
            )

            result = self._wait_for_result(
                batch_id=combined_id,
                expected_job_id=job.job_id,
            )

            summary = batch_pb2.BatchSummary(
                batch_id=combined_id,
                total_jobs=1,
                results_received=1,
                context=a_msg.context,
            )
            summary.results.append(result)

            self.zenoh_pub.put(summary.SerializeToString())

            self.logger.info(
                "[orchestrator] published batch summary to zenoh: "
                "batch_id=%s total_jobs=%s results_received=%s",
                summary.batch_id,
                summary.total_jobs,
                summary.results_received,
            )

        except Exception:
            self.logger.exception("[orchestrator] failed to process combined batch")
        finally:
            with self.lock:
                self.active = False

    def _wait_for_result(
        self,
        batch_id: str,
        expected_job_id: int,
    ) -> result_pb2.JobResult:
        self.logger.info(
            "[orchestrator] waiting for result: batch_id=%s job_id=%s",
            batch_id,
            expected_job_id,
        )

        while True:
            method_frame, header_frame, body = self.rabbit_channel.basic_get(
                queue=RESULTS_QUEUE,
                auto_ack=False,
            )

            if method_frame is None:
                time.sleep(0.25)
                continue

            try:
                result = result_pb2.JobResult()
                result.ParseFromString(body)

                if result.batch_id != batch_id:
                    self.logger.warning(
                        "[orchestrator] ignoring stale/non-matching result: "
                        "batch_id=%s job_id=%s",
                        result.batch_id,
                        result.job_id,
                    )
                    self.rabbit_channel.basic_ack(
                        delivery_tag=method_frame.delivery_tag
                    )
                    continue

                if result.job_id != expected_job_id:
                    self.logger.warning(
                        "[orchestrator] ignoring unexpected job result: "
                        "batch_id=%s job_id=%s expected_job_id=%s",
                        result.batch_id,
                        result.job_id,
                        expected_job_id,
                    )
                    self.rabbit_channel.basic_ack(
                        delivery_tag=method_frame.delivery_tag
                    )
                    continue

                self.rabbit_channel.basic_ack(
                    delivery_tag=method_frame.delivery_tag
                )

                self.logger.info(
                    "[orchestrator] received result: batch_id=%s job_id=%s "
                    "worker=%s result=%s",
                    result.batch_id,
                    result.job_id,
                    result.worker,
                    result.result,
                )

                return result

            except Exception:
                self.logger.exception("[orchestrator] error processing result")
                self.rabbit_channel.basic_nack(
                    delivery_tag=method_frame.delivery_tag,
                    requeue=True,
                )


def main() -> None:
    logger = init_logging("orchestrator-python")

    rabbit_connection = connect_with_retry(logger=logger)
    rabbit_channel = rabbit_connection.channel()
    declare_queues(rabbit_channel)

    logger.info("[orchestrator] connected to RabbitMQ")

    with open_zenoh_session() as zenoh_session:
        logger.info("[orchestrator] connected to Zenoh")

        consumer_pub = zenoh_session.declare_publisher(ORCHESTRATOR_TO_CONSUMER_KEY)

        coordinator = BatchCoordinator(
            rabbit_channel=rabbit_channel,
            zenoh_pub=consumer_pub,
        )

        zenoh_session.declare_subscriber(TOPIC_A_KEY, coordinator.on_topic_a)
        zenoh_session.declare_subscriber(TOPIC_B_KEY, coordinator.on_topic_b)

        logger.info(
            "[orchestrator] subscribed to topic_a=%s topic_b=%s "
            "match_window=%.3fs",
            TOPIC_A_KEY,
            TOPIC_B_KEY,
            MATCH_WINDOW_SECONDS,
        )

        while True:
            time.sleep(0.1)
            with coordinator.lock:
                if not coordinator.active:
                    coordinator._expire_stale_locked(time.monotonic())


if __name__ == "__main__":
    main()