import threading
import time
from dataclasses import dataclass
from typing import Optional

from example1 import input_pair_pb2

from bw.services.orchestrator.batch_runner import BatchRunner


@dataclass
class PendingA:
    message: input_pair_pb2.TopicAMessage
    received_monotonic: float


@dataclass
class PendingB:
    message: input_pair_pb2.TopicBMessage
    received_monotonic: float


class BatchCoordinator:
    def __init__(
        self,
        batch_runner: BatchRunner,
        logger,
        match_window_seconds: float,
    ) -> None:
        self.batch_runner = batch_runner
        self.logger = logger
        self.match_window_seconds = match_window_seconds

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

    def expire_stale_if_idle(self, now: float) -> None:
        with self.lock:
            if not self.active:
                self._expire_stale_locked(now)

    def _maybe_start_batch_locked(self, now: float) -> None:
        self._expire_stale_locked(now)

        if self.pending_a is None or self.pending_b is None:
            return

        delta = abs(
            self.pending_a.received_monotonic
            - self.pending_b.received_monotonic
        )

        if delta > self.match_window_seconds:
            self.logger.info(
                "[orchestrator] messages too far apart: "
                "a_cycle_id=%s b_cycle_id=%s delta=%.3fs window=%.3fs",
                self.pending_a.message.cycle_id,
                self.pending_b.message.cycle_id,
                delta,
                self.match_window_seconds,
            )

            if self.pending_a.received_monotonic < self.pending_b.received_monotonic:
                self.pending_a = None
            else:
                self.pending_b = None
            return

        a_msg = self.pending_a.message
        b_msg = self.pending_b.message
        job_id = self.job_number

        self.job_number += 1
        self.pending_a = None
        self.pending_b = None
        self.active = True

        thread = threading.Thread(
            target=self._run_batch_thread,
            args=(a_msg, b_msg, job_id),
            daemon=True,
        )
        thread.start()

    def _expire_stale_locked(self, now: float) -> None:
        if (
            self.pending_a is not None
            and (now - self.pending_a.received_monotonic)
            > self.match_window_seconds
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
            and (now - self.pending_b.received_monotonic)
            > self.match_window_seconds
        ):
            age = now - self.pending_b.received_monotonic
            self.logger.info(
                "[orchestrator] expiring stale topic B: cycle_id=%s age=%.3fs",
                self.pending_b.message.cycle_id,
                age,
            )
            self.pending_b = None

    def _run_batch_thread(
        self,
        a_msg: input_pair_pb2.TopicAMessage,
        b_msg: input_pair_pb2.TopicBMessage,
        job_id: int,
    ) -> None:
        try:
            self.batch_runner.run_batch(
                a_msg=a_msg,
                b_msg=b_msg,
                job_id=job_id,
            )
        except Exception:
            self.logger.exception("[orchestrator] failed to process combined batch")
        finally:
            with self.lock:
                self.active = False