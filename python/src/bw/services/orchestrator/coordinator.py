import os
import threading
import time
from dataclasses import dataclass
from typing import Optional

from example1 import batch_pb2, input_pair_pb2

from bw.services.orchestrator.batch_runner import BatchRunner


WORKER_GATHER_WINDOW_SECONDS = float(
    os.getenv("WORKER_GATHER_WINDOW_SECONDS", "")
)

MAX_ACTIVE_WORKERS = int(os.getenv("MAX_ACTIVE_WORKERS", ""))

WORKER_LAST_SUCCESS_TTL_SECONDS = float(
    os.getenv("WORKER_LAST_SUCCESS_TTL_SECONDS", "")
)


@dataclass
class PendingWorker:
    message: input_pair_pb2.WorkerMessage
    received_monotonic: float
    sequence_number: int


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

        self.latest_jobs_batch: Optional[batch_pb2.BatchRequest] = None
        self.latest_jobs_received_monotonic: Optional[float] = None

        self.pending_workers: list[PendingWorker] = []
        self.worker_window_timer: Optional[threading.Timer] = None
        self.worker_sequence_number = 0

        self.active_worker_count = 0
        self.worker_last_success: dict[str, float] = {}

    def on_topic_a(self, sample) -> None:
        """
        Topic A now means: latest jobs batch.

        This message can be reused for future worker batches until a newer jobs
        batch arrives.
        """
        jobs_batch = batch_pb2.BatchRequest()
        jobs_batch.ParseFromString(sample.payload.to_bytes())
        now = time.monotonic()

        with self.lock:
            self.latest_jobs_batch = jobs_batch
            self.latest_jobs_received_monotonic = now

        self.logger.info(
            "[orchestrator] received jobs batch: batch_id=%s total_jobs=%s",
            jobs_batch.batch_id,
            jobs_batch.total_jobs,
        )

    def on_topic_b(self, sample) -> None:
        """
        Topic B now means: worker message.

        We gather worker messages for a short window, then prioritize them.
        """
        worker_msg = input_pair_pb2.WorkerMessage()
        worker_msg.ParseFromString(sample.payload.to_bytes())
        now = time.monotonic()

        with self.lock:
            self.worker_sequence_number += 1

            self.pending_workers.append(
                PendingWorker(
                    message=worker_msg,
                    received_monotonic=now,
                    sequence_number=self.worker_sequence_number,
                )
            )

            self.logger.info(
                "[orchestrator] received worker message: "
                "cycle_id=%s worker_id=%s pending_workers=%s",
                worker_msg.cycle_id,
                worker_msg.worker_id,
                len(self.pending_workers),
            )

            if self.worker_window_timer is None:
                self.worker_window_timer = threading.Timer(
                    WORKER_GATHER_WINDOW_SECONDS,
                    self._flush_worker_window,
                )
                self.worker_window_timer.daemon = True
                self.worker_window_timer.start()

    def expire_stale_if_idle(self, now: float) -> None:
        """
        Kept so main.py does not need to change much.

        This no longer expires A/B message pairs. Instead, it occasionally
        prunes old worker bookkeeping.
        """
        with self.lock:
            self._prune_worker_history_locked(now)

    def _flush_worker_window(self) -> None:
        with self.lock:
            self.worker_window_timer = None

            if self.latest_jobs_batch is None:
                dropped_count = len(self.pending_workers)
                self.pending_workers.clear()

                self.logger.warning(
                    "[orchestrator] dropping worker window because no jobs "
                    "batch has been received yet: dropped_workers=%s",
                    dropped_count,
                )
                return

            if not self.pending_workers:
                return

            available_slots = MAX_ACTIVE_WORKERS - self.active_worker_count
            if available_slots <= 0:
                self.logger.info(
                    "[orchestrator] no active worker slots available: "
                    "active=%s max=%s pending=%s",
                    self.active_worker_count,
                    MAX_ACTIVE_WORKERS,
                    len(self.pending_workers),
                )

                self._restart_worker_window_timer_locked()
                return

            workers_to_run = self._choose_workers_to_run_locked(
                max_workers=available_slots
            )

            if not workers_to_run:
                return

            jobs_batch = self.latest_jobs_batch

            for pending_worker in workers_to_run:
                self.active_worker_count += 1

                thread = threading.Thread(
                    target=self._run_worker_thread,
                    args=(jobs_batch, pending_worker.message),
                    daemon=True,
                )
                thread.start()

            if self.pending_workers:
                self._restart_worker_window_timer_locked()

    def _choose_workers_to_run_locked(
        self,
        max_workers: int,
    ) -> list[PendingWorker]:
        """
        Prioritize workers that have not successfully run recently.

        Lower last_success time wins. Workers that have never run get 0.0,
        so they are chosen first.
        """
        deduped: dict[str, PendingWorker] = {}

        for pending_worker in self.pending_workers:
            worker_id = pending_worker.message.worker_id

            if worker_id not in deduped:
                deduped[worker_id] = pending_worker

        prioritized = sorted(
            deduped.values(),
            key=lambda pending_worker: (
                self.worker_last_success.get(
                    pending_worker.message.worker_id,
                    0.0,
                ),
                pending_worker.sequence_number,
            ),
        )

        selected = prioritized[:max_workers]
        selected_worker_ids = {
            pending_worker.message.worker_id for pending_worker in selected
        }

        self.pending_workers = [
            pending_worker
            for pending_worker in self.pending_workers
            if pending_worker.message.worker_id not in selected_worker_ids
        ]

        self.logger.info(
            "[orchestrator] selected workers: selected=%s remaining_pending=%s",
            [worker.message.worker_id for worker in selected],
            len(self.pending_workers),
        )

        return selected

    def _run_worker_thread(
        self,
        jobs_batch: batch_pb2.BatchRequest,
        worker_msg: input_pair_pb2.WorkerMessage,
    ) -> None:
        succeeded = False

        try:
            self.batch_runner.run_worker_batch(
                jobs_batch=jobs_batch,
                worker_msg=worker_msg,
            )
            succeeded = True

        except Exception:
            self.logger.exception(
                "[orchestrator] failed to run worker batch: worker_id=%s",
                worker_msg.worker_id,
            )

        finally:
            now = time.monotonic()

            with self.lock:
                self.active_worker_count -= 1

                if succeeded:
                    self.worker_last_success[worker_msg.worker_id] = now

                self._prune_worker_history_locked(now)

                if self.pending_workers and self.worker_window_timer is None:
                    self._restart_worker_window_timer_locked()

    def _restart_worker_window_timer_locked(self) -> None:
        self.worker_window_timer = threading.Timer(
            WORKER_GATHER_WINDOW_SECONDS,
            self._flush_worker_window,
        )
        self.worker_window_timer.daemon = True
        self.worker_window_timer.start()

    def _prune_worker_history_locked(self, now: float) -> None:
        stale_worker_ids = [
            worker_id
            for worker_id, last_success in self.worker_last_success.items()
            if now - last_success > WORKER_LAST_SUCCESS_TTL_SECONDS
        ]

        for worker_id in stale_worker_ids:
            del self.worker_last_success[worker_id]

        if stale_worker_ids:
            self.logger.info(
                "[orchestrator] pruned worker history: count=%s",
                len(stale_worker_ids),
            )