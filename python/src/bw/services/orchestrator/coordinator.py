import os
import threading
import time
from dataclasses import dataclass

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


@dataclass
class BatchWindow:
    jobs_batch: batch_pb2.BatchRequest
    received_monotonic: float
    pending_workers: dict[str, PendingWorker]
    timer: threading.Timer | None
    sequence_number: int = 0


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

        self.current_window: BatchWindow | None = None

        self.active_worker_count = 0

        # worker_id -> last successful monotonic timestamp
        self.worker_last_success: dict[str, float] = {}

    def on_topic_a(self, sample) -> None:
        """
        Jobs batch arrived.

        This starts a short collection window. Worker messages that arrive
        during this window are candidates for this jobs batch.
        """
        jobs_batch = batch_pb2.BatchRequest()
        jobs_batch.ParseFromString(sample.payload.to_bytes())
        now = time.monotonic()

        with self.lock:
            if self.current_window is not None:
                dropped_workers = len(self.current_window.pending_workers)

                if self.current_window.timer is not None:
                    self.current_window.timer.cancel()

                self.logger.info(
                    "[orchestrator] replacing unflushed jobs window: "
                    "old_batch_id=%s dropped_workers=%s",
                    self.current_window.jobs_batch.batch_id,
                    dropped_workers,
                )

            timer = threading.Timer(
                WORKER_GATHER_WINDOW_SECONDS,
                self._flush_current_window,
            )
            timer.daemon = True

            self.current_window = BatchWindow(
                jobs_batch=jobs_batch,
                received_monotonic=now,
                pending_workers={},
                timer=timer,
            )

            timer.start()

        self.logger.info(
            "[orchestrator] received jobs batch: batch_id=%s total_jobs=%s "
            "worker_gather_window=%.3fs",
            jobs_batch.batch_id,
            jobs_batch.total_jobs,
            WORKER_GATHER_WINDOW_SECONDS,
        )

    def on_topic_b(self, sample) -> None:
        """
        Worker message arrived.

        Only worker messages that arrive during the current jobs batch window
        are considered. If there is no active jobs window, the worker message is
        dropped.
        """
        worker_msg = input_pair_pb2.WorkerMessage()
        worker_msg.ParseFromString(sample.payload.to_bytes())
        now = time.monotonic()

        with self.lock:
            if self.current_window is None:
                self.logger.info(
                    "[orchestrator] dropping worker message because no active "
                    "jobs window exists: cycle_id=%s worker_id=%s",
                    worker_msg.cycle_id,
                    worker_msg.worker_id,
                )
                return

            self.current_window.sequence_number += 1

            self.current_window.pending_workers[worker_msg.worker_id] = PendingWorker(
                message=worker_msg,
                received_monotonic=now,
                sequence_number=self.current_window.sequence_number,
            )

            pending_count = len(self.current_window.pending_workers)
            batch_id = self.current_window.jobs_batch.batch_id

        self.logger.info(
            "[orchestrator] received worker message for current batch: "
            "batch_id=%s cycle_id=%s worker_id=%s pending_unique_workers=%s",
            batch_id,
            worker_msg.cycle_id,
            worker_msg.worker_id,
            pending_count,
        )

    def expire_stale_if_idle(self, now: float) -> None:
        with self.lock:
            self._prune_worker_history_locked(now)

    def _flush_current_window(self) -> None:
        with self.lock:
            if self.current_window is None:
                return

            window = self.current_window
            self.current_window = None

            available_slots = MAX_ACTIVE_WORKERS - self.active_worker_count

            if available_slots <= 0:
                dropped_count = len(window.pending_workers)

                self.logger.info(
                    "[orchestrator] dropping jobs window because no worker slots "
                    "are available: batch_id=%s active=%s max=%s "
                    "dropped_workers=%s",
                    window.jobs_batch.batch_id,
                    self.active_worker_count,
                    MAX_ACTIVE_WORKERS,
                    dropped_count,
                )
                return

            if not window.pending_workers:
                self.logger.info(
                    "[orchestrator] dropping jobs window because no worker "
                    "messages arrived: batch_id=%s",
                    window.jobs_batch.batch_id,
                )
                return

            workers_to_run = self._choose_workers_to_run_locked(
                workers=list(window.pending_workers.values()),
                max_workers=available_slots,
            )

            selected_worker_ids = {
                pending_worker.message.worker_id
                for pending_worker in workers_to_run
            }

            dropped_worker_ids = [
                worker_id
                for worker_id in window.pending_workers.keys()
                if worker_id not in selected_worker_ids
            ]

            jobs_batch = batch_pb2.BatchRequest()
            jobs_batch.CopyFrom(window.jobs_batch)

            for pending_worker in workers_to_run:
                worker_msg = input_pair_pb2.WorkerMessage()
                worker_msg.CopyFrom(pending_worker.message)

                self.active_worker_count += 1

                thread = threading.Thread(
                    target=self._run_worker_thread,
                    args=(jobs_batch, worker_msg),
                    daemon=True,
                )
                thread.start()

            self.logger.info(
                "[orchestrator] flushed jobs window: batch_id=%s selected=%s "
                "dropped=%s active=%s max=%s",
                jobs_batch.batch_id,
                list(selected_worker_ids),
                dropped_worker_ids,
                self.active_worker_count,
                MAX_ACTIVE_WORKERS,
            )

    def _choose_workers_to_run_locked(
        self,
        workers: list[PendingWorker],
        max_workers: int,
    ) -> list[PendingWorker]:
        """
        Priority:
        1. Workers that have never completed successfully.
        2. Workers with the oldest successful completion time.
        3. Earlier arrival in this batch window.
        """
        prioritized = sorted(
            workers,
            key=lambda pending_worker: (
                self.worker_last_success.get(
                    pending_worker.message.worker_id,
                    0.0,
                ),
                pending_worker.sequence_number,
            ),
        )

        return prioritized[:max_workers]

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