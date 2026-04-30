import os
import queue
import threading
import time

from example1 import batch_pb2, input_pair_pb2, result_pb2

from bw.messaging.rabbitmq import (
    JOBS_QUEUE,
    connect_with_retry,
    declare_queues,
    publish_bytes,
)
from bw.services.orchestrator.result_dispatcher import RabbitResultDispatcher


RESULT_TIMEOUT_SECONDS = float(os.getenv("RESULT_TIMEOUT_SECONDS", "30"))


class BatchRunner:
    def __init__(
        self,
        result_dispatcher: RabbitResultDispatcher,
        consumer_pub,
        logger,
    ) -> None:
        self.result_dispatcher = result_dispatcher
        self.consumer_pub = consumer_pub
        self.logger = logger
        self.consumer_pub_lock = threading.Lock()

    def run_worker_batch(
        self,
        jobs_batch: batch_pb2.BatchRequest,
        worker_msg: input_pair_pb2.WorkerMessage,
    ) -> None:
        batch_id = f"{jobs_batch.batch_id}-{worker_msg.worker_id}"

        self.logger.info(
            "[orchestrator] running worker batch: "
            "source_batch_id=%s run_batch_id=%s selected_worker_id=%s total_jobs=%s",
            jobs_batch.batch_id,
            batch_id,
            worker_msg.worker_id,
            jobs_batch.total_jobs,
        )

        result_queue = self.result_dispatcher.register_batch(batch_id)

        try:
            worker_batch_request = self._build_worker_batch_request(
                batch_id=batch_id,
                jobs_batch=jobs_batch,
                worker_msg=worker_msg,
            )

            self._publish_worker_batch_request(worker_batch_request)

            results = self._wait_for_results(
                batch_id=batch_id,
                expected_result_count=1,
                result_queue=result_queue,
            )

            summary = self._build_summary(
                batch_id=batch_id,
                total_jobs=jobs_batch.total_jobs,
                context=jobs_batch.context,
                results=results,
            )

            self._publish_summary(summary)

        finally:
            self.result_dispatcher.unregister_batch(batch_id)

    def _build_worker_batch_request(
        self,
        batch_id: str,
        jobs_batch: batch_pb2.BatchRequest,
        worker_msg: input_pair_pb2.WorkerMessage,
    ) -> batch_pb2.WorkerBatchRequest:
        request = batch_pb2.WorkerBatchRequest(
            batch_id=batch_id,
            context=jobs_batch.context,
        )

        request.jobs_batch.CopyFrom(jobs_batch)
        request.worker.CopyFrom(worker_msg)

        return request

    def _publish_worker_batch_request(
        self,
        request: batch_pb2.WorkerBatchRequest,
    ) -> None:
        connection = connect_with_retry(logger=self.logger)

        try:
            channel = connection.channel()
            declare_queues(channel)

            publish_bytes(
                channel,
                JOBS_QUEUE,
                request.SerializeToString(),
            )

            self.logger.info(
                "[orchestrator] queued worker batch request: "
                "batch_id=%s source_batch_id=%s selected_worker_id=%s jobs=%s",
                request.batch_id,
                request.jobs_batch.batch_id,
                request.worker.worker_id,
                len(request.jobs_batch.jobs),
            )

        finally:
            try:
                connection.close()
            except Exception:
                self.logger.exception(
                    "[orchestrator] failed to close RabbitMQ publish connection"
                )

    def _wait_for_results(
        self,
        batch_id: str,
        expected_result_count: int,
        result_queue: queue.Queue[result_pb2.JobResult],
    ) -> list[result_pb2.JobResult]:
        self.logger.info(
            "[orchestrator] waiting for worker-batch results: "
            "batch_id=%s expected=%s",
            batch_id,
            expected_result_count,
        )

        deadline = time.monotonic() + RESULT_TIMEOUT_SECONDS
        results: list[result_pb2.JobResult] = []
        seen_result_ids: set[int] = set()

        while len(results) < expected_result_count:
            remaining = deadline - time.monotonic()

            if remaining <= 0:
                raise TimeoutError(
                    f"Timed out waiting for worker-batch result: "
                    f"batch_id={batch_id} received={len(results)} "
                    f"expected={expected_result_count}"
                )

            try:
                result = result_queue.get(timeout=min(remaining, 0.25))
            except queue.Empty:
                continue

            if result.job_id in seen_result_ids:
                self.logger.warning(
                    "[orchestrator] ignoring duplicate worker-batch result: "
                    "batch_id=%s result_id=%s",
                    result.batch_id,
                    result.job_id,
                )
                continue

            seen_result_ids.add(result.job_id)
            results.append(result)

            self.logger.info(
                "[orchestrator] received worker-batch result %s/%s: "
                "batch_id=%s result_id=%s actual_worker=%s result=%s",
                len(results),
                expected_result_count,
                result.batch_id,
                result.job_id,
                result.worker,
                result.result,
            )

        return results

    def _build_summary(
        self,
        batch_id: str,
        total_jobs: int,
        context,
        results: list[result_pb2.JobResult],
    ) -> batch_pb2.BatchSummary:
        summary = batch_pb2.BatchSummary(
            batch_id=batch_id,
            total_jobs=total_jobs,
            results_received=len(results),
            context=context,
        )

        summary.results.extend(results)
        return summary

    def _publish_summary(self, summary: batch_pb2.BatchSummary) -> None:
        with self.consumer_pub_lock:
            self.consumer_pub.put(summary.SerializeToString())

        self.logger.info(
            "[orchestrator] published batch summary to zenoh: "
            "batch_id=%s total_jobs=%s results_received=%s",
            summary.batch_id,
            summary.total_jobs,
            summary.results_received,
        )