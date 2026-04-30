import time

from example1 import batch_pb2, input_pair_pb2, job_pb2, result_pb2

from bw.messaging.rabbitmq import (
    JOBS_QUEUE,
    RESULTS_QUEUE,
    publish_bytes,
)


class BatchRunner:
    def __init__(self, rabbit_channel, consumer_pub, logger) -> None:
        self.rabbit_channel = rabbit_channel
        self.consumer_pub = consumer_pub
        self.logger = logger

    def run_worker_batch(
        self,
        jobs_batch: batch_pb2.BatchRequest,
        worker_msg: input_pair_pb2.WorkerMessage,
    ) -> None:
        """
        Run one selected worker against the latest jobs batch.

        For now, this publishes all jobs from the latest BatchRequest to
        RabbitMQ, waits for all matching results, then publishes a BatchSummary.
        """
        batch_id = f"{jobs_batch.batch_id}-{worker_msg.worker_id}"

        self.logger.info(
            "[orchestrator] running worker batch: "
            "source_batch_id=%s run_batch_id=%s worker_id=%s total_jobs=%s",
            jobs_batch.batch_id,
            batch_id,
            worker_msg.worker_id,
            jobs_batch.total_jobs,
        )

        jobs_to_publish = []

        for original_job in jobs_batch.jobs:
            job = job_pb2.Job()
            job.CopyFrom(original_job)

            job.batch_id = batch_id
            job.context.CopyFrom(jobs_batch.context)
            job.steps.append(f"selected-worker:{worker_msg.worker_id}")

            jobs_to_publish.append(job)

        for job in jobs_to_publish:
            self._publish_job(job)

        results = self._wait_for_results(
            batch_id=batch_id,
            expected_job_count=len(jobs_to_publish),
        )

        summary = self._build_summary(
            batch_id=batch_id,
            context=jobs_batch.context,
            results=results,
        )

        self._publish_summary(summary)

    def _publish_job(self, job: job_pb2.Job) -> None:
        publish_bytes(
            self.rabbit_channel,
            JOBS_QUEUE,
            job.SerializeToString(),
        )

        self.logger.info(
            "[orchestrator] queued job: batch_id=%s job_id=%s",
            job.batch_id,
            job.job_id,
        )

    def _wait_for_results(
        self,
        batch_id: str,
        expected_job_count: int,
    ) -> list[result_pb2.JobResult]:
        self.logger.info(
            "[orchestrator] waiting for results: batch_id=%s expected=%s",
            batch_id,
            expected_job_count,
        )

        results: list[result_pb2.JobResult] = []
        seen_job_ids: set[int] = set()

        while len(results) < expected_job_count:
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
                        "batch_id=%s job_id=%s expected_batch_id=%s",
                        result.batch_id,
                        result.job_id,
                        batch_id,
                    )
                    self.rabbit_channel.basic_ack(
                        delivery_tag=method_frame.delivery_tag
                    )
                    continue

                if result.job_id in seen_job_ids:
                    self.logger.warning(
                        "[orchestrator] ignoring duplicate result: "
                        "batch_id=%s job_id=%s",
                        result.batch_id,
                        result.job_id,
                    )
                    self.rabbit_channel.basic_ack(
                        delivery_tag=method_frame.delivery_tag
                    )
                    continue

                seen_job_ids.add(result.job_id)
                results.append(result)

                self.rabbit_channel.basic_ack(
                    delivery_tag=method_frame.delivery_tag
                )

                self.logger.info(
                    "[orchestrator] received result %s/%s: "
                    "batch_id=%s job_id=%s worker=%s result=%s",
                    len(results),
                    expected_job_count,
                    result.batch_id,
                    result.job_id,
                    result.worker,
                    result.result,
                )

            except Exception:
                self.logger.exception("[orchestrator] error processing result")
                self.rabbit_channel.basic_nack(
                    delivery_tag=method_frame.delivery_tag,
                    requeue=True,
                )

        return results

    def _build_summary(
        self,
        batch_id: str,
        context,
        results: list[result_pb2.JobResult],
    ) -> batch_pb2.BatchSummary:
        summary = batch_pb2.BatchSummary(
            batch_id=batch_id,
            total_jobs=len(results),
            results_received=len(results),
            context=context,
        )

        summary.results.extend(results)
        return summary

    def _publish_summary(self, summary: batch_pb2.BatchSummary) -> None:
        self.consumer_pub.put(summary.SerializeToString())

        self.logger.info(
            "[orchestrator] published batch summary to zenoh: "
            "batch_id=%s total_jobs=%s results_received=%s",
            summary.batch_id,
            summary.total_jobs,
            summary.results_received,
        )