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

    def run_batch(
        self,
        a_msg: input_pair_pb2.TopicAMessage,
        b_msg: input_pair_pb2.TopicBMessage,
        job_id: int,
    ) -> None:
        combined_id = f"{a_msg.cycle_id}-{b_msg.cycle_id}"

        combined = self._build_combined_input(
            combined_id=combined_id,
            a_msg=a_msg,
            b_msg=b_msg,
        )

        job = self._build_job(
            combined_id=combined_id,
            job_id=job_id,
            combined=combined,
            a_msg=a_msg,
        )

        self._publish_job(
            job=job,
            combined_id=combined_id,
            a_msg=a_msg,
            b_msg=b_msg,
        )

        result = self._wait_for_result(
            batch_id=combined_id,
            expected_job_id=job.job_id,
        )

        summary = self._build_summary(
            combined_id=combined_id,
            context=a_msg.context,
            result=result,
        )

        self._publish_summary(summary)

    def _build_combined_input(
        self,
        combined_id: str,
        a_msg: input_pair_pb2.TopicAMessage,
        b_msg: input_pair_pb2.TopicBMessage,
    ) -> input_pair_pb2.CombinedInput:
        return input_pair_pb2.CombinedInput(
            combined_id=combined_id,
            topic_a=a_msg,
            topic_b=b_msg,
            context=a_msg.context,
        )

    def _build_job(
        self,
        combined_id: str,
        job_id: int,
        combined: input_pair_pb2.CombinedInput,
        a_msg: input_pair_pb2.TopicAMessage,
    ) -> job_pb2.Job:
        return job_pb2.Job(
            batch_id=combined_id,
            job_id=job_id,
            payload=job_pb2.WorkPayload(
                raw_bytes=combined.SerializeToString()
            ),
            context=a_msg.context,
            steps=["combine-topic-a-topic-b", "process-combined-input"],
        )

    def _publish_job(
        self,
        job: job_pb2.Job,
        combined_id: str,
        a_msg: input_pair_pb2.TopicAMessage,
        b_msg: input_pair_pb2.TopicBMessage,
    ) -> None:
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

    def _build_summary(
        self,
        combined_id: str,
        context,
        result: result_pb2.JobResult,
    ) -> batch_pb2.BatchSummary:
        summary = batch_pb2.BatchSummary(
            batch_id=combined_id,
            total_jobs=1,
            results_received=1,
            context=context,
        )
        summary.results.append(result)
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