use std::sync::Arc;

use prost::Message;
use rayon::{
    ThreadPool,
    ThreadPoolBuilder,
};
use tokio::sync::oneshot;

use bw_core::{
    config::DynError,
    proto::demo::{
        chores::{
            ChoreFilterRequest, ChoreFilterResult, ChoreFilterSummary, Chores,
            PersonAvailability,
        },
        common::RequestContext,
    },
};

use crate::processing::processor::process_chore_filter_request;

pub struct BatchRunner {
    summary_pub: Arc<zenoh::pubsub::Publisher<'static>>,
    worker_pool: Arc<ThreadPool>,
}

impl Clone for BatchRunner {
    fn clone(&self) -> Self {
        Self {
            summary_pub: Arc::clone(&self.summary_pub),
            worker_pool: Arc::clone(&self.worker_pool),
        }
    }
}

impl BatchRunner {
    pub fn new(
        summary_pub: zenoh::pubsub::Publisher<'static>,
        num_worker_threads: usize,
    ) -> Result<Self, DynError> {
        let worker_pool = ThreadPoolBuilder::new()
            .num_threads(num_worker_threads)
            .thread_name(|index| {
                format!("orchestrator-worker-{index}")
            })
            .build()?;

        Ok(Self {
            summary_pub: Arc::new(summary_pub),
            worker_pool: Arc::new(worker_pool),
        })
    }

    pub async fn run_chore_filter(
        &self,
        chores: Chores,
        person: PersonAvailability,
    ) -> Result<(), DynError> {
        let filter_id = format!("{}-{}", chores.chores_id, person.person_id);

        tracing::info!(
            "[batch-runner] running local chore filter: chores_id={} filter_id={} person_id={} chores={} available_minutes={}",
            chores.chores_id,
            filter_id,
            person.person_id,
            chores.chores.len(),
            person.available_minutes,
        );

        let request = Self::build_chore_filter_request(
            filter_id,
            chores.clone(),
            person,
        );

        let result = self.run_processor_on_worker(request).await?;

        let summary = Self::build_summary(
            chores.chores_id,
            chores.context,
            vec![result],
        );

        self.publish_summary(summary).await
    }

    pub async fn run_chore_filter_window(
        &self,
        chores: Chores,
        people: Vec<PersonAvailability>,
    ) -> Result<(), DynError> {
        tracing::info!(
            "[batch-runner] running local window chore filter: chores_id={} people={} chores={}",
            chores.chores_id,
            people.len(),
            chores.chores.len(),
        );

        let chores_for_worker = chores.clone();
        let worker_pool = Arc::clone(&self.worker_pool);

        let (tx, rx) = oneshot::channel();

        worker_pool.spawn(move || {
            let mut results = Vec::new();

            for person in people {
                let filter_id = format!(
                    "{}-{}",
                    chores_for_worker.chores_id,
                    person.person_id,
                );

                let request = Self::build_chore_filter_request(
                    filter_id,
                    chores_for_worker.clone(),
                    person,
                );

                results.push(process_chore_filter_request(request));
            }

            let _ = tx.send(results);
        });

        let results = rx.await.map_err(|err| {
            format!("window worker failed to return result: {err}")
        })?;

        let summary = Self::build_summary(
            chores.chores_id,
            chores.context,
            results,
        );

        self.publish_summary(summary).await
    }

    async fn run_processor_on_worker(
        &self,
        request: ChoreFilterRequest,
    ) -> Result<ChoreFilterResult, DynError> {
        let worker_pool = Arc::clone(&self.worker_pool);
        let (tx, rx) = oneshot::channel();

        worker_pool.spawn(move || {
            let result = process_chore_filter_request(request);
            let _ = tx.send(result);
        });

        let result = rx.await.map_err(|err| {
            format!("worker failed to return result: {err}")
        })?;

        Ok(result)
    }

    fn build_chore_filter_request(
        filter_id: String,
        chores: Chores,
        person: PersonAvailability,
    ) -> ChoreFilterRequest {
        ChoreFilterRequest {
            filter_id,
            context: chores.context.clone(),
            chores: Some(chores),
            person: Some(person),
        }
    }

    fn build_summary(
        chores_id: String,
        context: Option<RequestContext>,
        results: Vec<ChoreFilterResult>,
    ) -> ChoreFilterSummary {
        let total_chores_accepted = results
            .iter()
            .map(|result| result.accepted_chores.len() as u32)
            .sum();

        ChoreFilterSummary {
            chores_id,
            total_people_evaluated: results.len() as u32,
            total_chores_accepted,
            results,
            context,
        }
    }

    async fn publish_summary(
        &self,
        summary: ChoreFilterSummary,
    ) -> Result<(), DynError> {
        let mut buf = Vec::with_capacity(summary.encoded_len());
        summary.encode(&mut buf)?;

        self.summary_pub.put(buf).await?;

        tracing::info!(
            "[batch-runner] published local chore filter summary: chores_id={} people_evaluated={} chores_accepted={}",
            summary.chores_id,
            summary.total_people_evaluated,
            summary.total_chores_accepted,
        );

        Ok(())
    }
}