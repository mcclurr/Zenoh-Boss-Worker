use async_trait::async_trait;

use bw_core::proto::demo::chores::{
    Chores,
    PersonAvailability,
};

use crate::execution::batch_runner::BatchRunner;

#[derive(Debug, Clone, Copy)]
pub enum JobSubmissionMode {
    PerPerson,
    WindowBatch,
}

#[derive(Debug, Clone)]
pub struct PersonJobCompletion {
    pub person_id: String,
    pub succeeded: bool,
}

#[async_trait]
pub trait WindowExecutor {
    async fn submit_window(
        &mut self,
        chores: Chores,
        people: Vec<PersonAvailability>,
    ) -> Vec<PersonJobCompletion>;
}

pub struct PerPersonExecutor {
    batch_runner: BatchRunner,
}

impl PerPersonExecutor {
    pub fn new(batch_runner: BatchRunner) -> Self {
        Self { batch_runner }
    }
}

#[async_trait]
impl WindowExecutor for PerPersonExecutor {
    async fn submit_window(
        &mut self,
        chores: Chores,
        people: Vec<PersonAvailability>,
    ) -> Vec<PersonJobCompletion> {
        let mut handles = Vec::new();

        for person in people {
            let batch_runner = self.batch_runner.clone();
            let chores = chores.clone();
            let person_id = person.person_id.clone();

            tracing::info!(
                "[executor] starting per-person job: chores_id={} person_id={}",
                chores.chores_id,
                person_id,
            );

            let handle = tokio::spawn(async move {
                let succeeded = match batch_runner
                    .run_chore_filter(chores, person)
                    .await
                {
                    Ok(()) => true,
                    Err(err) => {
                        tracing::error!(
                            "[executor] failed per-person job: person_id={} error={}",
                            person_id,
                            err,
                        );
                        false
                    }
                };

                PersonJobCompletion {
                    person_id,
                    succeeded,
                }
            });

            handles.push(handle);
        }

        let mut completions = Vec::new();

        for handle in handles {
            match handle.await {
                Ok(completion) => completions.push(completion),
                Err(err) => {
                    tracing::error!(
                        "[executor] failed to join per-person task: {}",
                        err,
                    );
                }
            }
        }

        completions
    }
}

pub struct WindowBatchExecutor {
    batch_runner: BatchRunner,
}

impl WindowBatchExecutor {
    pub fn new(batch_runner: BatchRunner) -> Self {
        Self { batch_runner }
    }
}

#[async_trait]
impl WindowExecutor for WindowBatchExecutor {
    async fn submit_window(
        &mut self,
        chores: Chores,
        people: Vec<PersonAvailability>,
    ) -> Vec<PersonJobCompletion> {
        let batch_runner = self.batch_runner.clone();

        let person_ids = people
            .iter()
            .map(|person| person.person_id.clone())
            .collect::<Vec<_>>();

        tracing::info!(
            "[executor] starting window-batch job: chores_id={} people={}",
            chores.chores_id,
            people.len(),
        );

        let handle = tokio::spawn(async move {
            batch_runner
                .run_chore_filter_window(chores, people)
                .await
        });

        let succeeded = match handle.await {
            Ok(Ok(())) => true,
            Ok(Err(err)) => {
                tracing::error!(
                    "[executor] failed window-batch job: error={}",
                    err,
                );
                false
            }
            Err(err) => {
                tracing::error!(
                    "[executor] failed to join window-batch task: {}",
                    err,
                );
                false
            }
        };

        person_ids
            .into_iter()
            .map(|person_id| PersonJobCompletion {
                person_id,
                succeeded,
            })
            .collect()
    }
}

pub fn build_window_executor(
    mode: JobSubmissionMode,
    batch_runner: BatchRunner,
) -> Box<dyn WindowExecutor + Send> {
    match mode {
        JobSubmissionMode::PerPerson => {
            Box::new(PerPersonExecutor::new(batch_runner))
        }

        JobSubmissionMode::WindowBatch => {
            Box::new(WindowBatchExecutor::new(batch_runner))
        }
    }
}