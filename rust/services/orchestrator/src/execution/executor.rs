use tokio::sync::mpsc::UnboundedSender;

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

pub trait WindowExecutor {
    fn submit_window(
        &mut self,
        chores: Chores,
        people: Vec<PersonAvailability>,
    );
}

pub struct PerPersonExecutor {
    batch_runner: BatchRunner,
    completion_tx: UnboundedSender<PersonJobCompletion>,
}

impl PerPersonExecutor {
    pub fn new(
        batch_runner: BatchRunner,
        completion_tx: UnboundedSender<PersonJobCompletion>,
    ) -> Self {
        Self {
            batch_runner,
            completion_tx,
        }
    }
}

impl WindowExecutor for PerPersonExecutor {
    fn submit_window(
        &mut self,
        chores: Chores,
        people: Vec<PersonAvailability>,
    ) {
        for person in people {
            let batch_runner = self.batch_runner.clone();
            let completion_tx = self.completion_tx.clone();

            let chores = chores.clone();
            let person_id = person.person_id.clone();

            tracing::info!(
                "[executor] started per-person job: chores_id={} person_id={}",
                chores.chores_id,
                person_id,
            );

            tokio::spawn(async move {
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

                if completion_tx
                    .send(PersonJobCompletion {
                        person_id,
                        succeeded,
                    })
                    .is_err()
                {
                    tracing::error!(
                        "[executor] failed to send per-person completion"
                    );
                }
            });
        }
    }
}

pub struct WindowBatchExecutor {
    batch_runner: BatchRunner,
    completion_tx: UnboundedSender<PersonJobCompletion>,
}

impl WindowBatchExecutor {
    pub fn new(
        batch_runner: BatchRunner,
        completion_tx: UnboundedSender<PersonJobCompletion>,
    ) -> Self {
        Self {
            batch_runner,
            completion_tx,
        }
    }
}

impl WindowExecutor for WindowBatchExecutor {
    fn submit_window(
        &mut self,
        chores: Chores,
        people: Vec<PersonAvailability>,
    ) {
        let batch_runner = self.batch_runner.clone();
        let completion_tx = self.completion_tx.clone();

        let person_ids = people
            .iter()
            .map(|person| person.person_id.clone())
            .collect::<Vec<_>>();

        tracing::info!(
            "[executor] started window-batch job: chores_id={} people={}",
            chores.chores_id,
            people.len(),
        );

        tokio::spawn(async move {
            let succeeded = match batch_runner
                .run_chore_filter_window(chores, people)
                .await
            {
                Ok(()) => true,
                Err(err) => {
                    tracing::error!(
                        "[executor] failed window-batch job: error={}",
                        err,
                    );
                    false
                }
            };

            for person_id in person_ids {
                if completion_tx
                    .send(PersonJobCompletion {
                        person_id,
                        succeeded,
                    })
                    .is_err()
                {
                    tracing::error!(
                        "[executor] failed to send window-batch completion"
                    );
                }
            }
        });
    }
}

pub fn build_window_executor(
    mode: JobSubmissionMode,
    batch_runner: BatchRunner,
    completion_tx: UnboundedSender<PersonJobCompletion>,
) -> Box<dyn WindowExecutor + Send> {
    match mode {
        JobSubmissionMode::PerPerson => {
            Box::new(PerPersonExecutor::new(
                batch_runner,
                completion_tx,
            ))
        }

        JobSubmissionMode::WindowBatch => {
            Box::new(WindowBatchExecutor::new(
                batch_runner,
                completion_tx,
            ))
        }
    }
}