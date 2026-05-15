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

impl WindowExecutor for PerPersonExecutor {
    fn submit_window(
        &mut self,
        chores: Chores,
        people: Vec<PersonAvailability>,
    ) -> Vec<PersonJobCompletion> {
        let mut completions = Vec::new();

        for person in people {
            let person_id = person.person_id.clone();

            println!(
                "[executor] running per-person job: chores_id={} person_id={}",
                chores.chores_id,
                person_id,
            );

            let succeeded = std::panic::catch_unwind({
                let batch_runner = self.batch_runner.clone();
                let chores = chores.clone();
                let person = person.clone();

                move || {
                    let summary = batch_runner.run_chore_filter(chores, person);

                    println!(
                        "[executor] completed per-person summary: chores_id={} people_evaluated={} chores_accepted={}",
                        summary.chores_id,
                        summary.total_people_evaluated,
                        summary.total_chores_accepted,
                    );
                }
            })
            .is_ok();

            completions.push(PersonJobCompletion {
                person_id,
                succeeded,
            });
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

impl WindowExecutor for WindowBatchExecutor {
    fn submit_window(
        &mut self,
        chores: Chores,
        people: Vec<PersonAvailability>,
    ) -> Vec<PersonJobCompletion> {
        let person_ids: Vec<String> = people
            .iter()
            .map(|person| person.person_id.clone())
            .collect();

        println!(
            "[executor] running window-batch job: chores_id={} people={}",
            chores.chores_id,
            people.len(),
        );

        let succeeded = std::panic::catch_unwind({
            let batch_runner = self.batch_runner.clone();
            let chores = chores.clone();
            let people = people.clone();

            move || {
                let summary = batch_runner.run_chore_filter_window(chores, people);

                println!(
                    "[executor] completed window-batch summary: chores_id={} people_evaluated={} chores_accepted={}",
                    summary.chores_id,
                    summary.total_people_evaluated,
                    summary.total_chores_accepted,
                );
            }
        })
        .is_ok();

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