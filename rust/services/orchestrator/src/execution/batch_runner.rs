use bw_core::proto::demo::chores::{
    ChoreFilterRequest, ChoreFilterResult, ChoreFilterSummary, Chores,
    PersonAvailability,
};

use crate::processing::processor::process_chore_filter_request;

#[derive(Debug, Clone, Default)]
pub struct BatchRunner;

impl BatchRunner {
    pub fn new() -> Self {
        Self
    }

    pub fn run_chore_filter(
        &self,
        chores: Chores,
        person: PersonAvailability,
    ) -> ChoreFilterSummary {
        let filter_id = format!("{}-{}", chores.chores_id, person.person_id);

        println!(
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

        let result = process_chore_filter_request(request);

        Self::build_summary(
            chores.chores_id,
            chores.context,
            vec![result],
        )
    }

    pub fn run_chore_filter_window(
        &self,
        chores: Chores,
        people: Vec<PersonAvailability>,
    ) -> ChoreFilterSummary {
        println!(
            "[batch-runner] running local window chore filter: chores_id={} people={} chores={}",
            chores.chores_id,
            people.len(),
            chores.chores.len(),
        );

        let mut results: Vec<ChoreFilterResult> = Vec::new();

        for person in people {
            let filter_id = format!("{}-{}", chores.chores_id, person.person_id);

            let request = Self::build_chore_filter_request(
                filter_id,
                chores.clone(),
                person,
            );

            results.push(process_chore_filter_request(request));
        }

        Self::build_summary(
            chores.chores_id,
            chores.context,
            results,
        )
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
        context: Option<bw_core::proto::demo::common::RequestContext>,
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
}