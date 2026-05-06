use std::collections::HashMap;

use bw_core::config::DynError;
use lapjv::lapjv;
use ndarray::Array2;

pub const IMPOSSIBLE_SCORE: f64 = -1.0;
pub const SOLVER_IMPOSSIBLE_COST: f64 = 1_000_000_000.0;

#[derive(Debug, Clone)]
pub struct AssignmentInput {
    pub workers: Vec<String>,
    pub jobs: Vec<String>,
    pub costs: HashMap<(String, String), u32>,
}

#[derive(Debug, Clone)]
pub struct AssignmentOutput {
    pub assignments: Vec<(String, String, u32)>,
    pub total_cost: u32,
}

pub fn solve_assignment(input: AssignmentInput) -> Result<AssignmentOutput, DynError> {
    let workers = input.workers;
    let jobs = input.jobs;

    if workers.is_empty() {
        return Ok(AssignmentOutput {
            assignments: vec![],
            total_cost: 0,
        });
    }

    if jobs.is_empty() {
        return Err("Cannot assign workers because there are no jobs".into());
    }

    if workers.len() > jobs.len() {
        return Err(format!(
            "More workers than jobs is not supported: workers={} jobs={}",
            workers.len(),
            jobs.len()
        )
        .into());
    }

    let mut score_matrix =
        Array2::<f64>::from_elem((workers.len(), jobs.len()), IMPOSSIBLE_SCORE);

    for (worker_index, worker_id) in workers.iter().enumerate() {
        for (job_index, job_id) in jobs.iter().enumerate() {
            if let Some(score) = input.costs.get(&(worker_id.clone(), job_id.clone())) {
                score_matrix[[worker_index, job_index]] = *score as f64;
            }
        }
    }

    let mut solver_matrix =
        Array2::<f64>::from_elem((workers.len(), jobs.len()), SOLVER_IMPOSSIBLE_COST);

    for worker_index in 0..workers.len() {
        for job_index in 0..jobs.len() {
            let score = score_matrix[[worker_index, job_index]];

            if score != IMPOSSIBLE_SCORE {
                solver_matrix[[worker_index, job_index]] = -score;
            }
        }
    }

    let (_row_assignment, column_assignment) = lapjv(&solver_matrix)?;

    let mut assignments = Vec::new();
    let mut total_cost: u32 = 0;

    for (worker_index, job_index) in column_assignment.iter().enumerate() {
        let solver_cost = solver_matrix[[worker_index, *job_index]];

        if solver_cost >= SOLVER_IMPOSSIBLE_COST {
            return Err("No valid assignment found".into());
        }

        let worker_id = workers[worker_index].clone();
        let job_id = jobs[*job_index].clone();
        let cost = score_matrix[[worker_index, *job_index]] as u32;

        assignments.push((worker_id, job_id, cost));
        total_cost = total_cost.saturating_add(cost);
    }

    Ok(AssignmentOutput {
        assignments,
        total_cost,
    })
}