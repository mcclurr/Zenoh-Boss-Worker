use std::collections::HashMap;

use bw_core::config::DynError;
use lapjv::lapjv;
use ndarray::Array2;

pub const IMPOSSIBLE_SCORE: f64 = -1.0;
pub const SOLVER_IMPOSSIBLE_COST: f64 = 1_000_000_000.0;
pub const DUMMY_ASSIGNMENT_COST: f64 = 0.0;

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

    if workers.is_empty() || jobs.is_empty() {
        return Ok(AssignmentOutput {
            assignments: vec![],
            total_cost: 0,
        });
    }

    let real_worker_count = workers.len();
    let real_job_count = jobs.len();
    let solver_size = real_worker_count.max(real_job_count);

    let mut solver_matrix =
        Array2::<f64>::from_elem((solver_size, solver_size), DUMMY_ASSIGNMENT_COST);

    let mut score_matrix =
        Array2::<f64>::from_elem((real_worker_count, real_job_count), IMPOSSIBLE_SCORE);

    for (worker_index, worker_id) in workers.iter().enumerate() {
        for (job_index, job_id) in jobs.iter().enumerate() {
            if let Some(score) = input.costs.get(&(worker_id.clone(), job_id.clone())) {
                score_matrix[[worker_index, job_index]] = *score as f64;
                solver_matrix[[worker_index, job_index]] = -(*score as f64);
            } else {
                solver_matrix[[worker_index, job_index]] = SOLVER_IMPOSSIBLE_COST;
            }
        }
    }

    let (row_assignment, _column_assignment) = lapjv(&solver_matrix)?;

    let mut assignments = Vec::new();
    let mut total_cost: u32 = 0;

    for (worker_index, job_index) in row_assignment.iter().enumerate() {
        let job_index = *job_index;

        let is_dummy_worker = worker_index >= real_worker_count;
        let is_dummy_job = job_index >= real_job_count;

        if is_dummy_worker || is_dummy_job {
            continue;
        }

        let solver_cost = solver_matrix[[worker_index, job_index]];

        if solver_cost >= SOLVER_IMPOSSIBLE_COST {
            continue;
        }

        let worker_id = workers[worker_index].clone();
        let job_id = jobs[job_index].clone();
        let cost = score_matrix[[worker_index, job_index]] as u32;

        assignments.push((worker_id, job_id, cost));
        total_cost = total_cost.saturating_add(cost);
    }

    Ok(AssignmentOutput {
        assignments,
        total_cost,
    })
}