use std::collections::HashMap;

use prost::Message;

use bw_core::{
    config::DynError,
    proto::demo::assignment::{
        Assignment, AssignmentRequest, AssignmentResult,
    },
};

use crate::solver::{solve_assignment, AssignmentInput, AssignmentOutput};

pub fn handle_assignment_bytes(payload: &[u8]) -> Result<Vec<u8>, DynError> {
    let request = AssignmentRequest::decode(payload)?;
    let result = handle_assignment_request(request)?;

    let mut buf = Vec::with_capacity(result.encoded_len());
    result.encode(&mut buf)?;

    Ok(buf)
}

pub fn handle_assignment_request(
    request: AssignmentRequest,
) -> Result<AssignmentResult, DynError> {
    let worker_ids = request
        .workers
        .iter()
        .map(|worker| worker.worker_id.clone())
        .collect::<Vec<_>>();

    let job_ids = request
        .jobs
        .iter()
        .map(|job| job.job_id.clone())
        .collect::<Vec<_>>();

    let costs = request
        .costs
        .iter()
        .map(|cost| {
            (
                (cost.worker_id.clone(), cost.job_id.clone()),
                cost.cost,
            )
        })
        .collect::<HashMap<_, _>>();

    let output = solve_assignment(AssignmentInput {
        workers: worker_ids,
        jobs: job_ids,
        costs,
    })?;

    Ok(build_result(request, output))
}

fn build_result(
    request: AssignmentRequest,
    output: AssignmentOutput,
) -> AssignmentResult {
    let assignments = output
        .assignments
        .into_iter()
        .map(|(worker_id, job_id, cost)| Assignment {
            worker_id,
            job_id,
            cost,
        })
        .collect();

    AssignmentResult {
        assignment_id: request.assignment_id,
        assignments,
        total_cost: output.total_cost,
        context: request.context,
    }
}