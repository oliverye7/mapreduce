//! The MapReduce coordinator.
//!

use anyhow::Result;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use crate::rpc::coordinator::*;
use crate::*;

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, RwLock};

pub mod args;

pub struct Coordinator {
    // TODO: add your own fields
    state: Arc<Mutex<CoordinatorState>>,
}

#[derive(Default)]
pub struct CoordinatorState {
    worker_id: u16,
    worker_heartbeats: HashMap<u16, SystemTime>,
    job_id: u32,
    job_queue: VecDeque<u32>,
    job_information: HashMap<u32, JobRequest>,
}

#[derive(Debug, Clone)]
pub struct JobRequest {
    pub files: Vec<String>,
    pub output_dir: String,
    pub app: String,
    pub n_reduce: u32,
    pub args: Vec<u8>,
    pub status: JobState,
    pub map_task_assignments: Vec<MapTaskAssignment>, // for task reduce
    pub tasks: Vec<Task>, // all the map tasks that a worker can be assigned in a single job
    pub job_id: u32,
}

#[derive(Debug, Clone)]
pub struct JobState {
    pub done: bool,
    pub failed: bool,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Task {
    pub status: bool,   // completed?
    pub worker_id: u16, // if status = true, check to make sure the worker is alive
    pub file: String,   // single file that the Task is supposed to work on
    pub reduce: bool,   // if false, can assume we have a map task
    pub started: i8,    // flag
    pub task_id: u32,
    pub assigned: bool,
}

impl Coordinator {
    pub fn new(state: Arc<Mutex<CoordinatorState>>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl coordinator_server::Coordinator for Coordinator {
    async fn submit_job(
        &self,
        req: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobReply>, Status> {
        let request = req.get_ref();
        let files = request.files.clone();
        let output_dir = request.output_dir.clone();
        let app = request.app.clone();
        let n_reduce = request.n_reduce.clone();
        let args = request.args.clone();

        match crate::app::named(&app) {
            Ok(_) => (),
            Err(e) => return Err(Status::new(Code::InvalidArgument, e.to_string())),
        }

        let done = false;
        let failed = false;
        let errors: Vec<String> = Vec::new();
        let status = JobState {
            done,
            failed,
            errors,
        };

        let mut tasks: Vec<Task> = Vec::new();
        let mut map_task_assignments: Vec<MapTaskAssignment> = Vec::new();
        let mut task_id = 0;
        for elem in &files {
            let task = Task {
                status: false,
                worker_id: 0,
                file: elem.to_string(),
                reduce: false,
                started: -1,
                task_id: task_id,
                assigned: false,
            };
            tasks.push(task);

            let assignment = MapTaskAssignment {
                task: task_id,
                worker_id: 0,
            };
            map_task_assignments.push(assignment);
            task_id += 1;
        }

        task_id = 0;
        for i in 0..n_reduce as usize {
            let task = Task {
                status: false,
                worker_id: 0,
                file: "".to_string(),
                reduce: true,
                started: -1,
                task_id: task_id,
                assigned: false,
            };
            tasks.push(task);
            task_id += 1;
        }

        let mut state = self.state.lock().await;
        let mut id = state.job_id;
        let job_request = JobRequest {
            files: files,
            output_dir: output_dir,
            app: app,
            n_reduce: n_reduce,
            args: args,
            status: status,
            tasks: tasks,
            map_task_assignments: map_task_assignments,
            job_id: id,
        };
        id += 1;
        state.job_id = id;

        //let mut queue = self.job_queue.write().await;
        let mut queue = &mut state.job_queue;
        queue.push_back(id);
        state.job_queue = queue.clone();

        //let mut information_hash = self.job_information.write().await;
        let mut information_hash = &mut state.job_information;
        information_hash.insert(id, job_request);
        state.job_information = information_hash.clone();

        ::log::info!("Added job to the queue. ID {}", id);
        Ok(Response::new(SubmitJobReply { job_id: id as u32 }))
    }

    async fn poll_job(
        &self,
        req: Request<PollJobRequest>,
    ) -> Result<Response<PollJobReply>, Status> {
        let req = req.get_ref();
        let job_id = req.job_id;

        //let all_jobs = self.job_information.read().await;
        let state = self.state.lock().await;
        let information_hash = &state.job_information;

        if let Some(job_details) = information_hash.get(&job_id) {
            let job_state = &job_details.status;
            return Ok(Response::new(PollJobReply {
                done: job_state.done,
                failed: job_state.failed,
                errors: job_state.errors.clone(),
            }));
        } else {
            return Err(Status::new(Code::NotFound, "job id is invalid"));
        }
    }

    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        // TODO: Worker registration
        let mut state = self.state.lock().await;
        let req = req.get_ref();
        let id = req.worker_id;
        let mut guard = &mut state.worker_heartbeats;

        let time = SystemTime::now();

        guard.insert(id as u16, time);
        state.worker_heartbeats = guard.clone();

        Ok(Response::new(HeartbeatReply {}))
    }

    async fn register(
        &self,
        _req: Request<RegisterRequest>,
    ) -> Result<Response<RegisterReply>, Status> {
        // TODO: Worker registration
        //let mut id = self.worker_id.write().await;

        let mut state = self.state.lock().await;
        let mut id = state.worker_id;
        id += 1;
        state.worker_id = id;
        ::log::info!(
            "Received registration request. Assigned worker with ID {}",
            id
        );
        Ok(Response::new(RegisterReply {
            worker_id: id as u32,
        }))
    }

    async fn get_task(
        &self,
        req: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskReply>, Status> {
        // TODO: Tasks
        let req = req.get_ref();
        let id = req.worker_id; //TODO: VERIFY
        ::log::info!(
            "==========================================] request from worker {}",
            id
        );

        let mut state = self.state.lock().await;
        let job_queue = &state.job_queue;
        let mut information_hash = state.job_information.clone();

        let mut all_map_tasks_done = true;
        let mut all_tasks_done = true;

        // check the last time we received a heartbeat from all workers. for any workers that crashed:
        // mark task as not-started, clear the assignment, double check all fields that are relevant

        let guard = state.worker_heartbeats.clone();
        let time = SystemTime::now();
        let mut guard_copy = guard.clone();
        let mut cleanup: Vec<u16> = Vec::new();

        for (worker_id, last_beat) in guard.iter() {
            ::log::info!("checking worker {}...", worker_id);
            if let Ok(elapsed) = time.duration_since(*last_beat) {
                if (elapsed >= Duration::from_secs(TASK_TIMEOUT_SECS)) {
                    // a worker has crashed, look up its job and task assignment using the id
                    ::log::info!("worker {} has crashed...", worker_id);
                    for job_id in job_queue.iter() {
                        let job_id = *job_id as u32;
                        if let Some(job_details) = information_hash.get_mut(&job_id) {
                            for task in &mut job_details.tasks {
                                // found the task, reset all fields to be a nonstarted task
                                if (task.worker_id == *worker_id) {
                                    ::log::info!(
                                        "job {} task {}, reduce = {} from worker {} is being reset",
                                        job_id,
                                        task.task_id,
                                        task.reduce,
                                        worker_id
                                    );
                                    task.status = false;
                                    task.worker_id = 0;
                                    task.started = -1;
                                    task.assigned = false;
                                }
                            }
                        }
                    }
                    cleanup.push(*worker_id);
                }
            }
        }
        guard_copy = guard.clone();
        for dead_worker in &cleanup {
            guard_copy.remove(dead_worker);
            ::log::info!("worker {} has died", dead_worker);
        }

        for job_id in job_queue.iter() {
            let job_id = *job_id as u32;
            // we have a job_id, assign it

            if let Some(job_details) = information_hash.get_mut(&job_id) {
                // we found the job details; since we are in the first job not completed, we assign any task we want

                for task in &mut job_details.tasks {
                    if (task.status != true && task.reduce == false) {
                        all_map_tasks_done = false;
                    }
                    if (task.started == -1 && task.reduce == false && !task.assigned) {
                        //nobody is working on this map task yet, assign it
                        all_map_tasks_done = false;
                        all_tasks_done = false;
                        task.worker_id = id as u16;
                        task.started = 1;
                        task.assigned = true;

                        for assignment in &mut job_details.map_task_assignments {
                            if (assignment.task == task.task_id) {
                                assignment.worker_id = id;
                            }
                        }
                        ::log::info!(
                            "Job {}, reduce_type = {}, Task {} given to worker with ID {}",
                            job_id,
                            task.reduce,
                            task.task_id,
                            id
                        );

                        let result: Result<tonic::Response<rpc::coordinator::GetTaskReply>> =
                            Ok(Response::new(GetTaskReply {
                                job_id: job_id,
                                output_dir: job_details.output_dir.clone(),
                                app: job_details.app.clone(),
                                task: task.task_id,
                                file: task.file.clone(),
                                n_reduce: job_details.n_reduce,
                                n_map: job_details.files.len() as u32,
                                reduce: false,
                                wait: false,
                                map_task_assignments: job_details.map_task_assignments.clone(),
                                args: job_details.args.clone(),
                            }));

                        state.job_queue = job_queue.clone();
                        state.job_information = information_hash.clone();
                        state.worker_heartbeats = guard_copy.clone();

                        let response: Result<Response<GetTaskReply>, Status> =
                            result.map_err(|err| Status::internal(err.to_string()));
                        return response;
                    } else {
                        // TODO: check to see if the task failed
                        //::log::info!("task.started: {}, task.reduce: {}, task.status: {}, task.id: {}", task.started, task.reduce, task.status, task.task_id);
                    }
                }
                if (all_map_tasks_done) {
                    // we can start sending reduce tasks
                    ::log::info!("all map tasks done for job {}", job_id);
                    for task in &mut job_details.tasks {
                        if (task.started == -1 && task.reduce == true && !task.assigned) {
                            ::log::info!(
                                "Job {}, reduce_type = {}, Task {} given to worker with ID {}",
                                job_id,
                                task.reduce,
                                task.task_id,
                                id
                            );
                            task.worker_id = id as u16;
                            task.started = 1;
                            task.assigned = true;
                            all_tasks_done = false;

                            let result: Result<tonic::Response<rpc::coordinator::GetTaskReply>> =
                                Ok(Response::new(GetTaskReply {
                                    job_id: job_id,
                                    output_dir: job_details.output_dir.clone(),
                                    app: job_details.app.clone(),
                                    task: task.task_id,
                                    file: "".to_string(),
                                    n_reduce: job_details.n_reduce,
                                    n_map: job_details.files.len() as u32,
                                    reduce: true,
                                    wait: false,
                                    map_task_assignments: job_details.map_task_assignments.clone(),
                                    args: job_details.args.clone(),
                                }));

                            state.job_queue = job_queue.clone();
                            state.job_information = information_hash.clone();
                            state.worker_heartbeats = guard_copy.clone();

                            let response: Result<Response<GetTaskReply>, Status> =
                                result.map_err(|err| Status::internal(err.to_string()));
                            return response;
                        }
                    }
                }
                //for task in &mut job_details.tasks {
                //}
            } else {
                todo!("oof");
                // weird. we have a queued job but it isn't in our hash; can this case even happen?
            }
        }
        // we finished looping through ALL JOBS and ALL TASKS in EACH JOB
        // there are no queued jobs, idle
        state.job_queue = job_queue.clone();
        state.job_information = information_hash.clone();
        state.worker_heartbeats = guard_copy.clone();
        ::log::info!("idle worker {} ", id);
        return (Ok(Response::new(GetTaskReply {
            job_id: 0,
            output_dir: "".to_string(),
            app: "".to_string(),
            task: 0,
            file: "".to_string(),
            n_reduce: 0,
            n_map: 0,
            reduce: false,
            wait: true,
            map_task_assignments: Vec::new(),
            args: Vec::new(),
        })));
    }

    async fn finish_task(
        &self,
        req: Request<FinishTaskRequest>,
    ) -> Result<Response<FinishTaskReply>, Status> {
        // TODO: Tasks
        let req = req.get_ref();
        let worker_id = req.worker_id;
        let job_id = req.job_id;
        let completed_task = req.task;
        let reduce = req.reduce;

        let mut state = self.state.lock().await;
        let mut information_hash = state.job_information.clone();

        // mark task as finished
        let mut all_tasks_done = true;
        if let Some(job_details) = information_hash.get_mut(&job_id) {
            // we found the job details; since we are in the first job not completed, we assign any task we want
            for task in &mut job_details.tasks {
                if (task.task_id == completed_task && reduce == task.reduce) {
                    ::log::info!(
                        "job {}, task {} is finished, reduce_type: {} finished by worker {}",
                        job_id,
                        task.task_id,
                        task.reduce,
                        worker_id
                    );
                    task.status = true; // set completed status = done
                    task.started = 1;
                    task.assigned = true;
                }
                if (task.status != true) {
                    //::log::info!("task {}, reduce_type: {} is NOT finished", task.task_id, task.reduce);
                    all_tasks_done = false;
                }
            }
        }
        let job_queue = &state.job_queue;
        let mut queue = job_queue.clone();
        if (all_tasks_done) {
            if let Some(index) = queue.iter().position(|&x| x == job_id) {
                // Remove the element at the found index
                queue.remove(index);
            }
            if let Some(job_details) = information_hash.get_mut(&job_id) {
                let mut job_status = &mut job_details.status;
                job_status.done = true;
                job_status.failed = false;
            }
        }

        state.job_queue = queue.clone();
        state.job_information = information_hash.clone();

        Ok(Response::new(FinishTaskReply {}))
    }

    async fn fail_task(
        &self,
        req: Request<FailTaskRequest>,
    ) -> Result<Response<FailTaskReply>, Status> {
        // TODO: Fault tolerance

        let req = req.get_ref();
        let worker_id = req.worker_id;
        let job_id = req.job_id;
        let failed_task_id = req.task;
        let reduce = req.reduce;
        let retry = req.retry;
        let error = req.error.clone();

        let mut state = self.state.lock().await;
        let mut information_hash = state.job_information.clone();
        let job_queue = &mut state.job_queue;

        if let Some(job_details) = information_hash.get_mut(&job_id) {
            // case 1: reduce task fails, retry == true
            // mark task as not-started, clear the assignment, double check all fields that are relevant
            let assigned_worker = worker_id;
            if (retry) {
                for task in &mut job_details.tasks {
                    if (task.task_id == failed_task_id && reduce == task.reduce) {
                        // found the task, reset all fields to be a nonstarted task
                        task.status = false;
                        task.worker_id = 0;
                        task.started = -1;
                        task.assigned = false;
                    }
                }
            } else {
                // case 2: job fails, retry == false
                // update job status, remove the job from the queue
                job_details.status.done = false;
                job_details.status.failed = true;
                job_details.status.errors.push(error);
                if let Some(index) = job_queue.iter().position(|&x| x == job_id) {
                    // Remove the element at the found index
                    job_queue.remove(index);
                }
            }
        }
        state.job_queue = job_queue.clone();
        state.job_information = information_hash.clone();
        Ok(Response::new(FailTaskReply {}))
    }
}

pub async fn start(_args: args::Args) -> Result<()> {
    let addr = COORDINATOR_ADDR.parse().unwrap();

    let coordinator_state = CoordinatorState::default();

    let coordinator = Coordinator {
        state: Arc::new(Mutex::new(coordinator_state)),
    };

    let svc = coordinator_server::CoordinatorServer::new(coordinator);
    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
