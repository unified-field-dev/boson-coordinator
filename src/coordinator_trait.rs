//! Object-safe coordinator surface for local and distributed Boson backends.

use async_trait::async_trait;

use boson_core::{Job, JobStatus, Result, Run, TaskConfig, TaskRunStats};
use boson_runtime::{Boson, TaskRegistry};

/// Backend for Boson administration and enqueue.
///
/// Object-safe (`dyn`-compatible) so hosts can hold `Arc<dyn BosonCoordinatorBackend>` without
/// knowing whether jobs run through an in-process [`Boson`] runtime
/// ([`CoordinatorAdapter`](crate::CoordinatorAdapter)) or across an HTTP boundary
/// (`HttpRemoteBosonCoordinatorBackend`, `remote-http` feature).
///
/// # Examples
///
/// ```rust,ignore
/// use boson_coordinator::BosonCoordinatorBackend;
///
/// # async fn enqueue_and_check(backend: &dyn BosonCoordinatorBackend) -> boson_coordinator::Result<()> {
/// let job_id = backend
///     .enqueue("send_email", serde_json::json!({}), serde_json::json!({"to": "a@b.com"}), None)
///     .await?;
/// let job = backend.get_job(&job_id).await;
/// assert!(job.is_some());
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait BosonCoordinatorBackend: Send + Sync {
    /// Enqueue a task for background execution.
    ///
    /// `task_name` must be registered via `#[boson::task]` inventory. `actor_json` captures who
    /// is enqueueing (used by handlers that need identity); `params_json` is the task's typed
    /// parameters serialized to JSON. Returns the new job id on success.
    async fn enqueue(
        &self,
        task_name: &str,
        actor_json: serde_json::Value,
        params_json: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> Result<String>;

    /// Load one job by id.
    async fn get_job(&self, job_id: &str) -> Option<Job>;

    /// List jobs with optional status filter.
    async fn list_jobs(
        &self,
        status_filter: Option<JobStatus>,
        offset: usize,
        limit: usize,
    ) -> Vec<Job>;

    /// Cancel a job if still active.
    async fn cancel_job(&self, job_id: &str) -> Result<()>;

    /// Load task config (with registry defaults).
    async fn get_task_config(&self, task_name: &str) -> Result<TaskConfig>;

    /// Persist task config.
    async fn upsert_task_config(&self, config: TaskConfig) -> Result<()>;

    /// List runs.
    async fn list_runs(&self, job_id_filter: Option<&str>, offset: usize, limit: usize)
        -> Vec<Run>;

    /// Load one run.
    async fn get_run(&self, run_id: &str) -> Option<Run>;

    /// Task registry for discovery.
    fn registry(&self) -> &TaskRegistry;

    /// Count jobs.
    async fn count_jobs(&self, status_filter: Option<JobStatus>) -> u64;

    /// Count runs.
    async fn count_runs(&self, job_id_filter: Option<&str>) -> u64;

    /// Count runs since timestamp.
    async fn count_runs_since(&self, since: chrono::DateTime<chrono::Utc>) -> u64;

    /// Count jobs for one task.
    async fn count_jobs_for_task(&self, task_name: &str, status: Option<JobStatus>) -> u64;

    /// Aggregate run stats for one task.
    async fn task_run_stats(&self, task_name: &str) -> TaskRunStats;

    /// When this coordinator wraps upstream [`Boson`], return the runtime handle for Axum.
    fn as_boson_runtime(&self) -> Option<std::sync::Arc<Boson>> {
        None
    }
}
