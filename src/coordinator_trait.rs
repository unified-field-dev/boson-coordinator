//! Object-safe coordinator surface for local and distributed Boson backends.

use std::future::Future;

use async_trait::async_trait;

use boson_core::{
    page_jobs_after, page_runs_after, Job, JobKey, JobStatus, Result, Run, RunKey, TaskConfig,
    TaskRunStats,
};
use boson_runtime::{Boson, TaskRegistry};

/// Most rows the default keyset bodies read through the offset methods per call.
///
/// [`BosonCoordinatorBackend::list_jobs_after`] and [`BosonCoordinatorBackend::list_runs_after`]
/// fall back to [`BosonCoordinatorBackend::list_jobs`] / [`BosonCoordinatorBackend::list_runs`]
/// when an implementation does not override them. Those fallbacks read at most this many matching
/// rows (in the offset methods' order) and page within them, so rows past the cap never appear.
pub const DEFAULT_KEYSET_SCAN_CAP: usize = 10_000;

/// Rows requested per offset call while the default keyset bodies scan.
const DEFAULT_KEYSET_SCAN_CHUNK: usize = 500;

/// Collect rows from an offset-paged source, `DEFAULT_KEYSET_SCAN_CHUNK` at a time, up to
/// [`DEFAULT_KEYSET_SCAN_CAP`]. A short chunk ends the scan.
async fn scan_offset_pages<T, F, Fut>(mut fetch: F) -> Vec<T>
where
    F: FnMut(usize, usize) -> Fut + Send,
    Fut: Future<Output = Vec<T>> + Send,
    T: Send,
{
    let mut all = Vec::new();
    while all.len() < DEFAULT_KEYSET_SCAN_CAP {
        let want = DEFAULT_KEYSET_SCAN_CHUNK.min(DEFAULT_KEYSET_SCAN_CAP - all.len());
        let chunk = fetch(all.len(), want).await;
        let short = chunk.len() < want;
        all.extend(chunk);
        if short {
            break;
        }
    }
    all.truncate(DEFAULT_KEYSET_SCAN_CAP);
    all
}

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
///
/// # Errors
///
/// Methods returning [`Result`] propagate [`boson_core::BosonError`] from the backing runtime
/// or HTTP client (enqueue validation, persistence, cancel races). Methods returning `Option`
/// use `None` for not-found jobs or runs without an error.
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

    /// List up to `limit` jobs that sort strictly after `after`, oldest first.
    ///
    /// Order is `created_at ASC, job_id ASC`. A job comes after the key when its `created_at` is
    /// later than `after.created_at`, or equal with a greater `job_id`, so two jobs created in the
    /// same instant still page in a fixed order. `after: None` starts at the oldest job.
    /// `status_filter` narrows the scan to one status.
    ///
    /// To page, pass `JobKey::from(last)` for the last job of each non-empty page; an empty page
    /// means the scan is done. Each job appears once across the pages, with no `OFFSET` drift when
    /// jobs are enqueued mid-scan: a new job's `created_at` is the enqueue time, so it sorts after
    /// every key already returned and shows up at the end of the scan. A job whose status changes
    /// mid-scan keeps its position; with a `status_filter`, it shows up only if it matches when
    /// its page is read. See [`boson_core::QueueBackend::list_jobs_after`] for engine-specific
    /// `job_id` ordering.
    ///
    /// Backend errors yield an empty page, like [`Self::list_jobs`].
    ///
    /// The default body reads matching jobs through [`Self::list_jobs`] in chunks, up to
    /// [`DEFAULT_KEYSET_SCAN_CAP`] rows, and pages them with [`page_jobs_after`]. It is O(n) per
    /// page, silently omits rows past the cap, and inherits offset drift between its own chunks.
    /// Implementations backed by an ordered store should override it.
    async fn list_jobs_after(
        &self,
        status_filter: Option<JobStatus>,
        after: Option<&JobKey>,
        limit: usize,
    ) -> Vec<Job> {
        if limit == 0 {
            return Vec::new();
        }
        let all =
            scan_offset_pages(|offset, chunk| self.list_jobs(status_filter, offset, chunk)).await;
        page_jobs_after(all, status_filter, after, limit)
    }

    /// Cancel a job if still active.
    async fn cancel_job(&self, job_id: &str) -> Result<()>;

    /// Load task config (with registry defaults).
    async fn get_task_config(&self, task_name: &str) -> Result<TaskConfig>;

    /// Persist task config.
    async fn upsert_task_config(&self, config: TaskConfig) -> Result<()>;

    /// List runs.
    async fn list_runs(&self, job_id_filter: Option<&str>, offset: usize, limit: usize)
        -> Vec<Run>;

    /// List up to `limit` runs that sort strictly after `after`, newest first.
    ///
    /// Order is `started_at DESC, run_id DESC`. A run comes after the key when its `started_at` is
    /// earlier than `after.started_at`, or equal with a smaller `run_id`, so two runs started in
    /// the same instant still page in a fixed order. `after: None` starts at the newest run.
    /// `job_id_filter` narrows the scan to one job's runs.
    ///
    /// To page, pass `RunKey::from(last)` for the last run of each non-empty page; an empty page
    /// means the scan is done. Each run appears once across the pages. A run that starts mid-scan
    /// has a later `started_at` than every key already returned, so it sorts before the cursor and
    /// the scan does not return it; start a new scan with `after: None` to see it. This order is
    /// newest first even where [`Self::list_runs`] returns oldest first.
    ///
    /// Backend errors yield an empty page, like [`Self::list_runs`].
    ///
    /// The default body reads matching runs through [`Self::list_runs`] in chunks, up to
    /// [`DEFAULT_KEYSET_SCAN_CAP`] rows, and pages them with [`page_runs_after`]. It is O(n) per
    /// page, silently omits rows past the cap, and inherits offset drift between its own chunks.
    /// Implementations backed by an ordered store should override it.
    async fn list_runs_after(
        &self,
        job_id_filter: Option<&str>,
        after: Option<&RunKey>,
        limit: usize,
    ) -> Vec<Run> {
        if limit == 0 {
            return Vec::new();
        }
        let all =
            scan_offset_pages(|offset, chunk| self.list_runs(job_id_filter, offset, chunk)).await;
        page_runs_after(all, job_id_filter, after, limit)
    }

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
