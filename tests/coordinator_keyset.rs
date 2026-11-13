//! Keyset paging contracts: `list_jobs_after` / `list_runs_after` through the real adapter on the
//! mem backend, and the trait's default bodies over a fake that only implements offset reads.
//!
//! Test-only fixtures below are intentionally undocumented; this binary target is exempt from
//! the library's `missing_docs = "deny"` lint (see `Cargo.toml`).
#![allow(missing_docs)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use boson_backend_mem::MemQueueBackend;
use boson_coordinator::{
    BosonCoordinatorBackend, CoordinatorAdapter, JobKey, RunKey, DEFAULT_KEYSET_SCAN_CAP,
};
use boson_core::{
    BosonError, ExecutionContext, Job, JobStatus, JsonExecutionContextFactory, Run, TaskConfig,
    TaskRunStats,
};
use boson_runtime::{Boson, TaskRegistry};
use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::json;

// Unlimited so the fixtures can enqueue thousands of jobs in one burst.
#[boson_macros::task(name = "keyset_echo", max_in_flight = 0, max_enqueue_per_second = 0)]
async fn keyset_echo(_ctx: Box<dyn ExecutionContext>) -> boson_core::Result<()> {
    Ok(())
}

const PAGE: usize = 100;

/// Fresh mem backend plus a runtime over it; returns the runtime and its adapter.
///
/// The backend is passed to the builder directly (not through the process-global
/// `QueueRouter`, which installs once) so each test counts only its own jobs.
fn mem_adapter() -> (Arc<Boson>, Arc<dyn BosonCoordinatorBackend>) {
    let boson = Arc::new(
        Boson::builder()
            .queue_backend(Arc::new(MemQueueBackend::new()))
            .execution_context_factory(JsonExecutionContextFactory)
            .auto_registry()
            .build()
            .unwrap(),
    );
    let adapter: Arc<dyn BosonCoordinatorBackend> =
        Arc::new(CoordinatorAdapter::new(Arc::clone(&boson)));
    (boson, adapter)
}

async fn enqueue_n(backend: &dyn BosonCoordinatorBackend, n: usize) -> Vec<String> {
    let mut ids = Vec::with_capacity(n);
    for _ in 0..n {
        let id = backend
            .enqueue("keyset_echo", json!({}), json!({}), None)
            .await
            .expect("enqueue");
        ids.push(id);
    }
    ids
}

/// Every third job is claimed (running), every fifth canceled, the rest stay queued.
async fn mix_statuses(boson: &Boson, backend: &dyn BosonCoordinatorBackend, ids: &[String]) {
    let queue = boson.queue_backend();
    for (i, id) in ids.iter().enumerate() {
        if i.is_multiple_of(5) {
            backend.cancel_job(id).await.expect("cancel");
        } else if i.is_multiple_of(3) {
            assert!(queue.try_claim_job(id).await.expect("claim").is_some());
        }
    }
}

/// Page `list_jobs_after` to the end, calling `mid_scan` once after the second page.
async fn scan_jobs<F, Fut>(
    backend: &dyn BosonCoordinatorBackend,
    status: Option<JobStatus>,
    mut mid_scan: F,
) -> Vec<Job>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut seen = Vec::new();
    let mut cursor: Option<JobKey> = None;
    let mut pages = 0_u32;
    loop {
        let page = backend.list_jobs_after(status, cursor.as_ref(), PAGE).await;
        assert!(page.len() <= PAGE);
        let Some(last) = page.last() else { break };
        cursor = Some(JobKey::from(last));
        seen.extend(page);
        pages += 1;
        if pages == 2 {
            mid_scan().await;
        }
    }
    seen
}

fn assert_jobs_ordered(jobs: &[Job]) {
    for pair in jobs.windows(2) {
        let (a, b) = (&pair[0], &pair[1]);
        assert!(
            (a.created_at, a.job_id.as_str()) < (b.created_at, b.job_id.as_str()),
            "out of order: {} then {}",
            a.job_id,
            b.job_id
        );
    }
}

fn ids_of(jobs: &[Job]) -> Vec<String> {
    jobs.iter().map(|j| j.job_id.clone()).collect()
}

#[tokio::test]
async fn adapter_list_jobs_after_pages_every_job_once_in_order() {
    let (boson, backend) = mem_adapter();
    let ids = enqueue_n(backend.as_ref(), 2_500).await;
    mix_statuses(&boson, backend.as_ref(), &ids).await;

    let all = scan_jobs(backend.as_ref(), None, || async {}).await;
    assert_jobs_ordered(&all);
    assert_eq!(all.len(), ids.len());
    let seen: HashSet<String> = ids_of(&all).into_iter().collect();
    assert_eq!(seen, ids.iter().cloned().collect::<HashSet<_>>());

    for status in [JobStatus::Queued, JobStatus::Running, JobStatus::Canceled] {
        let filtered = scan_jobs(backend.as_ref(), Some(status), || async {}).await;
        assert_jobs_ordered(&filtered);
        assert!(filtered.iter().all(|j| j.status == status));
        let expected = all.iter().filter(|j| j.status == status).count();
        assert!(expected > 0, "fixture has no {status:?} jobs");
        assert_eq!(filtered.len(), expected);
        assert_eq!(
            filtered.len() as u64,
            backend.count_jobs(Some(status)).await
        );
    }
}

#[tokio::test]
async fn adapter_list_jobs_after_returns_mid_scan_enqueues_once_at_end() {
    let (_boson, backend) = mem_adapter();
    let before = enqueue_n(backend.as_ref(), 450).await;
    let added: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let (added_ref, backend_ref) = (&added, backend.as_ref());

    let all = scan_jobs(backend_ref, None, || async move {
        let ids = enqueue_n(backend_ref, 30).await;
        added_ref.lock().unwrap().extend(ids);
    })
    .await;

    let added = added.into_inner().unwrap();
    assert_eq!(added.len(), 30);
    assert_jobs_ordered(&all);
    assert_eq!(all.len(), before.len() + added.len());
    let seen = ids_of(&all);
    let unique: HashSet<&String> = seen.iter().collect();
    assert_eq!(unique.len(), seen.len(), "a job appeared twice");
    let tail: HashSet<&String> = seen[before.len()..].iter().collect();
    assert_eq!(tail, added.iter().collect::<HashSet<_>>());
}

#[tokio::test]
async fn adapter_list_jobs_after_zero_limit_is_empty() {
    let (_boson, backend) = mem_adapter();
    enqueue_n(backend.as_ref(), 3).await;
    assert!(backend.list_jobs_after(None, None, 0).await.is_empty());
}

fn base() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
}

/// Runs for jobs `job-a` / `job-b`; groups of three share `started_at` to exercise the tie-break.
fn fixture_runs(n: usize) -> Vec<Run> {
    (0..n)
        .map(|i| {
            let job = if i.is_multiple_of(2) {
                "job-a"
            } else {
                "job-b"
            };
            let mut run = Run::new(job, "keyset_echo", 1);
            run.run_id = format!("run-{i:06}");
            run.started_at = base() + Duration::milliseconds(i64::try_from(i / 3).unwrap());
            run
        })
        .collect()
}

async fn scan_runs(backend: &dyn BosonCoordinatorBackend, job: Option<&str>) -> Vec<Run> {
    let mut seen = Vec::new();
    let mut cursor: Option<RunKey> = None;
    loop {
        let page = backend.list_runs_after(job, cursor.as_ref(), PAGE).await;
        assert!(page.len() <= PAGE);
        let Some(last) = page.last() else { break };
        cursor = Some(RunKey::from(last));
        seen.extend(page);
    }
    seen
}

/// Runs expected from a full scan: filtered, `started_at DESC, run_id DESC`.
fn expected_run_ids(runs: &[Run], job: Option<&str>) -> Vec<String> {
    let mut want: Vec<&Run> = runs
        .iter()
        .filter(|r| job.is_none_or(|j| r.job_id == j))
        .collect();
    want.sort_by(|a, b| (b.started_at, &b.run_id).cmp(&(a.started_at, &a.run_id)));
    want.into_iter().map(|r| r.run_id.clone()).collect()
}

fn run_ids(runs: &[Run]) -> Vec<String> {
    runs.iter().map(|r| r.run_id.clone()).collect()
}

#[tokio::test]
async fn adapter_list_runs_after_pages_newest_first() {
    let (boson, backend) = mem_adapter();
    let runs = fixture_runs(750);
    let queue = boson.queue_backend();
    for run in &runs {
        queue.upsert_run(run).await.expect("upsert run");
    }

    for job in [None, Some("job-a"), Some("job-b")] {
        let got = scan_runs(backend.as_ref(), job).await;
        assert_eq!(
            run_ids(&got),
            expected_run_ids(&runs, job),
            "filter {job:?}"
        );
    }
    assert!(backend.list_runs_after(None, None, 0).await.is_empty());
}

/// Implements only the offset reads; keyset reads come from the trait's default bodies.
struct OffsetOnlyBackend {
    registry: TaskRegistry,
    jobs: Vec<Job>,
    runs: Vec<Run>,
}

/// Jobs in shuffled insertion order with shared `created_at` groups and mixed statuses.
fn fixture_jobs(n: usize) -> Vec<Job> {
    let statuses = [JobStatus::Queued, JobStatus::Running, JobStatus::Success];
    let mut jobs: Vec<Job> = (0..n)
        .map(|i| {
            let mut j = Job::new("keyset_echo", json!({}), json!({}), 0, "global", 0, None);
            j.job_id = format!("job-{i:06}");
            j.created_at = base() + Duration::milliseconds(i64::try_from(i / 3).unwrap());
            j.status = statuses[i % statuses.len()];
            j
        })
        .collect();
    // Deterministic shuffle so the default body cannot rely on the offset order.
    jobs.sort_by_key(|j| j.job_id.bytes().rev().collect::<Vec<u8>>());
    jobs
}

impl OffsetOnlyBackend {
    fn new(jobs: usize, runs: usize) -> Self {
        Self {
            registry: TaskRegistry::auto_discover(),
            jobs: fixture_jobs(jobs),
            runs: fixture_runs(runs),
        }
    }
}

#[async_trait]
impl BosonCoordinatorBackend for OffsetOnlyBackend {
    async fn enqueue(
        &self,
        _task_name: &str,
        _actor_json: serde_json::Value,
        _params_json: serde_json::Value,
        _idempotency_key: Option<String>,
    ) -> boson_core::Result<String> {
        Err(BosonError::internal("not implemented"))
    }

    async fn get_job(&self, _job_id: &str) -> Option<Job> {
        None
    }

    async fn list_jobs(
        &self,
        status_filter: Option<JobStatus>,
        offset: usize,
        limit: usize,
    ) -> Vec<Job> {
        self.jobs
            .iter()
            .filter(|j| status_filter.is_none_or(|s| j.status == s))
            .skip(offset)
            .take(limit)
            .cloned()
            .collect()
    }

    async fn cancel_job(&self, _job_id: &str) -> boson_core::Result<()> {
        Err(BosonError::internal("not implemented"))
    }

    async fn get_task_config(&self, _task_name: &str) -> boson_core::Result<TaskConfig> {
        Err(BosonError::internal("not implemented"))
    }

    async fn upsert_task_config(&self, _config: TaskConfig) -> boson_core::Result<()> {
        Err(BosonError::internal("not implemented"))
    }

    async fn list_runs(
        &self,
        job_id_filter: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Vec<Run> {
        self.runs
            .iter()
            .filter(|r| job_id_filter.is_none_or(|j| r.job_id == j))
            .skip(offset)
            .take(limit)
            .cloned()
            .collect()
    }

    async fn get_run(&self, _run_id: &str) -> Option<Run> {
        None
    }

    fn registry(&self) -> &TaskRegistry {
        &self.registry
    }

    async fn count_jobs(&self, _status_filter: Option<JobStatus>) -> u64 {
        0
    }

    async fn count_runs(&self, _job_id_filter: Option<&str>) -> u64 {
        0
    }

    async fn count_runs_since(&self, _since: DateTime<Utc>) -> u64 {
        0
    }

    async fn count_jobs_for_task(&self, _task_name: &str, _status: Option<JobStatus>) -> u64 {
        0
    }

    async fn task_run_stats(&self, _task_name: &str) -> TaskRunStats {
        TaskRunStats {
            runs_total: 0,
            success_count: 0,
        }
    }
}

#[tokio::test]
async fn default_list_jobs_after_pages_offset_only_backend() {
    let backend = OffsetOnlyBackend::new(2_500, 0);

    let all = scan_jobs(&backend, None, || async {}).await;
    assert_jobs_ordered(&all);
    assert_eq!(all.len(), 2_500);

    let success = scan_jobs(&backend, Some(JobStatus::Success), || async {}).await;
    assert_jobs_ordered(&success);
    assert!(success.iter().all(|j| j.status == JobStatus::Success));
    let expected = backend
        .jobs
        .iter()
        .filter(|j| j.status == JobStatus::Success)
        .count();
    assert_eq!(success.len(), expected);
    assert!(backend.list_jobs_after(None, None, 0).await.is_empty());
}

#[tokio::test]
async fn default_list_runs_after_pages_offset_only_backend() {
    let backend = OffsetOnlyBackend::new(0, 750);
    for job in [None, Some("job-a")] {
        let got = scan_runs(&backend, job).await;
        assert_eq!(run_ids(&got), expected_run_ids(&backend.runs, job));
    }
    assert!(backend.list_runs_after(None, None, 0).await.is_empty());
}

#[tokio::test]
async fn default_list_jobs_after_stops_at_scan_cap() {
    let backend = OffsetOnlyBackend::new(DEFAULT_KEYSET_SCAN_CAP + 50, 0);
    let page = backend
        .list_jobs_after(None, None, DEFAULT_KEYSET_SCAN_CAP + 50)
        .await;
    assert_eq!(page.len(), DEFAULT_KEYSET_SCAN_CAP);
    assert_jobs_ordered(&page);
}
