//! Coordinator contracts without optional features: adapter workflow, stats, task-config bootstrap.
//!
//! Test-only fixtures below are intentionally undocumented; this binary target is exempt from
//! the library's `missing_docs = "deny"` lint (see `Cargo.toml`).
#![allow(missing_docs)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::await_holding_lock)]

use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use boson_backend_mem::MemQueueBackend;
use boson_coordinator::stats::count_queued_jobs;
use boson_coordinator::{
    ensure_default_task_configs_embedded, ensure_default_task_configs_embedded_with_skip,
    BosonCoordinatorBackend, CoordinatorAdapter,
};
use boson_core::{
    BosonError, ExecutionContext, Job, JobStatus, JsonExecutionContextFactory, QueueRouter, Run,
    TaskConfig, TaskRunStats,
};
use boson_runtime::{Boson, TaskRegistry};
use chrono::Utc;
use serde_json::json;

#[boson_macros::task(name = "coord_echo")]
async fn coord_echo(_ctx: Box<dyn ExecutionContext>) -> boson_core::Result<()> {
    Ok(())
}

/// Serialize tests that install a process-global mem queue (parallel enqueue races otherwise).
fn queue_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn install_mem_backend() {
    let backend: Arc<dyn boson_core::QueueBackend> = Arc::new(MemQueueBackend::new());
    QueueRouter::set_global(QueueRouter::with_default(backend));
}

fn test_boson() -> Boson {
    Boson::builder()
        .queue_backend_from_global()
        .execution_context_factory(JsonExecutionContextFactory)
        .auto_registry()
        .build()
        .unwrap()
}

fn test_adapter() -> Arc<dyn BosonCoordinatorBackend> {
    install_mem_backend();
    Arc::new(CoordinatorAdapter::new(Arc::new(test_boson())))
}

/// Adapter tests share `QueueRouter` global state — take this for the whole test body.
fn lock_queue() -> std::sync::MutexGuard<'static, ()> {
    queue_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Stub backend that rejects every upsert (sad path for task-config bootstrap).
struct FailUpsertBackend {
    registry: TaskRegistry,
}

/// Records upserted task names (skip-list contract without global queue state).
struct RecordingUpsertBackend {
    registry: TaskRegistry,
    upserted: Mutex<Vec<String>>,
}

#[async_trait]
impl BosonCoordinatorBackend for FailUpsertBackend {
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
        _status_filter: Option<JobStatus>,
        _offset: usize,
        _limit: usize,
    ) -> Vec<Job> {
        vec![]
    }

    async fn cancel_job(&self, _job_id: &str) -> boson_core::Result<()> {
        Err(BosonError::internal("not implemented"))
    }

    async fn get_task_config(&self, _task_name: &str) -> boson_core::Result<TaskConfig> {
        Err(BosonError::internal("not implemented"))
    }

    async fn upsert_task_config(&self, _config: TaskConfig) -> boson_core::Result<()> {
        Err(BosonError::internal("upsert denied"))
    }

    async fn list_runs(
        &self,
        _job_id_filter: Option<&str>,
        _offset: usize,
        _limit: usize,
    ) -> Vec<Run> {
        vec![]
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

    async fn count_runs_since(&self, _since: chrono::DateTime<Utc>) -> u64 {
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

#[async_trait]
impl BosonCoordinatorBackend for RecordingUpsertBackend {
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
        _status_filter: Option<JobStatus>,
        _offset: usize,
        _limit: usize,
    ) -> Vec<Job> {
        vec![]
    }

    async fn cancel_job(&self, _job_id: &str) -> boson_core::Result<()> {
        Err(BosonError::internal("not implemented"))
    }

    async fn get_task_config(&self, _task_name: &str) -> boson_core::Result<TaskConfig> {
        Err(BosonError::internal("not implemented"))
    }

    async fn upsert_task_config(&self, config: TaskConfig) -> boson_core::Result<()> {
        self.upserted
            .lock()
            .expect("upserted lock")
            .push(config.task_name);
        Ok(())
    }

    async fn list_runs(
        &self,
        _job_id_filter: Option<&str>,
        _offset: usize,
        _limit: usize,
    ) -> Vec<Run> {
        vec![]
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

    async fn count_runs_since(&self, _since: chrono::DateTime<Utc>) -> u64 {
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
async fn adapter_enqueue_list_count_cancel_workflow() {
    let _guard = lock_queue();
    let backend = test_adapter();
    assert!(backend.registry().list().contains(&"coord_echo"));

    let job_id = backend
        .enqueue(
            "coord_echo",
            json!({"System": {"operation": "workflow"}}),
            json!({}),
            None,
        )
        .await
        .expect("enqueue");

    let listed = backend.list_jobs(Some(JobStatus::Queued), 0, 50).await;
    assert!(listed.iter().any(|j| j.job_id == job_id));

    let depth = count_queued_jobs(backend.as_ref()).await;
    assert!(depth >= 1);

    let counted = backend
        .count_jobs_for_task("coord_echo", Some(JobStatus::Queued))
        .await;
    assert!(counted >= 1);

    backend.cancel_job(&job_id).await.expect("cancel");
    let job = backend.get_job(&job_id).await.expect("job present");
    assert_eq!(job.status, JobStatus::Canceled);
}

#[tokio::test]
async fn adapter_cancel_missing_job_is_not_found() {
    let _guard = lock_queue();
    let backend = test_adapter();
    let err = backend.cancel_job("missing-job-id").await.unwrap_err();
    assert!(matches!(err, BosonError::JobNotFound(_)));
}

#[tokio::test]
async fn count_queued_jobs_tracks_enqueue() {
    let _guard = lock_queue();
    let backend = test_adapter();
    let before = count_queued_jobs(backend.as_ref()).await;
    backend
        .enqueue("coord_echo", json!({}), json!({}), None)
        .await
        .expect("enqueue");
    let after = count_queued_jobs(backend.as_ref()).await;
    assert_eq!(after, before + 1);
}

#[tokio::test]
async fn ensure_default_task_configs_upserts_registered_task() {
    let _guard = lock_queue();
    let backend = test_adapter();
    ensure_default_task_configs_embedded(Arc::clone(&backend))
        .await
        .expect("bootstrap");
    let cfg = backend.get_task_config("coord_echo").await.expect("config");
    assert_eq!(cfg.task_name, "coord_echo");
}

#[tokio::test]
async fn ensure_default_task_configs_skip_omits_listed_tasks() {
    let concrete = Arc::new(RecordingUpsertBackend {
        registry: TaskRegistry::auto_discover(),
        upserted: Mutex::new(Vec::new()),
    });
    let backend: Arc<dyn BosonCoordinatorBackend> = Arc::clone(&concrete) as _;
    ensure_default_task_configs_embedded_with_skip(backend, &["coord_echo"])
        .await
        .expect("bootstrap with skip");
    let upserted = concrete.upserted.lock().expect("upserted lock").clone();
    assert!(
        !upserted.iter().any(|n| n == "coord_echo"),
        "coord_echo should be skipped, got {upserted:?}"
    );
}

#[tokio::test]
async fn ensure_default_task_configs_maps_upsert_failure() {
    let backend: Arc<dyn BosonCoordinatorBackend> = Arc::new(FailUpsertBackend {
        registry: TaskRegistry::auto_discover(),
    });
    let err = ensure_default_task_configs_embedded(backend)
        .await
        .expect_err("upsert must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("ensure boson task config") && msg.contains("upsert denied"),
        "unexpected error: {msg}"
    );
}
