//! Integration tests: task macro, enqueue, list jobs, adapter, and Axum bridge.
//!
//! Test-only fixtures below are intentionally undocumented; this binary target is exempt from
//! the library's `missing_docs = "deny"` lint (see `Cargo.toml`).
#![allow(missing_docs)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use axum::{
    body::Body,
    extract::FromRef,
    http::{Request, StatusCode},
    Router,
};
use boson_backend_mem::MemQueueBackend;
use boson_coordinator::axum_api::{
    boson_router, AllowAllAdminAuth, BosonState, StaticTokenAdminAuth, NEST_PATH,
};
use boson_coordinator::{BosonCoordinatorBackend, CoordinatorAdapter};
use boson_core::{
    BosonError, ExecutionContext, JobStatus, JsonExecutionContextFactory, QueueRouter,
};
use boson_runtime::{configure, default, Boson};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

#[boson_macros::task(name = "test_echo")]
async fn test_echo(_ctx: Box<dyn ExecutionContext>) -> boson_core::Result<()> {
    Ok(())
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

#[derive(Clone)]
struct AppState {
    boson: BosonState,
}

impl FromRef<AppState> for boson_axum::BosonState {
    fn from_ref(app: &AppState) -> Self {
        app.boson.inner_axum()
    }
}

struct HttpTestApp {
    router: Router,
}

impl HttpTestApp {
    fn new() -> Self {
        install_mem_backend();
        let boson = Arc::new(test_boson());
        let state = AppState {
            boson: BosonState::builder(Arc::clone(&boson))
                .admin_auth(Arc::new(AllowAllAdminAuth))
                .require_admin_auth(true)
                .build()
                .expect("require_admin with AllowAllAdminAuth"),
        };
        let router = Router::new()
            .nest(NEST_PATH, boson_router::<AppState>())
            .with_state(state);
        Self { router }
    }

    async fn request(&self, req: Request<Body>) -> (StatusCode, Value) {
        let response = self.router.clone().oneshot(req).await.expect("oneshot");
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        let json: Value = serde_json::from_slice(&body).expect("json body");
        (status, json)
    }
}

fn enqueue_http_request(task_name: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(format!("{NEST_PATH}/jobs/enqueue"))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "task_name": task_name,
                "params": {},
            })
            .to_string(),
        ))
        .expect("request")
}

#[tokio::test]
async fn test_boson_build_and_enqueue_unknown_fails() {
    install_mem_backend();
    let boson = test_boson();
    let err = boson
        .enqueue(
            "unknown_task",
            serde_json::json!({}),
            serde_json::json!({}),
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, BosonError::TaskNotFound(_)));
}

#[tokio::test]
async fn test_boson_registry_includes_macro_task() {
    install_mem_backend();
    let boson = test_boson();
    let list = boson.registry().list();
    assert!(list.contains(&"test_echo"));
}

#[tokio::test]
async fn test_enqueue_and_list_jobs() {
    install_mem_backend();
    configure(test_boson());
    let boson = default().unwrap();
    let job_id = boson
        .enqueue(
            "test_echo",
            serde_json::json!({"System": {"operation": "test"}}),
            serde_json::json!({}),
            None,
        )
        .await
        .unwrap();
    assert_ne!(job_id, "");
    let jobs = boson.registry().list();
    assert!(jobs.contains(&"test_echo"));
    let listed = boson
        .list_jobs(Some(JobStatus::Queued), 0, 10)
        .await
        .unwrap();
    assert!(listed.iter().any(|j| j.job_id == job_id));
}

#[tokio::test]
async fn adapter_enqueue_and_get_job() {
    let backend = test_adapter();
    let job_id = backend
        .enqueue(
            "test_echo",
            json!({"System": {"operation": "adapter"}}),
            json!({}),
            None,
        )
        .await
        .expect("enqueue");
    let job = backend.get_job(&job_id).await.expect("job present");
    assert_eq!(job.job_id, job_id);
    assert_eq!(job.task_name, "test_echo");
    assert_eq!(job.status, JobStatus::Queued);
}

#[tokio::test]
async fn adapter_enqueue_unknown_task_fails() {
    let backend = test_adapter();
    let err = backend
        .enqueue("missing_task", json!({}), json!({}), None)
        .await
        .unwrap_err();
    assert!(matches!(err, BosonError::TaskNotFound(_)));
}

#[tokio::test]
async fn adapter_get_job_missing_returns_none() {
    let backend = test_adapter();
    assert!(backend.get_job("no-such-job").await.is_none());
}

#[tokio::test]
async fn axum_enqueue_returns_job_id() {
    let app = HttpTestApp::new();
    let (status, body) = app.request(enqueue_http_request("test_echo")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    let job_id = body["data"]["job_id"].as_str().expect("job_id");
    assert_ne!(job_id, "");
}

#[tokio::test]
async fn axum_enqueue_unknown_task_is_bad_request() {
    let app = HttpTestApp::new();
    let (status, body) = app.request(enqueue_http_request("missing_task")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["success"], false);
    assert!(body["error"].as_str().is_some());
}

#[tokio::test]
async fn axum_get_job_not_found() {
    let app = HttpTestApp::new();
    let (status, body) = app
        .request(
            Request::builder()
                .uri(format!("{NEST_PATH}/jobs/missing-id"))
                .body(Body::empty())
                .expect("request"),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["success"], false);
}

#[tokio::test]
async fn boson_state_from_adapter_runtime() {
    let backend = test_adapter();
    let state = BosonState::new(backend).expect("adapter-backed runtime");
    let _ = state.inner_axum();
}

#[tokio::test]
async fn axum_enqueue_then_get_job_roundtrip() {
    let app = HttpTestApp::new();
    let (status, body) = app.request(enqueue_http_request("test_echo")).await;
    assert_eq!(status, StatusCode::OK);
    let job_id = body["data"]["job_id"].as_str().expect("job_id").to_string();

    let (get_status, get_body) = app
        .request(
            Request::builder()
                .uri(format!("{NEST_PATH}/jobs/{job_id}"))
                .body(Body::empty())
                .expect("request"),
        )
        .await;
    assert_eq!(get_status, StatusCode::OK);
    assert_eq!(get_body["success"], true);
    assert_eq!(get_body["data"]["job_id"], job_id);
    assert_eq!(get_body["data"]["task_name"], "test_echo");
}

/// Remote-shaped backend: no `as_boson_runtime`, so [`BosonState::new`] must reject it.
struct NoRuntimeBackend;

#[async_trait::async_trait]
impl BosonCoordinatorBackend for NoRuntimeBackend {
    async fn enqueue(
        &self,
        _task_name: &str,
        _actor_json: serde_json::Value,
        _params_json: serde_json::Value,
        _idempotency_key: Option<String>,
    ) -> boson_core::Result<String> {
        Err(BosonError::internal("not implemented"))
    }

    async fn get_job(&self, _job_id: &str) -> Option<boson_core::Job> {
        None
    }

    async fn list_jobs(
        &self,
        _status_filter: Option<JobStatus>,
        _offset: usize,
        _limit: usize,
    ) -> Vec<boson_core::Job> {
        vec![]
    }

    async fn cancel_job(&self, _job_id: &str) -> boson_core::Result<()> {
        Err(BosonError::internal("not implemented"))
    }

    async fn get_task_config(
        &self,
        _task_name: &str,
    ) -> boson_core::Result<boson_core::TaskConfig> {
        Err(BosonError::internal("not implemented"))
    }

    async fn upsert_task_config(&self, _config: boson_core::TaskConfig) -> boson_core::Result<()> {
        Err(BosonError::internal("not implemented"))
    }

    async fn list_runs(
        &self,
        _job_id_filter: Option<&str>,
        _offset: usize,
        _limit: usize,
    ) -> Vec<boson_core::Run> {
        vec![]
    }

    async fn get_run(&self, _run_id: &str) -> Option<boson_core::Run> {
        None
    }

    fn registry(&self) -> &boson_runtime::TaskRegistry {
        static REG: std::sync::OnceLock<boson_runtime::TaskRegistry> = std::sync::OnceLock::new();
        REG.get_or_init(boson_runtime::TaskRegistry::auto_discover)
    }

    async fn count_jobs(&self, _status_filter: Option<JobStatus>) -> u64 {
        0
    }

    async fn count_runs(&self, _job_id_filter: Option<&str>) -> u64 {
        0
    }

    async fn count_runs_since(&self, _since: chrono::DateTime<chrono::Utc>) -> u64 {
        0
    }

    async fn count_jobs_for_task(&self, _task_name: &str, _status: Option<JobStatus>) -> u64 {
        0
    }

    async fn task_run_stats(&self, _task_name: &str) -> boson_core::TaskRunStats {
        boson_core::TaskRunStats {
            runs_total: 0,
            success_count: 0,
        }
    }
}

#[tokio::test]
async fn boson_state_new_rejects_non_adapter_backend() {
    match BosonState::new(Arc::new(NoRuntimeBackend)) {
        Ok(_) => panic!("expected non-adapter backend to be rejected"),
        Err(BosonError::Internal { message, .. }) => {
            assert!(message.contains("CoordinatorAdapter-backed runtime"));
        }
        Err(other) => panic!("expected Internal, got {other:?}"),
    }
}

#[tokio::test]
async fn boson_state_builder_default_fail_closed_at_request_time() {
    install_mem_backend();
    std::env::remove_var(boson_coordinator::axum_api::OPEN_LAB_MODE_ENV);
    let boson = Arc::new(test_boson());
    let state = AppState {
        boson: BosonState::builder(Arc::clone(&boson))
            .build()
            .expect("default builder succeeds; auth enforced at request time"),
    };
    let router = Router::new()
        .nest(NEST_PATH, boson_router::<AppState>())
        .with_state(state);
    let response = router
        .oneshot(enqueue_http_request("test_echo"))
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn axum_default_mount_denies_unauthenticated() {
    install_mem_backend();
    std::env::remove_var(boson_coordinator::axum_api::OPEN_LAB_MODE_ENV);
    let boson = Arc::new(test_boson());
    let state = AppState {
        boson: BosonState::from_runtime(boson),
    };
    let router = Router::new()
        .nest(NEST_PATH, boson_router::<AppState>())
        .with_state(state);
    let response = router
        .oneshot(enqueue_http_request("test_echo"))
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn axum_open_lab_mode_allows_unauthenticated() {
    install_mem_backend();
    std::env::set_var(boson_coordinator::axum_api::OPEN_LAB_MODE_ENV, "1");
    let boson = Arc::new(test_boson());
    let state = AppState {
        boson: BosonState::from_runtime(boson),
    };
    let router = Router::new()
        .nest(NEST_PATH, boson_router::<AppState>())
        .with_state(state);
    let response = router
        .oneshot(enqueue_http_request("test_echo"))
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::OK);
    std::env::remove_var(boson_coordinator::axum_api::OPEN_LAB_MODE_ENV);
}

#[tokio::test]
async fn axum_require_admin_rejects_missing_token() {
    install_mem_backend();
    let boson = Arc::new(test_boson());
    let state = AppState {
        boson: BosonState::builder(boson)
            .admin_auth(Arc::new(StaticTokenAdminAuth::new("lab-secret")))
            .require_admin_auth(true)
            .build()
            .expect("token auth"),
    };
    let router = Router::new()
        .nest(NEST_PATH, boson_router::<AppState>())
        .with_state(state);
    let response = router
        .oneshot(enqueue_http_request("test_echo"))
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}
