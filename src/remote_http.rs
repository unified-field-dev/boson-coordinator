//! HTTP client [`BosonCoordinatorBackend`] for split `server-apps`
//! → `boson-server` (`/api/boson/*`).
//!
//! Resolution:
//! 1. `BOSON_REMOTE_BASE_URL`
//! 2. `SUBSYSTEM_GATEWAY_BASE_URL` + `SUBSYSTEM_CELL_SLUG` (default `home`) → `{base}/cell/{cell}/sub/boson`
//!
//! [`TaskRegistry`] stays **in-process** (same binary as subsystem
//! workers) for `registry()`; job/run mutations go to the remote API.
//!
//! # Examples
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use boson_coordinator::BosonCoordinatorBackend;
//! use boson_coordinator::remote_http::build_remote_coordinator;
//!
//! let backend: Arc<dyn BosonCoordinatorBackend> = build_remote_coordinator()?;
//! let _job_id = backend
//!     .enqueue("send_email", serde_json::json!({}), serde_json::json!({}), None)
//!     .await?;
//! ```

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::coordinator_trait::BosonCoordinatorBackend;
use crate::remote_api_types::{
    EnqueueRequest, EnqueueResponse, JobResponse, RunResponse, TaskConfigResponse,
    UpdateTaskConfigRequest,
};
use boson_core::{BosonError, Result};
use boson_core::{Job, JobStatus, Run, RunStatus, TaskConfig, TaskRunStats};
use boson_runtime::TaskRegistry;

const DEFAULT_REMOTE_TIMEOUT_MS: u64 = 3000;

/// Resolve Boson HTTP base (path prefix before `/api/boson`).
///
/// Checks `BOSON_REMOTE_BASE_URL` first, then falls back to
/// `{SUBSYSTEM_GATEWAY_BASE_URL}/cell/{SUBSYSTEM_CELL_SLUG}/sub/boson` (cell slug defaults to
/// `home`). Returns `None` if neither is configured.
///
/// # Examples
///
/// ```rust,ignore
/// std::env::set_var("BOSON_REMOTE_BASE_URL", "https://gateway.example.com/");
/// let base = boson_coordinator::remote_http::resolve_boson_remote_base_url();
/// assert_eq!(base, Some("https://gateway.example.com".to_string()));
/// ```
pub fn resolve_boson_remote_base_url() -> Option<String> {
    if let Ok(u) = std::env::var("BOSON_REMOTE_BASE_URL") {
        let t = u.trim();
        if !t.is_empty() {
            return Some(trim_slash(t));
        }
    }
    let base = std::env::var("SUBSYSTEM_GATEWAY_BASE_URL").ok()?;
    let base = base.trim();
    if base.is_empty() {
        return None;
    }
    let cell = std::env::var("SUBSYSTEM_CELL_SLUG").unwrap_or_else(|_| "home".to_string());
    let cell = cell.trim();
    if cell.is_empty() {
        return None;
    }
    Some(format!("{}/cell/{}/sub/boson", trim_slash(base), cell))
}

fn trim_slash(s: &str) -> String {
    s.trim_end_matches('/').to_string()
}

fn remote_timeout() -> Duration {
    let ms = std::env::var("SUBSYSTEM_REMOTE_HTTP_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_REMOTE_TIMEOUT_MS);
    Duration::from_millis(ms.max(100))
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(remote_timeout())
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

fn boson_sign_path(api_path: &str) -> String {
    format!("/api/boson{}", api_path)
}

pub use soliton::subsystem_auth::{
    load_hmac_key_material_from_env, subsystem_hmac_header_pair, SUBSYSTEM_AUTH_HEADER_NAME,
};

/// Environment variable for HMAC key material (UTF-8, or `hex:` + hex bytes).
///
/// Same name as Soliton / fleet runtimes (`SUBSYSTEM_AUTH_HMAC_KEY`).
pub const SUBSYSTEM_HMAC_KEY_ENV: &str = "SUBSYSTEM_AUTH_HMAC_KEY";

fn add_subsystem_hmac(
    req: reqwest::RequestBuilder,
    method: &reqwest::Method,
    path_and_query: &str,
    body: &[u8],
) -> reqwest::RequestBuilder {
    match subsystem_hmac_header_pair(method.as_str(), path_and_query, body) {
        Some((name, tag)) => req.header(name, tag),
        None => req,
    }
}

#[derive(Debug, Deserialize)]
struct ApiEnvelope<T> {
    success: bool,
    data: Option<T>,
    error: Option<String>,
}

fn api_err<T>(env: ApiEnvelope<T>, ctx: &str) -> BosonError {
    BosonError::internal(format!(
        "{}: {}",
        ctx,
        env.error.unwrap_or_else(|| "unknown error".to_string())
    ))
}

/// Remote Boson coordinator (HTTP) with a local [`TaskRegistry`] for discovery.
///
/// Job/run mutations and reads go over HTTP to `{base_url}/api/boson/*`; `registry()` is served
/// from the `registry` passed at construction, so task discovery does not require a round trip.
/// Prefer [`build_remote_coordinator`] unless you need to construct the base URL or registry
/// yourself.
pub struct HttpRemoteBosonCoordinatorBackend {
    client: reqwest::Client,
    base_url: String,
    registry: Arc<TaskRegistry>,
}

impl HttpRemoteBosonCoordinatorBackend {
    /// Build a client against `base_url` (trailing slash optional), serving task discovery from
    /// `registry`.
    pub fn new(base_url: String, registry: Arc<TaskRegistry>) -> Self {
        Self {
            client: http_client(),
            base_url: trim_slash(&base_url),
            registry,
        }
    }

    fn api(&self, path: &str) -> String {
        format!("{}/api/boson{}", self.base_url, path)
    }

    async fn get_json<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T> {
        let url = self.api(path);
        let sign_path = boson_sign_path(path);
        let req = add_subsystem_hmac(
            self.client.get(&url),
            &reqwest::Method::GET,
            &sign_path,
            &[],
        );
        let resp = req
            .send()
            .await
            .map_err(|e| BosonError::internal(format!("boson remote GET {}: {}", path, e)))?;
        let env: ApiEnvelope<T> = resp.json().await.map_err(|e| {
            BosonError::internal(format!("boson remote GET {} decode: {}", path, e))
        })?;
        if !env.success {
            return Err(api_err(env, &format!("GET {}", path)));
        }
        env.data
            .ok_or_else(|| BosonError::internal(format!("boson remote GET {}: empty data", path)))
    }

    async fn post_json<B: serde::Serialize + ?Sized, T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let url = self.api(path);
        let sign_path = boson_sign_path(path);
        let body_vec = serde_json::to_vec(body).map_err(|e| {
            BosonError::internal(format!("boson remote POST {} serialize: {}", path, e))
        })?;
        let req = add_subsystem_hmac(
            self.client
                .post(&url)
                .header(reqwest::header::CONTENT_TYPE, "application/json"),
            &reqwest::Method::POST,
            &sign_path,
            &body_vec,
        );
        let resp = req
            .body(body_vec)
            .send()
            .await
            .map_err(|e| BosonError::internal(format!("boson remote POST {}: {}", path, e)))?;
        let env: ApiEnvelope<T> = resp.json().await.map_err(|e| {
            BosonError::internal(format!("boson remote POST {} decode: {}", path, e))
        })?;
        if !env.success {
            return Err(api_err(env, &format!("POST {}", path)));
        }
        env.data
            .ok_or_else(|| BosonError::internal(format!("boson remote POST {}: empty data", path)))
    }

    async fn post_empty_ok(&self, path: &str) -> Result<()> {
        let url = self.api(path);
        let sign_path = boson_sign_path(path);
        let req = add_subsystem_hmac(
            self.client.post(&url),
            &reqwest::Method::POST,
            &sign_path,
            &[],
        );
        let resp = req
            .send()
            .await
            .map_err(|e| BosonError::internal(format!("boson remote POST {}: {}", path, e)))?;
        let env: ApiEnvelope<serde_json::Value> = resp.json().await.map_err(|e| {
            BosonError::internal(format!("boson remote POST {} decode: {}", path, e))
        })?;
        if !env.success {
            return Err(api_err(env, &format!("POST {}", path)));
        }
        Ok(())
    }
}

fn job_status_from_str(s: &str) -> JobStatus {
    match s.to_ascii_lowercase().as_str() {
        "queued" => JobStatus::Queued,
        "running" => JobStatus::Running,
        "success" => JobStatus::Success,
        "failed" => JobStatus::Failed,
        "canceled" | "cancelled" => JobStatus::Canceled,
        _ => JobStatus::Queued,
    }
}

fn job_from_response(j: JobResponse) -> Job {
    Job {
        job_id: j.job_id,
        task_name: j.task_name,
        actor_json: serde_json::json!({}),
        params_json: serde_json::json!({}),
        priority: j.priority,
        pool: j.pool,
        status: job_status_from_str(&j.status),
        idempotency_key: None,
        created_at: DateTime::parse_from_rfc3339(&j.created_at)
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
        signature_hash: 0,
        attempt: 1,
    }
}

fn run_status_from_str(s: &str) -> RunStatus {
    match s.to_ascii_lowercase().as_str() {
        "running" => RunStatus::Running,
        "success" => RunStatus::Success,
        "failed" => RunStatus::Failed,
        "canceled" | "cancelled" => RunStatus::Canceled,
        "timeout" => RunStatus::Timeout,
        _ => RunStatus::Running,
    }
}

fn run_from_response(r: RunResponse) -> Run {
    Run {
        run_id: r.run_id,
        job_id: r.job_id,
        task_name: r.task_name,
        attempt: r.attempt,
        status: run_status_from_str(&r.status),
        started_at: DateTime::parse_from_rfc3339(&r.started_at)
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
        finished_at: r
            .finished_at
            .as_deref()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.with_timezone(&Utc)),
        duration_ms: r.duration_ms,
        error_message: None,
    }
}

fn task_config_from_response(c: TaskConfigResponse) -> TaskConfig {
    TaskConfig {
        task_name: c.task_name,
        priority: c.priority,
        pool: c.pool,
        retry_policy: c.retry_policy,
        rate_limit_policy: c.rate_limit_policy,
        idempotency_mode: None,
        updated_at: DateTime::parse_from_rfc3339(&c.updated_at)
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
    }
}

#[async_trait]
impl BosonCoordinatorBackend for HttpRemoteBosonCoordinatorBackend {
    async fn enqueue(
        &self,
        task_name: &str,
        _actor_json: serde_json::Value,
        params_json: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> Result<String> {
        let req = EnqueueRequest {
            task_name: task_name.to_string(),
            params: params_json,
            idempotency_key,
        };
        let resp: EnqueueResponse = self.post_json("/jobs/enqueue", &req).await?;
        Ok(resp.job_id)
    }

    async fn get_job(&self, job_id: &str) -> Option<Job> {
        let path = format!("/jobs/{}", urlencoding::encode(job_id));
        let url = self.api(&path);
        let sign_path = boson_sign_path(&path);
        let req = add_subsystem_hmac(
            self.client.get(&url),
            &reqwest::Method::GET,
            &sign_path,
            &[],
        );
        let resp = match req.send().await {
            Ok(r) => r,
            Err(_) => return None,
        };
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return None;
        }
        let env: ApiEnvelope<JobResponse> = match resp.json().await {
            Ok(e) => e,
            Err(_) => return None,
        };
        if !env.success {
            return None;
        }
        env.data.map(job_from_response)
    }

    async fn list_jobs(
        &self,
        status_filter: Option<JobStatus>,
        offset: usize,
        limit: usize,
    ) -> Vec<Job> {
        let need = (offset.saturating_add(limit)).max(limit).clamp(1, 50_000);
        let mut path = format!("/jobs?limit={}", need);
        if let Some(st) = status_filter {
            path.push_str("&status=");
            path.push_str(st.to_string().as_str());
        }
        match self.get_json::<Vec<JobResponse>>(&path).await {
            Ok(rows) => rows
                .into_iter()
                .map(job_from_response)
                .skip(offset)
                .take(limit)
                .collect(),
            Err(_e) => {
                log::warn!(target: "boson_remote_http", "remote http failure: {:?}", "list_jobs");
                vec![]
            }
        }
    }

    async fn cancel_job(&self, job_id: &str) -> Result<()> {
        let path = format!("/jobs/{}/cancel", urlencoding::encode(job_id));
        self.post_empty_ok(&path).await
    }

    async fn get_task_config(&self, task_name: &str) -> Result<TaskConfig> {
        let path = format!("/tasks/{}/config", urlencoding::encode(task_name));
        let c: TaskConfigResponse = self.get_json(&path).await?;
        Ok(task_config_from_response(c))
    }

    async fn upsert_task_config(&self, config: TaskConfig) -> Result<()> {
        let path = format!("/tasks/{}/config", urlencoding::encode(&config.task_name));
        let body = UpdateTaskConfigRequest {
            priority: Some(config.priority),
            pool: Some(config.pool.clone()),
            retry_policy: Some(config.retry_policy),
            rate_limit_policy: Some(config.rate_limit_policy),
        };
        let _: TaskConfigResponse = self.post_json(&path, &body).await?;
        Ok(())
    }

    async fn list_runs(
        &self,
        job_id_filter: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Vec<Run> {
        let need = (offset.saturating_add(limit)).max(limit).clamp(1, 50_000);
        let mut path = format!("/runs?limit={}", need);
        if let Some(j) = job_id_filter {
            path.push_str("&job_id=");
            path.push_str(&urlencoding::encode(j));
        }
        match self.get_json::<Vec<RunResponse>>(&path).await {
            Ok(rows) => rows
                .into_iter()
                .map(run_from_response)
                .skip(offset)
                .take(limit)
                .collect(),
            Err(_e) => {
                log::warn!(target: "boson_remote_http", "remote http failure: {:?}", "list_runs");
                vec![]
            }
        }
    }

    async fn get_run(&self, run_id: &str) -> Option<Run> {
        let path = format!("/runs/{}", urlencoding::encode(run_id));
        let url = self.api(&path);
        let sign_path = boson_sign_path(&path);
        let req = add_subsystem_hmac(
            self.client.get(&url),
            &reqwest::Method::GET,
            &sign_path,
            &[],
        );
        let resp = match req.send().await {
            Ok(r) => r,
            Err(_) => return None,
        };
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return None;
        }
        let env: ApiEnvelope<RunResponse> = match resp.json().await {
            Ok(e) => e,
            Err(_) => return None,
        };
        if !env.success {
            return None;
        }
        env.data.map(run_from_response)
    }

    fn registry(&self) -> &TaskRegistry {
        self.registry.as_ref()
    }

    /// 0.1.n: derived from bounded list responses until HTTP count routes exist.
    async fn count_jobs(&self, status_filter: Option<JobStatus>) -> u64 {
        u64::try_from(self.list_jobs(status_filter, 0, 50_000).await.len()).unwrap_or(u64::MAX)
    }

    async fn count_runs(&self, job_id_filter: Option<&str>) -> u64 {
        u64::try_from(self.list_runs(job_id_filter, 0, 50_000).await.len()).unwrap_or(u64::MAX)
    }

    async fn count_runs_since(&self, since: DateTime<Utc>) -> u64 {
        u64::try_from(
            self.list_runs(None, 0, 50_000)
                .await
                .into_iter()
                .filter(|r| r.started_at >= since)
                .count(),
        )
        .unwrap_or(u64::MAX)
    }

    async fn count_jobs_for_task(&self, task_name: &str, status: Option<JobStatus>) -> u64 {
        u64::try_from(
            self.list_jobs(status, 0, 50_000)
                .await
                .into_iter()
                .filter(|j| j.task_name == task_name)
                .count(),
        )
        .unwrap_or(u64::MAX)
    }

    async fn task_run_stats(&self, task_name: &str) -> TaskRunStats {
        let runs: Vec<_> = self
            .list_runs(None, 0, 50_000)
            .await
            .into_iter()
            .filter(|r| r.task_name == task_name)
            .collect();
        let runs_total = u32::try_from(runs.len()).unwrap_or(u32::MAX);
        let success_count = u32::try_from(
            runs.iter()
                .filter(|r| r.status == RunStatus::Success)
                .count(),
        )
        .unwrap_or(u32::MAX);
        TaskRunStats {
            runs_total,
            success_count,
        }
    }
}

/// Build remote HTTP coordinator for split `server-apps` profile.
///
/// Resolves the base URL via [`resolve_boson_remote_base_url`] and auto-discovers the local
/// [`TaskRegistry`] from `#[boson::task]` inventory. Fails if no base URL is configured.
///
/// # Examples
///
/// ```rust,ignore
/// use boson_coordinator::BosonCoordinatorBackend;
/// use boson_coordinator::remote_http::build_remote_coordinator;
///
/// # fn wire() -> boson_coordinator::Result<()> {
/// let backend: std::sync::Arc<dyn BosonCoordinatorBackend> = build_remote_coordinator()?;
/// # let _ = backend;
/// # Ok(())
/// # }
/// ```
pub fn build_remote_coordinator() -> Result<std::sync::Arc<dyn BosonCoordinatorBackend>> {
    let base = resolve_boson_remote_base_url().ok_or_else(|| {
        BosonError::internal(
            "Boson remote shell requires BOSON_REMOTE_BASE_URL or SUBSYSTEM_GATEWAY_BASE_URL (and SUBSYSTEM_CELL_SLUG, default home)",
        )
    })?;
    let registry = std::sync::Arc::new(TaskRegistry::auto_discover());
    Ok(std::sync::Arc::new(HttpRemoteBosonCoordinatorBackend::new(
        base, registry,
    )))
}
