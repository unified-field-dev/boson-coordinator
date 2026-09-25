//! Remote HTTP backend: URL resolution and client happy/sad paths.
//!
//! Test-only fixtures below are intentionally undocumented; this binary target is exempt from
//! the library's `missing_docs = "deny"` lint (see `Cargo.toml`).
#![allow(missing_docs)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::await_holding_lock)]

use std::sync::{Arc, Mutex, OnceLock};

use boson_coordinator::remote_http::{
    build_remote_coordinator, resolve_boson_remote_base_url, subsystem_hmac_header_pair,
    HttpRemoteBosonCoordinatorBackend, SUBSYSTEM_AUTH_HEADER_NAME, SUBSYSTEM_HMAC_KEY_ENV,
};
use boson_coordinator::BosonCoordinatorBackend;
use boson_core::BosonError;
use boson_runtime::TaskRegistry;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// 32-byte UTF-8 key meeting soliton's [`MIN_HMAC_KEY_LEN`].
const TEST_HMAC_KEY: &str = "soliton-test-hmac-key-32-bytes!!";

fn clear_remote_env() {
    // SAFETY: tests hold `env_lock` while mutating process environment.
    unsafe {
        std::env::remove_var("BOSON_REMOTE_BASE_URL");
        std::env::remove_var("SUBSYSTEM_GATEWAY_BASE_URL");
        std::env::remove_var("SUBSYSTEM_CELL_SLUG");
    }
}

fn restore_env(key: &str, prev: Option<String>) {
    // SAFETY: tests hold `env_lock` while mutating process environment.
    unsafe {
        match prev {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
    }
}

async fn spawn_json_server(status_line: &str, body: &str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let status_line = status_line.to_string();
    let body = body.to_string();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut buf = vec![0u8; 8192];
        let _ = stream.read(&mut buf).await;
        let response = format!(
            "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes()).await;
    });
    format!("http://{addr}")
}

fn remote_backend(base_url: String) -> HttpRemoteBosonCoordinatorBackend {
    HttpRemoteBosonCoordinatorBackend::new(base_url, Arc::new(TaskRegistry::auto_discover()))
}

#[test]
fn resolve_boson_remote_base_url_happy_and_sad() {
    let _guard = env_lock().lock().expect("env lock");
    let prev_direct = std::env::var("BOSON_REMOTE_BASE_URL").ok();
    let prev_gateway = std::env::var("SUBSYSTEM_GATEWAY_BASE_URL").ok();
    let prev_cell = std::env::var("SUBSYSTEM_CELL_SLUG").ok();

    clear_remote_env();
    assert_eq!(resolve_boson_remote_base_url(), None);

    // SAFETY: tests hold `env_lock` while mutating process environment.
    unsafe {
        std::env::set_var("BOSON_REMOTE_BASE_URL", "https://boson.example.com/");
    }
    assert_eq!(
        resolve_boson_remote_base_url().as_deref(),
        Some("https://boson.example.com")
    );

    clear_remote_env();
    unsafe {
        std::env::set_var("SUBSYSTEM_GATEWAY_BASE_URL", "https://gw.example.com/");
        std::env::set_var("SUBSYSTEM_CELL_SLUG", "lab");
    }
    assert_eq!(
        resolve_boson_remote_base_url().as_deref(),
        Some("https://gw.example.com/cell/lab/sub/boson")
    );

    clear_remote_env();
    restore_env("BOSON_REMOTE_BASE_URL", prev_direct);
    restore_env("SUBSYSTEM_GATEWAY_BASE_URL", prev_gateway);
    restore_env("SUBSYSTEM_CELL_SLUG", prev_cell);
}

#[test]
fn build_remote_coordinator_requires_base_url() {
    let _guard = env_lock().lock().expect("env lock");
    let prev_direct = std::env::var("BOSON_REMOTE_BASE_URL").ok();
    let prev_gateway = std::env::var("SUBSYSTEM_GATEWAY_BASE_URL").ok();
    let prev_cell = std::env::var("SUBSYSTEM_CELL_SLUG").ok();

    clear_remote_env();
    match build_remote_coordinator() {
        Ok(_) => panic!("expected missing base URL to fail"),
        Err(err) => assert!(matches!(err, BosonError::Internal { .. })),
    }

    clear_remote_env();
    restore_env("BOSON_REMOTE_BASE_URL", prev_direct);
    restore_env("SUBSYSTEM_GATEWAY_BASE_URL", prev_gateway);
    restore_env("SUBSYSTEM_CELL_SLUG", prev_cell);
}

#[tokio::test]
async fn remote_enqueue_happy_path() {
    let base = spawn_json_server(
        "HTTP/1.1 200 OK",
        r#"{"success":true,"data":{"job_id":"job-remote-1"},"error":null}"#,
    )
    .await;
    let backend = remote_backend(base);
    let job_id = backend
        .enqueue(
            "any_task",
            serde_json::json!({}),
            serde_json::json!({}),
            None,
        )
        .await
        .expect("enqueue");
    assert_eq!(job_id, "job-remote-1");
}

#[tokio::test]
async fn remote_enqueue_api_error_is_internal() {
    let base = spawn_json_server(
        "HTTP/1.1 200 OK",
        r#"{"success":false,"data":null,"error":"task not found"}"#,
    )
    .await;
    let backend = remote_backend(base);
    let err = backend
        .enqueue(
            "missing",
            serde_json::json!({}),
            serde_json::json!({}),
            None,
        )
        .await
        .unwrap_err();
    match err {
        BosonError::Internal { message, .. } => assert!(message.contains("task not found")),
        other => panic!("expected Internal, got {other:?}"),
    }
}

#[tokio::test]
async fn remote_enqueue_connection_refused() {
    let backend = remote_backend("http://127.0.0.1:1".to_string());
    let err = backend
        .enqueue(
            "any_task",
            serde_json::json!({}),
            serde_json::json!({}),
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, BosonError::Internal { .. }));
}

#[tokio::test]
async fn remote_get_job_not_found_returns_none() {
    let base = spawn_json_server("HTTP/1.1 404 Not Found", r#"{"success":false}"#).await;
    let backend = remote_backend(base);
    assert!(backend.get_job("missing").await.is_none());
}

#[tokio::test]
async fn remote_get_job_happy_path() {
    let body = r#"{
        "success": true,
        "data": {
            "job_id": "job-42",
            "task_name": "echo",
            "status": "queued",
            "priority": 0,
            "pool": "default",
            "created_at": "2026-01-01T00:00:00Z"
        },
        "error": null
    }"#;
    let base = spawn_json_server("HTTP/1.1 200 OK", body).await;
    let backend = remote_backend(base);
    let job = backend.get_job("job-42").await.expect("job");
    assert_eq!(job.job_id, "job-42");
    assert_eq!(job.task_name, "echo");
}

#[tokio::test]
async fn remote_cancel_job_happy_path() {
    let base = spawn_json_server(
        "HTTP/1.1 200 OK",
        r#"{"success":true,"data":null,"error":null}"#,
    )
    .await;
    let backend = remote_backend(base);
    backend.cancel_job("job-1").await.expect("cancel");
}

#[tokio::test]
async fn remote_cancel_job_api_error_is_internal() {
    let base = spawn_json_server(
        "HTTP/1.1 200 OK",
        r#"{"success":false,"data":null,"error":"job not found"}"#,
    )
    .await;
    let backend = remote_backend(base);
    let err = backend.cancel_job("missing").await.unwrap_err();
    match err {
        BosonError::Internal { message, .. } => assert!(message.contains("job not found")),
        other => panic!("expected Internal, got {other:?}"),
    }
}

#[test]
fn subsystem_hmac_header_pair_none_without_key() {
    let _guard = env_lock().lock().expect("env lock");
    let prev = std::env::var(SUBSYSTEM_HMAC_KEY_ENV).ok();
    // SAFETY: tests hold `env_lock` while mutating process environment.
    unsafe {
        std::env::remove_var(SUBSYSTEM_HMAC_KEY_ENV);
    }
    assert!(subsystem_hmac_header_pair("GET", "/api/boson/jobs", b"").is_none());
    restore_env(SUBSYSTEM_HMAC_KEY_ENV, prev);
}

#[test]
fn subsystem_hmac_header_pair_stable_with_key() {
    let _guard = env_lock().lock().expect("env lock");
    let prev = std::env::var(SUBSYSTEM_HMAC_KEY_ENV).ok();
    // SAFETY: tests hold `env_lock` while mutating process environment.
    unsafe {
        std::env::set_var(SUBSYSTEM_HMAC_KEY_ENV, TEST_HMAC_KEY);
    }
    let a = subsystem_hmac_header_pair("POST", "/api/boson/jobs/enqueue", b"{}").expect("tag");
    let b = subsystem_hmac_header_pair("POST", "/api/boson/jobs/enqueue", b"{}").expect("tag");
    assert_eq!(a.0, SUBSYSTEM_AUTH_HEADER_NAME);
    assert_eq!(a.1, b.1);
    let c = subsystem_hmac_header_pair("POST", "/api/boson/jobs/enqueue", b"other").expect("tag");
    assert_ne!(a.1, c.1);
    restore_env(SUBSYSTEM_HMAC_KEY_ENV, prev);
}

#[tokio::test]
async fn remote_get_attaches_subsystem_hmac_header() {
    let _guard = env_lock().lock().expect("env lock");
    let prev = std::env::var(SUBSYSTEM_HMAC_KEY_ENV).ok();
    // SAFETY: tests hold `env_lock` while mutating process environment.
    unsafe {
        std::env::set_var(SUBSYSTEM_HMAC_KEY_ENV, TEST_HMAC_KEY);
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let seen = Arc::new(Mutex::new(None::<String>));
    let seen_c = Arc::clone(&seen);
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut buf = vec![0u8; 8192];
        let n = stream.read(&mut buf).await.expect("read");
        let req = String::from_utf8_lossy(&buf[..n]).to_string();
        let header = req
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("x-subsystem-auth:"))
            .map(|l| {
                l.split_once(':')
                    .map(|(_, v)| v.trim().to_string())
                    .unwrap()
            });
        *seen_c.lock().expect("lock") = header;
        let body = r#"{"success":true,"data":{"job_id":"j1","task_name":"echo","status":"queued","priority":0,"pool":"default","created_at":"2026-01-01T00:00:00Z"},"error":null}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes()).await;
    });

    let backend = remote_backend(format!("http://{addr}"));
    let _ = backend.get_job("j1").await;
    let expected = subsystem_hmac_header_pair("GET", "/api/boson/jobs/j1", b"").expect("tag");
    let got = seen.lock().expect("lock").clone();
    restore_env(SUBSYSTEM_HMAC_KEY_ENV, prev);
    assert_eq!(got.as_deref(), Some(expected.1.as_str()));
}
