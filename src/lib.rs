//! # Boson coordinator
//!
//! Coordinator API for hosts integrating [Boson]. An object-safe
//! [`BosonCoordinatorBackend`] trait abstracts "administer and enqueue Boson tasks" behind
//! one interface, whether the task queue lives in-process or behind a remote HTTP boundary.
//! Hosts and product code depend on this crate instead of reaching into upstream
//! `boson-runtime` directly, so swapping topology later is a backend swap.
//!
//! [Boson]: https://github.com/unified-field-dev/boson
//!
//! Mount `/api/boson` via [`axum_api::BosonState::builder`] with [`axum_api::AdminAuth`] (fail closed
//! by default; set `BOSON_OPEN_LAB_MODE=1` only for open lab mounts). The `remote-http` client signs
//! with `SUBSYSTEM_AUTH_HMAC_KEY` when set (Soliton-compatible `x-subsystem-auth`).
//!
//! ## Features
//!
//! - **Object-safe backend trait** — [`BosonCoordinatorBackend`] is `dyn`-compatible, so hosts
//!   hold `Arc<dyn BosonCoordinatorBackend>` without caring whether it is local or remote
//! - **Local runtime handle** — [`CoordinatorAdapter`] wraps an upstream
//!   [`Boson`](boson_runtime::Boson) runtime for same-process enqueue and admin
//! - **Axum routes** (`axum` feature) — [`axum_api::BosonState`] and
//!   [`axum_api::boson_router`] mount upstream's `/api/boson` routes on your own Axum state
//! - **Remote HTTP client** (`remote-http` feature) —
//!   `remote_http::HttpRemoteBosonCoordinatorBackend` talks to a split `boson-server` over
//!   HTTP while keeping task discovery in-process
//! - **Task-config bootstrap** — [`ensure_default_task_configs_embedded`] seeds
//!   `boson_task_config` rows from `#[boson::task]` inventory defaults
//! - **Autoscale helpers** — [`scaling::compute_target_workers`] turns queue depth into a
//!   worker count with hysteresis; [`stats::count_queued_jobs`] feeds it
//!
//! *One coordinator trait, two backends — the same product code enqueues and administers Boson
//! tasks whether they run in this process or across an HTTP boundary.*
//!
//! # Getting started
//!
//! Most hosts only need one of the two backends below, wrapped in `Arc<dyn
//! BosonCoordinatorBackend>` so product code never depends on which one is active.
//!
//! ## In-process: `CoordinatorAdapter`
//!
//! Use this when your binary already owns an upstream [`Boson`](boson_runtime::Boson) runtime
//! (the common case for a monolith, or a worker binary that also serves admin routes).
//!
//! ```rust,ignore
//! use std::sync::Arc;
//!
//! use boson_coordinator::{BosonCoordinatorBackend, CoordinatorAdapter};
//!
//! # fn wire(boson: Arc<boson_runtime::Boson>) {
//! let backend: Arc<dyn BosonCoordinatorBackend> = Arc::new(CoordinatorAdapter::new(boson));
//! # let _ = backend;
//! # }
//! ```
//!
//! Enqueue and inspect jobs through the trait, not the concrete type, so callers stay portable
//! to the remote backend below:
//!
//! ```rust,ignore
//! # async fn enqueue(backend: &dyn boson_coordinator::BosonCoordinatorBackend) -> boson_coordinator::Result<()> {
//! let job_id = backend
//!     .enqueue("send_email", serde_json::json!({}), serde_json::json!({"to": "a@b.com"}), None)
//!     .await?;
//! let job = backend.get_job(&job_id).await;
//! # let _ = job;
//! # Ok(())
//! # }
//! ```
//!
//! ## Remote HTTP: `remote_http::HttpRemoteBosonCoordinatorBackend` (`remote-http` feature)
//!
//! Use this when product code and the Boson runtime live in different binaries (split
//! `server-apps` → `boson-server` topology). Task discovery (`registry()`) stays in-process; job
//! and run mutations go over HTTP to `/api/boson/*`. See the `remote_http` module for base-URL resolution.
//!
//! ```rust,ignore
//! use std::sync::Arc;
//!
//! use boson_coordinator::BosonCoordinatorBackend;
//! use boson_coordinator::remote_http::build_remote_coordinator;
//!
//! # async fn wire() -> boson_coordinator::Result<()> {
//! let backend: Arc<dyn BosonCoordinatorBackend> = build_remote_coordinator()?;
//! let job_id = backend
//!     .enqueue("send_email", serde_json::json!({}), serde_json::json!({}), None)
//!     .await?;
//! # let _ = job_id;
//! # Ok(())
//! # }
//! ```
//!
//! ## Axum routes (`axum` feature)
//!
//! Mount upstream Boson's HTTP API on your own Axum router and state via
//! [`axum_api::boson_router`] and [`axum_api::BosonState`] — see [`axum_api`] for a full example.
//!
//! ## Concern → API
//!
//! | Concern | API |
//! |---------|-----|
//! | Backend-agnostic enqueue/admin surface | [`BosonCoordinatorBackend`] — the trait every backend implements |
//! | Local, in-process enqueue | [`CoordinatorAdapter`] wrapping upstream [`Boson`](boson_runtime::Boson) |
//! | Remote, split `boson-server` topology | `remote_http` (`remote-http` feature) — HTTP backend, base-URL resolution, DTOs |
//! | Mounting upstream's `/api/boson` on your router | [`axum_api`] (`axum` feature) — Axum state and router |
//! | Seeding task config at boot | [`ensure_default_task_configs_embedded`] — bootstrap from `#[boson::task]` inventory defaults |
//! | Autoscaling workers to queue depth | [`scaling`] / [`stats`] — worker-count math and the queue-depth metric that feeds it |
//!
//! Runnable examples: `cargo run -p boson-coordinator --example local_enqueue`
//! (and `axum_require_admin` with the `axum` feature).

#[cfg(feature = "axum")]
pub mod axum_api;
mod coordinator_adapter;
mod coordinator_trait;
mod default_task_config;
#[cfg(feature = "remote-http")]
mod remote_api_types;
pub mod scaling;
pub mod stats;

#[cfg(feature = "remote-http")]
pub mod remote_http;

/// Upstream error type shared by every [`BosonCoordinatorBackend`] implementation.
pub use boson_core::BosonError;
pub use coordinator_adapter::CoordinatorAdapter;
pub use coordinator_trait::BosonCoordinatorBackend;
pub use default_task_config::{
    ensure_default_task_configs_embedded, ensure_default_task_configs_embedded_with_skip,
};

/// Result alias used throughout this crate's public API (see [`BosonError`]).
pub type Result<T> = boson_core::Result<T>;
