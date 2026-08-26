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
//! - **Local coordinator backend** — Wrap an upstream [`Boson`](boson_runtime::Boson) runtime in
//!   [`CoordinatorAdapter`] and enqueue through [`BosonCoordinatorBackend`] so product code stays
//!   portable to a remote backend later. [Get started](#in-process-enqueue)
//! - **Remote coordinator backend** — Forward enqueue and admin calls to a split `boson-server` over HTTP
//!   (`remote-http` feature) while keeping task discovery in-process. [Get started](#remote-http-backend)
//! - **Axum API mount** — Nest upstream `/api/boson` routes on your host Axum router with fail-closed
//!   admin auth (`axum` feature). [Get started](#axum-mount)
//! - **Default task-config seeding** — Seed `boson_task_config` rows from `#[boson::task]` inventory
//!   defaults at host boot so workers see current retry and rate-limit policy.
//!   [Get started](#task-config-bootstrap)
//! - **Autoscale helpers** — [`scaling::compute_target_workers`] turns queue depth into a worker
//!   count with hysteresis; [`stats::count_queued_jobs`] feeds it ([`scaling`](scaling/index.html) /
//!   [`stats`](stats/index.html) API reference)
//!
//! # Feature flags
//!
//! | Feature | Default | Purpose |
//! |---------|---------|---------|
//! | *(none)* | yes | Trait, local [`CoordinatorAdapter`], default task-config bootstrap |
//! | `axum` | no | [`axum_api`] — mount upstream `/api/boson` on Axum state |
//! | `remote-http` | no | [`remote_http`] HTTP client for split `boson-server` topology |
//!
//! *One coordinator trait, two backends — the same product code enqueues and administers Boson
//! tasks whether they run in this process or across an HTTP boundary.*
//!
//! # Getting started
//!
//! Most hosts pick one backend, wrap it in `Arc<dyn BosonCoordinatorBackend>`, and keep product
//! code on the trait surface.
//!
//! ## In-process enqueue
//!
//! [`CoordinatorAdapter`] is the local backend when your binary already owns an upstream
//! [`Boson`](boson_runtime::Boson) runtime. Monoliths and worker binaries that serve admin
//! routes use this path once the runtime is built.
//!
//! Prerequisites: a shared `Arc<Boson>` runtime and tasks registered through `#[boson::task]`
//! inventory.
//!
//! ```rust,no_run
//! use std::sync::Arc;
//!
//! use boson_coordinator::{BosonCoordinatorBackend, CoordinatorAdapter};
//!
//! # async fn enqueue(boson: Arc<boson_runtime::Boson>) -> boson_coordinator::Result<()> {
//! let backend: Arc<dyn BosonCoordinatorBackend> = Arc::new(CoordinatorAdapter::new(boson));
//! let job_id = backend
//!     .enqueue(
//!         "send_email",
//!         serde_json::json!({}),
//!         serde_json::json!({"to": "a@b.com"}),
//!         None,
//!     )
//!     .await?;
//! let job = backend
//!     .get_job(&job_id)
//!     .await
//!     .expect("the enqueued job must be readable");
//! assert_eq!(job.task_name, "send_email");
//! # Ok(())
//! # }
//! ```
//!
//! Next: switch to [Remote HTTP backend](#remote-http-backend) when product code and
//! `boson-server` run in separate binaries.
//!
//! ## Remote HTTP backend
//!
//! The `remote-http` client implements [`BosonCoordinatorBackend`] against a remote
//! `boson-server`. Product code in `server-apps` enqueues here; `registry()` still runs
//! in-process so workers discover the same task inventory.
//!
//! Prerequisites: enable the `remote-http` feature and set `BOSON_REMOTE_BASE_URL` or
//! `SUBSYSTEM_GATEWAY_BASE_URL` + `SUBSYSTEM_CELL_SLUG`. Optional `SUBSYSTEM_AUTH_HMAC_KEY`
//! signs requests (Soliton-compatible `x-subsystem-auth`).
//!
//! ```rust,ignore
//! use std::sync::Arc;
//!
//! use boson_coordinator::BosonCoordinatorBackend;
//! use boson_coordinator::remote_http::build_remote_coordinator;
//!
//! # async fn enqueue() -> boson_coordinator::Result<()> {
//! let backend: Arc<dyn BosonCoordinatorBackend> = build_remote_coordinator()?;
//! let job_id = backend
//!     .enqueue("send_email", serde_json::json!({}), serde_json::json!({}), None)
//!     .await?;
//! assert!(!job_id.is_empty(), "remote enqueue must return a job id");
//! # Ok(())
//! # }
//! ```
//!
//! Base-URL resolution and signing details live in the [`remote_http`] module. Next:
//! [mount Axum routes](#axum-mount) when this binary also serves `/api/boson`.
//!
//! ## Axum mount
//!
//! Mount upstream Boson's HTTP API on your host router at startup after the coordinator runtime
//! is wired. Admin routes fail closed unless you install [`axum_api::AdminAuth`] or opt into open
//! lab mode (`BOSON_OPEN_LAB_MODE=1`).
//!
//! Prerequisites: `axum` feature, `Arc<Boson>` runtime, and an `AdminAuth` verifier on
//! [`axum_api::BosonState::builder`].
//!
//! ```rust,ignore
//! use std::sync::Arc;
//!
//! use axum::extract::FromRef;
//! use boson_coordinator::axum_api::{
//!     boson_router, BosonState, StaticTokenAdminAuth, NEST_PATH,
//! };
//!
//! #[derive(Clone)]
//! struct AppState {
//!     boson: BosonState,
//! }
//!
//! impl FromRef<AppState> for boson_axum::BosonState {
//!     fn from_ref(app: &AppState) -> Self {
//!         app.boson.inner_axum()
//!     }
//! }
//!
//! # fn mount(boson: Arc<boson_runtime::Boson>) -> anyhow::Result<axum::Router<AppState>> {
//! let state = AppState {
//!     boson: BosonState::builder(boson)
//!         .admin_auth(Arc::new(StaticTokenAdminAuth::new("lab-token")))
//!         .require_admin_auth(true)
//!         .build()?,
//! };
//! let router = boson_router::<AppState>().with_state(state);
//! println!("Boson routes mounted at {NEST_PATH}");
//! # Ok(router)
//! # }
//! ```
//!
//! Runnable variant: `cargo run -p boson-coordinator --example axum_require_admin --features axum`.
//!
//! ## Task-config bootstrap
//!
//! Call [`ensure_default_task_configs_embedded`] once at host boot after the coordinator backend
//! is wired and before workers dequeue jobs. The call upserts `boson_task_config` rows from
//! `#[boson::task]` descriptor defaults so retry, rate-limit, and pool settings stay current.
//!
//! Prerequisites: a [`BosonCoordinatorBackend`] handle and linked `#[boson::task]` inventory.
//! Skip hand-managed tasks with [`ensure_default_task_configs_embedded_with_skip`].
//!
//! ```rust,ignore
//! use std::sync::Arc;
//!
//! use boson_coordinator::{ensure_default_task_configs_embedded, BosonCoordinatorBackend};
//!
//! # async fn boot(backend: Arc<dyn BosonCoordinatorBackend>) -> anyhow::Result<()> {
//! ensure_default_task_configs_embedded(backend.clone()).await?;
//! let cfg = backend.get_task_config("send_email").await?;
//! assert_eq!(cfg.task_name, "send_email");
//! # Ok(())
//! # }
//! ```
//!
//! Runnable in-process enqueue example: `cargo run -p boson-coordinator --example local_enqueue`.

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
