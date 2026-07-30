# boson-coordinator verification

Boson coordinator (trait, local runtime handle, Axum / remote-http). Re-run after code or doc
changes. Covered by unit + integration tests below.

## Environment

```bash
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-boson-coordinator
```

## Unit + integration (CI)

```bash
cargo fmt --all --check
cargo clippy --all-targets --features axum,remote-http -- -D warnings
# Serialize tests: some suites share process-wide OnceLock registries / queue fixtures
# and can flake under parallel cargo test. Prefer isolation later; until then:
cargo test --features axum,remote-http -- --test-threads=1
```

Narrower runs:

```bash
cargo test                                    # scaling unit + coordinator_contract
cargo test --features axum                    # + Axum router integ
cargo test --features remote-http             # + remote HTTP client integ
```

### TEST_MAP

| Behavior | Level | Happy | Sad | Notes |
|----------|-------|-------|-----|-------|
| `compute_target_workers` / `AutoscalePolicy` | unit | scale up / down / hysteresis / clamp | — | `scaling::tests` |
| `CoordinatorAdapter` enqueue → list → count → cancel | integ | queued job visible; cancel → `Canceled` | cancel missing → `JobNotFound` | `tests/coordinator_contract.rs` (`adapter_enqueue_list_count_cancel_workflow`, `adapter_cancel_missing_job_is_not_found`) |
| `stats::count_queued_jobs` | integ | depth increments after enqueue | — | feeds autoscale |
| `ensure_default_task_configs_embedded` | integ | upserts registered task config | upsert fail → `anyhow` with task name | skip omits listed task names from upsert |
| Adapter enqueue / get | integ | enqueue + `get_job` queued | unknown task → `TaskNotFound`; missing job → `None` | `tests/integration_test.rs` |
| `axum_api::boson_router` enqueue / get | integ | enqueue → get job roundtrip | unknown task → 400; missing job → 404 | oneshot, no listen socket |
| `BosonState::new` | integ | adapter-backed runtime | non-adapter → `Internal` | requires `as_boson_runtime` |
| `remote_http` URL resolve / build | integ | direct + gateway cell URL | missing env → `None` / `Internal` | env lock |
| `HttpRemoteBosonCoordinatorBackend` enqueue / get / cancel | integ | 200 envelope → job id / job / Ok | API error / connection refused → `Internal`; 404 get → `None` | mock TCP JSON server |

## Notes

- Tests may `unwrap`/`expect`; production paths map failures to typed
  [`BosonError`](https://docs.rs/boson-core) / `anyhow` (no ordinary-path unwrap).
- Sad-path assertions check typed variants, HTTP status codes, or message
  content — (stronger than `is_err()` alone).
- Auth for raw `/api/boson` is host-injected; this crate does not claim to secure
  the HTTP boundary.
