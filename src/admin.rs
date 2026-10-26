//! Health, readiness and introspection endpoints.
//!
//! The S3 listener serves these endpoints before the S3 handlers. They stay
//! small by design. An orchestrator or an operator can read three JSON
//! documents. The reader needs no knowledge of this crate.
//!
//! * **Liveness** — the process is alive. It answers HTTP.
//! * **Readiness** — start-up finished. The server serves S3 traffic.
//! * **Introspection** — the effective runtime state: the Telegram tuning
//!   knobs, the listen port, and what each start-up reconciler did.
//!
//! None of them touch the database or Telegram. A rate-limited upstream
//! cannot make a healthy server look broken to an orchestrator.
//!
//! # Authentication
//!
//! These endpoints are unauthenticated. A probe that must sign a request
//! stops working when it expires. That is the opposite of what a health
//! check needs. Nothing sensitive is published. See
//! [`AdminState`](crate::admin::AdminState) for what the introspection
//! document can carry.
//!
//! # Path safety
//!
//! S3 addresses objects at `/{bucket}/{key}`. A client can name any bucket
//! with any path. A bare `/healthz` names bucket `healthz`. Every admin path
//! starts with [`ADMIN_PREFIX`](crate::admin::ADMIN_PREFIX). Its leading `.`
//! makes it unreachable as a bucket name. See that constant's
//! documentation.

use crate::backend::GrammersLimits;

use http::{Extensions, HeaderMap, Method, StatusCode, Uri};
use s3s::{Body, S3Request, S3Response, S3Result, route::S3Route};
use serde_json::{Value, json};

/// Reserved prefix for every admin endpoint.
///
/// The leading `.` is the whole point. An S3 bucket name must begin and end
/// with a lowercase letter or a digit. The crate's own `validate_bucket_name`
/// rejects anything else. So `.tele-s3` can never be a bucket. No
/// `PUT /.tele-s3/...` can create one. Under the S3 path grammar, the first
/// segment of a request is the bucket. Nothing an S3 client can ask for
/// lands inside this prefix.
pub const ADMIN_PREFIX: &str = "/.tele-s3";

/// Liveness: is this process alive and serving HTTP?
pub const LIVENESS_PATH: &str = "/.tele-s3/healthz";

/// Readiness: has start-up finished? Does the server serve S3 traffic?
pub const READINESS_PATH: &str = "/.tele-s3/readyz";

/// Introspection: the effective runtime state of this process.
pub const INTROSPECTION_PATH: &str = "/.tele-s3/introspect";

/// `error.type` for a failed orphan-blob reconciliation pass.
pub const ORPHAN_BLOB_RECONCILER_ERROR_TYPE: &str = "blob_reconciliation";

/// `error.type` for a failed publish-intent reconciliation pass.
pub const PUBLISH_INTENT_RECONCILER_ERROR_TYPE: &str = "publish_intent_reconciliation";

/// Which admin endpoint a request path names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminEndpoint {
    /// [`LIVENESS_PATH`].
    Liveness,
    /// [`READINESS_PATH`].
    Readiness,
    /// [`INTROSPECTION_PATH`].
    Introspection,
    /// The reserved prefix names no endpoint.
    Unknown,
}

impl AdminEndpoint {
    /// Stable name for the endpoint. The code uses it as a tracing field.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Liveness => "liveness",
            Self::Readiness => "readiness",
            Self::Introspection => "introspection",
            Self::Unknown => "unknown",
        }
    }
}

/// Classify a request path.
///
/// `None` means "not an admin request". The caller must pass the request to
/// S3 without changes.
#[must_use]
pub fn classify(path: &str) -> Option<AdminEndpoint> {
    match path {
        LIVENESS_PATH => Some(AdminEndpoint::Liveness),
        READINESS_PATH => Some(AdminEndpoint::Readiness),
        INTROSPECTION_PATH => Some(AdminEndpoint::Introspection),
        // Under the reserved prefix but unrecognised, so the admin layer owns
        // the answer rather than letting S3 fail on an impossible bucket name.
        _ if is_reserved(path) => Some(AdminEndpoint::Unknown),
        _ => None,
    }
}

/// Whether a path is inside [`ADMIN_PREFIX`].
///
/// The code matches on a segment boundary. A bucket named `.tele-s3x`
/// cannot claim the prefix. That bucket name is impossible, but the code
/// does not assume it.
fn is_reserved(path: &str) -> bool {
    path.strip_prefix(ADMIN_PREFIX)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
}

/// What one start-up reconciliation pass did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcilerOutcome {
    /// The pass ran. It reconciled or cleared `affected` rows. Zero is an
    /// ordinary result. Zero is not a failure.
    #[allow(missing_docs)]
    Completed { affected: usize },
    /// The pass returned an error.
    ///
    /// The code does not carry the message. The message is free-form operator
    /// text. It could quote anything the backend had in hand. The code serves
    /// this document without authentication. An operator needs to know which pass
    /// failed. The failed pass is a closed set. That set is the
    /// [`error.type`](ORPHAN_BLOB_RECONCILER_ERROR_TYPE) name. The code logs
    /// the message at start-up.
    Failed,
}

/// Outcome of both start-up reconciliation passes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartupReport {
    /// [`TeleS3::reconcile_orphan_blobs`](crate::s3::TeleS3).
    pub orphan_blobs: ReconcilerOutcome,
    /// [`TeleS3::reconcile_publish_intents`](crate::s3::TeleS3).
    pub publish_intents: ReconcilerOutcome,
}

/// The runtime state the admin endpoints publish.
///
/// The field list is the redaction policy. It holds a port, three numbers,
/// and two closed enums. No path allows `auth_access_key`,
/// `auth_secret_key`, `api_hash`, `bot_token`, `database_uri`, or any
/// other config value to reach a response. The value cannot reach it in
/// whole, in part, or in an error string. Adding a field here is the only
/// way to change the policy. The whitelist test makes that change
/// deliberate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdminState {
    listen_port: u16,
    limits: GrammersLimits,
    startup: StartupReport,
    trace_export: bool,
    metrics_export: bool,
    max_concurrent_connections: usize,
}

impl AdminState {
    /// Capture the state to publish.
    ///
    /// The method takes the port and the limits by value. It does not take a
    /// `&Config`. No argument can carry a credential.
    ///
    /// `trace_export` is a bool. It is not the endpoint itself. The endpoint
    /// is operator infrastructure. It often holds an in-cluster address with a
    /// hostname. The code serves this document without authentication. A probe
    /// needs to know whether spans leave the process at all.
    ///
    /// `metrics_export` answers the same question for the rate-limit and RPC
    /// metrics. An operator tunes
    /// `TELE_S3_MAX_CONCURRENT_REQUESTS`, `TELE_S3_IO_TIMEOUT_SECS`, and
    /// `TELE_S3_MAX_ATTEMPTS`. Without these metrics, the operator tunes
    /// blind. `metrics_export` tracks `trace_export` today. One endpoint turns
    /// both on. The code publishes `metrics_export` separately. A probe can
    /// then tell which half of the pipeline a future change made dark.
    #[must_use]
    pub fn new(
        listen_port: u16,
        limits: GrammersLimits,
        startup: StartupReport,
        trace_export: bool,
        metrics_export: bool,
        max_concurrent_connections: usize,
    ) -> Self {
        Self {
            listen_port,
            limits,
            startup,
            trace_export,
            metrics_export,
            max_concurrent_connections,
        }
    }

    /// Liveness.
    ///
    /// The method always returns [`StatusCode::OK`]. It derives from nothing.
    /// It does not use Telegram reachability. It does not use the database.
    /// It does not use the reconcilers. A server that answers this question
    /// is alive by definition. A Telegram rate limit must never look like a
    /// dead process to the supervisor. That would turn a throttled upstream
    /// into a restart loop over a healthy server.
    #[must_use]
    pub fn liveness(&self) -> AdminResponse {
        AdminResponse::ok(json!({ "status": "ok" }))
    }

    /// Readiness.
    ///
    /// The method also always returns [`StatusCode::OK`]. This is a deliberate
    /// decision, not an accident. Reaching this handler means start-up ran to
    /// completion. Reconciliation finishes before the listener binds. So the
    /// question "is reconciliation done?" has one answer here. The code reports
    /// how it went.
    ///
    /// A reconciler failure does not clear readiness. Both passes do
    /// best-effort garbage collection. A failed pass leaves unreferenced
    /// rows. No listing shows those rows. The next pass can reclaim them.
    /// The database and Telegram serve reads and writes either way. Reporting a
    /// failed pass as "not ready" would let an orchestrator stop routing
    /// traffic to a capable server. That would make a garbage-collection
    /// nuisance an outage. No remedy exists short of
    /// restarting the process that would fix it. The code publishes the outcome
    /// in the body. The code logs it at start-up. An operator can handle it
    /// there.
    #[must_use]
    pub fn readiness(&self) -> AdminResponse {
        AdminResponse::ok(json!({
            "status": "ready",
            "startup": self.startup_json(),
        }))
    }

    /// Introspection: the effective runtime state.
    ///
    /// An operator needs this when a request fails. The config file may be
    /// three environments away. The document shows the three Telegram knobs in
    /// force. Environment overrides and clamping set those knobs. It shows the
    /// bound port. It shows what start-up reconciliation achieved. It omits
    /// database counts. Database counts would put a live query on an
    /// unauthenticated probe path. That is a poor trade for a number no one
    /// needs to decide.
    #[must_use]
    pub fn introspection(&self) -> AdminResponse {
        AdminResponse::ok(json!({
            "status": "ok",
            "version": env!("CARGO_PKG_VERSION"),
            "listen_port": self.listen_port,
            "max_concurrent_connections": self.max_concurrent_connections,
            "telegram": {
                // The same names `init_tracing` publishes, so a log line and
                // this document can be read with one vocabulary.
                "max_concurrent_requests": self.limits.max_concurrent_requests,
                "io_timeout_ms": self.limits.io_timeout_ms(),
                "max_attempts": self.limits.max_attempts,
            },
            "telemetry": {
                "trace_export": self.trace_export,
                "metrics_export": self.metrics_export,
            },
            "startup": self.startup_json(),
        }))
    }

    /// The start-up reconciliation section. Readiness and introspection share
    /// it.
    fn startup_json(&self) -> Value {
        json!({
            "orphan_blob_reconciliation": reconciler_json(
                self.startup.orphan_blobs,
                ORPHAN_BLOB_RECONCILER_ERROR_TYPE,
            ),
            "publish_intent_reconciliation": reconciler_json(
                self.startup.publish_intents,
                PUBLISH_INTENT_RECONCILER_ERROR_TYPE,
            ),
        })
    }
}

/// One reconciliation entry. Every value is a literal or a number.
fn reconciler_json(outcome: ReconcilerOutcome, error_type: &str) -> Value {
    match outcome {
        ReconcilerOutcome::Completed { affected } => {
            json!({ "outcome": "completed", "affected": affected })
        }
        ReconcilerOutcome::Failed => json!({ "outcome": "failed", "error.type": error_type }),
    }
}

/// An admin response. The code keeps it as plain data. Every endpoint can
/// be tested without a listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminResponse {
    /// HTTP status code.
    pub status: StatusCode,
    /// JSON body.
    pub body: String,
}

impl AdminResponse {
    fn ok(value: Value) -> Self {
        Self {
            status: StatusCode::OK,
            body: value.to_string(),
        }
    }
}

/// A reserved path that names no endpoint.
///
/// The admin layer answers it. S3 does not. An operator who mistypes
/// `/.tele-s3/health` deserves a 404 that names the prefix. The operator
/// does not deserve an `InvalidBucketName` error about a bucket they never
/// asked for.
fn unknown_endpoint_response() -> AdminResponse {
    AdminResponse {
        status: StatusCode::NOT_FOUND,
        body: json!({
            "status": "not_found",
            "error.type": "unknown_admin_endpoint",
            "prefix": ADMIN_PREFIX,
        })
        .to_string(),
    }
}

/// Milliseconds in the IO timeout. The value saturates. It does not wrap.
/// An operator set the Duration. An unreadable number is worse than a
/// clamped one.
trait IoTimeoutMs {
    fn io_timeout_ms(&self) -> u64;
}

impl IoTimeoutMs for GrammersLimits {
    fn io_timeout_ms(&self) -> u64 {
        u64::try_from(self.io_timeout.as_millis()).unwrap_or(u64::MAX)
    }
}

/// The `s3s` hook that answers admin requests before S3 does.
///
/// Register it with `S3ServiceBuilder::set_route`. The hook passes every
/// unmatched request to the S3 pipeline. S3 traffic stays untouched. That
/// includes authentication.
#[derive(Debug, Clone, Copy)]
pub struct AdminRoute {
    state: AdminState,
}

impl AdminRoute {
    /// Build the route around the state to publish.
    #[must_use]
    pub fn new(state: AdminState) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl S3Route for AdminRoute {
    fn is_match(
        &self,
        _method: &Method,
        uri: &Uri,
        _headers: &HeaderMap,
        _extensions: &mut Extensions,
    ) -> bool {
        classify(uri.path()).is_some()
    }

    /// Admin probes are unauthenticated.
    ///
    /// `s3s` verifies a signature only when one is present. An unsigned probe
    /// arrives here without credentials. The default implementation would
    /// answer `AccessDenied`. That default fits S3 operations. It does not fit
    /// a health check. The code overrides it here only. The access stage of
    /// every S3 operation still runs as before.
    async fn check_access(&self, _req: &mut S3Request<Body>) -> S3Result<()> {
        Ok(())
    }

    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        let Some(endpoint) = classify(req.uri.path()) else {
            // Unreachable: `is_match` claimed this request.
            return Err(s3s::s3_error!(
                InternalError,
                "admin route matched a path it does not classify"
            ));
        };

        let response = match endpoint {
            AdminEndpoint::Liveness => self.state.liveness(),
            AdminEndpoint::Readiness => self.state.readiness(),
            AdminEndpoint::Introspection => self.state.introspection(),
            AdminEndpoint::Unknown => unknown_endpoint_response(),
        };

        tracing::debug!(
            endpoint = endpoint.name(),
            status_code = response.status.as_u16(),
            "served admin request"
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );

        let mut out = S3Response::with_headers(Body::from(response.body), headers);
        out.status = Some(response.status);
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used, missing_docs)]
    use super::*;

    use crate::s3::helpers::validate_bucket_name;
    use serde_json::Value;

    /// Sample state. Both reconcilers are healthy.
    fn state() -> AdminState {
        AdminState::new(
            8000,
            GrammersLimits {
                max_concurrent_requests: 4,
                io_timeout: std::time::Duration::from_secs(30),
                max_attempts: 3,
            },
            StartupReport {
                orphan_blobs: ReconcilerOutcome::Completed { affected: 2 },
                publish_intents: ReconcilerOutcome::Completed { affected: 0 },
            },
            true,
            true,
            128,
        )
    }

    fn body_of(response: &AdminResponse) -> Value {
        serde_json::from_str(&response.body).expect("admin bodies are JSON")
    }

    #[test]
    #[allow(clippy::expect_used)]
    fn reserved_paths_reach_the_admin_handler() {
        assert_eq!(classify(LIVENESS_PATH), Some(AdminEndpoint::Liveness));
        assert_eq!(classify(READINESS_PATH), Some(AdminEndpoint::Readiness));
        assert_eq!(
            classify(INTROSPECTION_PATH),
            Some(AdminEndpoint::Introspection)
        );
        // The prefix itself, and a typo under it, are still ours to answer.
        assert_eq!(classify(ADMIN_PREFIX), Some(AdminEndpoint::Unknown));
        assert_eq!(classify("/.tele-s3/health"), Some(AdminEndpoint::Unknown));
    }

    #[test]
    #[allow(clippy::expect_used)]
    fn s3_paths_are_never_admin_requests() {
        for path in [
            "/",
            "/my-bucket",
            "/my-bucket/key",
            "/my-bucket/nested/key.txt",
            "/healthz",
            "/.tele-s3x/key",
            "/not-.tele-s3/key",
            "/my-bucket/.tele-s3/healthz",
        ] {
            assert_eq!(classify(path), None, "{path} must reach S3");
        }
    }

    /// The prefix is reserved because the S3 API cannot accept it. The prefix
    /// cannot shadow a bucket. This test checks the crate's validator. It does
    /// not check a copy of the rule. The test cannot drift from what
    /// `CreateBucket` enforces.
    #[test]
    #[allow(clippy::expect_used)]
    fn the_reserved_prefix_cannot_be_a_bucket_name() {
        validate_bucket_name(".tele-s3").expect_err("a leading dot is not a bucket");
        validate_bucket_name(".tele-s3.healthz").expect_err("a leading dot is not a bucket either");
        // And the rule it relies on: the name has to start with a letter or a
        // digit, so anything under this prefix cannot become a bucket.
        assert!(validate_bucket_name("tele-s3").is_ok());
    }

    #[test]
    #[allow(clippy::expect_used)]
    fn liveness_answers_200_whatever_the_reconcilers_did() {
        for report in [
            StartupReport {
                orphan_blobs: ReconcilerOutcome::Completed { affected: 0 },
                publish_intents: ReconcilerOutcome::Completed { affected: 0 },
            },
            StartupReport {
                orphan_blobs: ReconcilerOutcome::Failed,
                publish_intents: ReconcilerOutcome::Failed,
            },
        ] {
            let state = AdminState::new(8000, GrammersLimits::default(), report, false, false, 128);
            let response = state.liveness();
            assert_eq!(response.status, StatusCode::OK);
            // Nothing about the reconcilers reaches a liveness answer.
            assert_eq!(body_of(&response), json!({ "status": "ok" }));
        }
    }

    #[test]
    #[allow(clippy::expect_used)]
    fn readiness_reports_the_reconciler_outcomes_without_failing() {
        let degraded = AdminState::new(
            8000,
            GrammersLimits::default(),
            StartupReport {
                orphan_blobs: ReconcilerOutcome::Failed,
                publish_intents: ReconcilerOutcome::Completed { affected: 7 },
            },
            false,
            false,
            128,
        );

        let response = degraded.readiness();
        // A failed pass is reported, never fatal: see `AdminState::readiness`.
        assert_eq!(response.status, StatusCode::OK);

        let body = body_of(&response);
        assert_eq!(body["status"], "ready");
        assert_eq!(
            body["startup"]["orphan_blob_reconciliation"]["outcome"],
            "failed"
        );
        assert_eq!(
            body["startup"]["orphan_blob_reconciliation"]["error.type"],
            ORPHAN_BLOB_RECONCILER_ERROR_TYPE
        );
        assert_eq!(
            body["startup"]["publish_intent_reconciliation"],
            json!({ "outcome": "completed", "affected": 7 })
        );
    }

    #[test]
    #[allow(clippy::expect_used)]
    fn introspection_publishes_the_effective_runtime_state() {
        let body = body_of(&state().introspection());

        assert_eq!(body["listen_port"], 8000);
        assert_eq!(body["max_concurrent_connections"], 128);
        assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(body["telegram"]["max_concurrent_requests"], 4);
        assert_eq!(body["telegram"]["io_timeout_ms"], 30_000);
        assert_eq!(body["telegram"]["max_attempts"], 3);
        assert_eq!(body["telemetry"]["trace_export"], true);
        assert_eq!(body["telemetry"]["metrics_export"], true);
        assert_eq!(
            body["startup"]["orphan_blob_reconciliation"],
            json!({ "outcome": "completed", "affected": 2 })
        );
    }

    /// The redaction policy is a whitelist. The introspection document has
    /// exactly these keys. A future field that carries a config value cannot
    /// be added without failing this test.
    #[test]
    #[allow(clippy::expect_used)]
    fn introspection_publishes_only_documented_fields() {
        fn keys(value: &Value) -> Vec<&str> {
            value
                .as_object()
                .expect("objects only")
                .keys()
                .map(String::as_str)
                .collect()
        }

        let body = body_of(&state().introspection());

        let mut top = keys(&body);
        top.sort_unstable();
        assert_eq!(
            top,
            [
                "listen_port",
                "max_concurrent_connections",
                "startup",
                "status",
                "telegram",
                "telemetry",
                "version"
            ]
        );

        let mut telemetry = keys(&body["telemetry"]);
        telemetry.sort_unstable();
        assert_eq!(telemetry, ["metrics_export", "trace_export"]);

        let mut telegram = keys(&body["telegram"]);
        telegram.sort_unstable();
        assert_eq!(
            telegram,
            ["io_timeout_ms", "max_attempts", "max_concurrent_requests"]
        );

        let mut startup = keys(&body["startup"]);
        startup.sort_unstable();
        assert_eq!(
            startup,
            [
                "orphan_blob_reconciliation",
                "publish_intent_reconciliation"
            ]
        );
    }

    /// Every value in the document is a literal or a number. A string from any
    /// other file would be the only way a secret could escape. There is
    /// nowhere for one to come from.
    #[test]
    #[allow(clippy::expect_used)]
    fn introspection_contains_no_free_form_text() {
        fn assert_literal_or_number(value: &Value) {
            match value {
                Value::Number(_) => {}
                // The one boolean in the document: whether spans are leaving
                // this process. It cannot have come from anywhere but this
                // file.
                Value::Bool(_) => {}
                Value::String(text) => assert!(
                    matches!(
                        text.as_str(),
                        "ok" | "ready"
                            | "completed"
                            | "failed"
                            | ORPHAN_BLOB_RECONCILER_ERROR_TYPE
                            | PUBLISH_INTENT_RECONCILER_ERROR_TYPE
                    ) || text == env!("CARGO_PKG_VERSION"),
                    "unexpected string {text:?} in the introspection document"
                ),
                Value::Object(map) => map.values().for_each(assert_literal_or_number),
                other => panic!("unexpected {other:?} in the introspection document"),
            }
        }

        assert_literal_or_number(&body_of(&state().introspection()));
    }

    #[test]
    #[allow(clippy::expect_used)]
    fn an_unknown_admin_path_is_a_404_not_an_s3_error() {
        let response = unknown_endpoint_response();
        assert_eq!(response.status, StatusCode::NOT_FOUND);
        assert_eq!(body_of(&response)["prefix"], ADMIN_PREFIX);
    }
}
