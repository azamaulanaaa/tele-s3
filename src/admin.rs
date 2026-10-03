//! Health, readiness and introspection endpoints.
//!
//! They are served from the S3 listener, ahead of the S3 handlers, and are
//! deliberately tiny: three JSON documents an orchestrator or an operator can
//! read without knowing anything about this crate.
//!
//! * **Liveness** — the process is alive and answering HTTP.
//! * **Readiness** — start-up finished and S3 traffic is being served.
//! * **Introspection** — the effective runtime state: the Telegram tuning
//!   knobs, the listen port and what each start-up reconciler did.
//!
//! None of them touch the database or Telegram, so a rate-limited upstream
//! can never make a perfectly healthy server look broken to an orchestrator.
//!
//! # Authentication
//!
//! These endpoints are **unauthenticated**. A probe that has to sign a request
//! is a probe that stops working the first time it expires, which is the
//! opposite of what a health check is for. Nothing sensitive is published:
//! see [`AdminState`] for what the introspection document is allowed to carry.
//!
//! # Path safety
//!
//! S3 addresses objects at `/{bucket}/{key}`, so any path a client chooses can
//! name a bucket. A bare `/healthz` would therefore be read as bucket
//! `healthz`. Every admin path starts with [`ADMIN_PREFIX`], whose leading `.`
//! makes it unreachable as a bucket name — see that constant's documentation.

use crate::backend::GrammersLimits;

use http::{Extensions, HeaderMap, Method, StatusCode, Uri};
use s3s::{Body, S3Request, S3Response, S3Result, route::S3Route};
use serde_json::{Value, json};

/// Reserved prefix for every admin endpoint.
///
/// The leading `.` is the whole point. An S3 bucket name must begin and end
/// with a lowercase letter or a digit — the crate's own
/// `validate_bucket_name` rejects anything else — so `.tele-s3` can never be
/// a bucket, and no `PUT /.tele-s3/...` can ever create one. Under the S3
/// path grammar the first segment of a request is the bucket, so nothing an
/// S3 client can legitimately ask for lands inside this prefix.
pub const ADMIN_PREFIX: &str = "/.tele-s3";

/// Liveness: is this process alive and serving HTTP?
pub const LIVENESS_PATH: &str = "/.tele-s3/healthz";

/// Readiness: has start-up finished, so that S3 traffic is being served?
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
    /// The reserved prefix, but no endpoint by that name.
    Unknown,
}

impl AdminEndpoint {
    /// Stable name for the endpoint, used as a tracing field.
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
/// `None` means "not an admin request": the caller must let the request
/// through to S3 untouched.
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
/// Matched on a segment boundary, so a bucket called `.tele-s3x` — itself
/// impossible, but worth not assuming — cannot claim the prefix.
fn is_reserved(path: &str) -> bool {
    path.strip_prefix(ADMIN_PREFIX)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
}

/// What one start-up reconciliation pass did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcilerOutcome {
    /// The pass ran. `affected` rows were reconciled (or cleared); zero is a
    /// perfectly ordinary result, not a failure.
    Completed { affected: usize },
    /// The pass returned an error.
    ///
    /// The message is deliberately not carried: it is free-form operator text
    /// that could quote anything the backend had in hand, and this document
    /// is served unauthenticated. Which pass failed is what an operator needs,
    /// and it is a closed set — the [`error.type`](ORPHAN_BLOB_RECONCILER_ERROR_TYPE)
    /// name. The message is logged at start-up, where it belongs.
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
/// The field list *is* the redaction policy: it holds a port, three numbers
/// and two closed enums, so there is no path by which `auth_access_key`,
/// `auth_secret_key`, `api_hash`, `bot_token`, `database_uri` or any other
/// config value — in whole, in part, or wrapped in an error string — reaches
/// a response. Adding a field here is the only way to change that, and the
/// whitelist test is what makes it a deliberate act.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdminState {
    listen_port: u16,
    limits: GrammersLimits,
    startup: StartupReport,
    trace_export: bool,
}

impl AdminState {
    /// Capture the state to publish.
    ///
    /// Takes the port and the limits by value rather than a `&Config` on
    /// purpose: there is no argument through which a credential could arrive.
    ///
    /// `trace_export` is a bool and not the endpoint itself, because the
    /// endpoint is operator infrastructure — often an in-cluster address with
    /// a hostname in it — and this document is served unauthenticated. What
    /// an operator needs to know from a probe is whether spans are leaving
    /// the process at all.
    pub fn new(
        listen_port: u16,
        limits: GrammersLimits,
        startup: StartupReport,
        trace_export: bool,
    ) -> Self {
        Self {
            listen_port,
            limits,
            startup,
            trace_export,
        }
    }

    /// Liveness.
    ///
    /// Always [`StatusCode::OK`], and derived from nothing at all: not
    /// Telegram reachability, not the database, not the reconcilers. A server
    /// answering this question at all is alive by definition, and a Telegram
    /// rate limit must never look like a dead process to whatever is
    /// supervising it — that would turn a throttled upstream into a restart
    /// loop over a healthy server.
    pub fn liveness(&self) -> AdminResponse {
        AdminResponse::ok(json!({ "status": "ok" }))
    }

    /// Readiness.
    ///
    /// Also always [`StatusCode::OK`], by a deliberate decision rather than
    /// by accident: reaching this handler already means start-up ran to
    /// completion, because reconciliation finishes before the listener binds.
    /// So the question "is reconciliation done?" has exactly one answer here,
    /// and the only thing left to report is *how it went*.
    ///
    /// A reconciler failure does **not** clear readiness. Both passes are
    /// best-effort garbage collection: a failed pass leaves unreferenced rows
    /// behind, which no listing shows and which the next pass can still
    /// reclaim. Reads and writes are served from the database and Telegram
    /// either way. Reporting a failed pass as "not ready" would let an
    /// orchestrator stop routing traffic to a server that is fully capable of
    /// serving it — converting a garbage-collection nuisance into an outage,
    /// with no remedy available short of restarting the very process that
    /// would fix it. The outcome is published in the body and logged at
    /// start-up instead, where an operator can act on it.
    pub fn readiness(&self) -> AdminResponse {
        AdminResponse::ok(json!({
            "status": "ready",
            "startup": self.startup_json(),
        }))
    }

    /// Introspection: the effective runtime state.
    ///
    /// What an operator needs when a request is failing and the config file
    /// is three environments away: the three Telegram knobs actually in force
    /// (after environment overrides and clamping), the port actually bound,
    /// and what start-up reconciliation achieved. No database counts — those
    /// would put a live query on an unauthenticated probe path, which is a
    /// poor trade for a number nobody needs to make a decision.
    pub fn introspection(&self) -> AdminResponse {
        AdminResponse::ok(json!({
            "status": "ok",
            "version": env!("CARGO_PKG_VERSION"),
            "listen_port": self.listen_port,
            "telegram": {
                // The same names `init_tracing` publishes, so a log line and
                // this document can be read with one vocabulary.
                "max_concurrent_requests": self.limits.max_concurrent_requests,
                "io_timeout_ms": self.limits.io_timeout_ms(),
                "max_attempts": self.limits.max_attempts,
            },
            "telemetry": {
                "trace_export": self.trace_export,
            },
            "startup": self.startup_json(),
        }))
    }

    /// The start-up reconciliation section, shared by readiness and
    /// introspection.
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

/// An admin response, kept as plain data so every endpoint can be tested
/// without a listener.
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
/// Answered here rather than by S3: an operator who mistypes
/// `/.tele-s3/health` deserves a 404 naming the prefix, not an
/// `InvalidBucketName` about a bucket they never asked for.
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

/// Milliseconds in the IO timeout, saturating rather than wrapping: the value
/// is a Duration an operator set, and an unreadable number is worse than a
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
/// Registered with `S3ServiceBuilder::set_route`. Every unmatched request is
/// passed straight through to the S3 pipeline, so S3 traffic — including its
/// authentication — is untouched.
#[derive(Debug, Clone, Copy)]
pub struct AdminRoute {
    state: AdminState,
}

impl AdminRoute {
    /// Build the route around the state to publish.
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
    /// `s3s` verifies a signature only when one is present, so an unsigned
    /// probe arrives here without credentials and the default implementation
    /// would answer `AccessDenied`. That default is right for S3 operations
    /// and wrong for a health check, so it is overridden — and only here: the
    /// access stage of every S3 operation still runs exactly as before.
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
    use super::*;

    use crate::s3::helpers::validate_bucket_name;
    use serde_json::Value;

    /// Sample state with both reconcilers healthy.
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
        )
    }

    fn body_of(response: &AdminResponse) -> Value {
        serde_json::from_str(&response.body).expect("admin bodies are JSON")
    }

    #[test]
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

    /// The reason the prefix is reserved the way it is: it is not a name the
    /// S3 API can ever accept, so it cannot shadow a bucket. Checked against
    /// the crate's own validator rather than a copy of the rule, so this test
    /// cannot drift from what `CreateBucket` enforces.
    #[test]
    fn the_reserved_prefix_cannot_be_a_bucket_name() {
        validate_bucket_name(".tele-s3").expect_err("a leading dot is not a bucket");
        validate_bucket_name(".tele-s3.healthz").expect_err("a leading dot is not a bucket either");
        // And the rule it relies on: the name has to start with a letter or a
        // digit, so anything under this prefix cannot become a bucket.
        assert!(validate_bucket_name("tele-s3").is_ok());
    }

    #[test]
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
            let state = AdminState::new(8000, GrammersLimits::default(), report, false);
            let response = state.liveness();
            assert_eq!(response.status, StatusCode::OK);
            // Nothing about the reconcilers reaches a liveness answer.
            assert_eq!(body_of(&response), json!({ "status": "ok" }));
        }
    }

    #[test]
    fn readiness_reports_the_reconciler_outcomes_without_failing() {
        let degraded = AdminState::new(
            8000,
            GrammersLimits::default(),
            StartupReport {
                orphan_blobs: ReconcilerOutcome::Failed,
                publish_intents: ReconcilerOutcome::Completed { affected: 7 },
            },
            false,
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
    fn introspection_publishes_the_effective_runtime_state() {
        let body = body_of(&state().introspection());

        assert_eq!(body["listen_port"], 8000);
        assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(body["telegram"]["max_concurrent_requests"], 4);
        assert_eq!(body["telegram"]["io_timeout_ms"], 30_000);
        assert_eq!(body["telegram"]["max_attempts"], 3);
        assert_eq!(body["telemetry"]["trace_export"], true);
        assert_eq!(
            body["startup"]["orphan_blob_reconciliation"],
            json!({ "outcome": "completed", "affected": 2 })
        );
    }

    /// The redaction policy, enforced as a whitelist: the introspection
    /// document has exactly these keys, so a future field that carried a
    /// config value could not be added without failing this test.
    #[test]
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
                "startup",
                "status",
                "telegram",
                "telemetry",
                "version"
            ]
        );

        let mut telemetry = keys(&body["telemetry"]);
        telemetry.sort_unstable();
        assert_eq!(telemetry, ["trace_export"]);

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

    /// Every value in the document is a literal or a number. A string that
    /// came from anywhere but this file would be the only way a secret could
    /// escape, and there is nowhere for one to come from.
    #[test]
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
    fn an_unknown_admin_path_is_a_404_not_an_s3_error() {
        let response = unknown_endpoint_response();
        assert_eq!(response.status, StatusCode::NOT_FOUND);
        assert_eq!(body_of(&response)["prefix"], ADMIN_PREFIX);
    }
}
