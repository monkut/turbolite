#[test]
fn test_s3_key_format() {
    // Verify the key format is "p/d/{gid}_v{version}"
    let key = format!("p/d/{}_v{}", 5, 3);
    assert_eq!(key, "p/d/5_v3");
    let key0 = format!("p/d/{}_v{}", 0, 1);
    assert_eq!(key0, "p/d/0_v1");
}

// ── ManifestCasError unit tests (Stage A) ──
//
// The primitives themselves can't be exercised without a live S3 endpoint;
// these tests cover the error type's public contract so downstream callers
// writing retry loops can rely on it.

#[cfg(feature = "cloud")]
#[test]
fn manifest_cas_error_precondition_failed_display() {
    let err = super::ManifestCasError::PreconditionFailed;
    assert!(err.to_string().contains("precondition failed"));
    // PreconditionFailed is the *signal* for retry — it has no source.
    assert!(std::error::Error::source(&err).is_none());
}

#[cfg(feature = "cloud")]
#[test]
fn manifest_cas_error_io_wraps_source() {
    let io_err = std::io::Error::new(std::io::ErrorKind::TimedOut, "request timed out");
    let cas_err: super::ManifestCasError = io_err.into();
    match &cas_err {
        super::ManifestCasError::Io(inner) => {
            assert_eq!(inner.kind(), std::io::ErrorKind::TimedOut);
        }
        super::ManifestCasError::PreconditionFailed => {
            panic!("expected Io variant")
        }
    }
    // The io::Error is exposed via Error::source so callers can downcast.
    assert!(std::error::Error::source(&cas_err).is_some());
}

// ── SDK-layer mock tests ──
//
// These wire an `aws_sdk_s3::Client` to a `StaticReplayClient` so we can
// make S3 return canned responses (412, 200 with ETag, etc.) and verify
// that `put_manifest_conditional` maps them correctly onto
// `ManifestCasError`. They don't touch real S3 or any local emulator —
// just prove the wiring between the aws-sdk response parsing, our
// `is_precondition_failed` predicate, and the `ManifestCasError` enum.
//
// Verifying the full end-to-end CAS (two concurrent writers against a
// compliant S3 endpoint) still requires real AWS — see rustyhip's
// `tests/cas_conflict.rs` for that integration path.

#[cfg(feature = "cloud")]
fn build_s3_client_with_mock(
    http: aws_smithy_runtime::client::http::test_util::StaticReplayClient,
) -> super::S3Client {
    use std::sync::atomic::AtomicU64;

    let creds = aws_sdk_s3::config::Credentials::new("test", "test", None, None, "static-test");
    let sdk_config = aws_config::SdkConfig::builder()
        .http_client(http)
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .credentials_provider(aws_sdk_s3::config::SharedCredentialsProvider::new(creds))
        .behavior_version(aws_config::BehaviorVersion::latest())
        .build();
    let client = aws_sdk_s3::Client::new(&sdk_config);

    super::S3Client {
        client,
        bucket: "test-bucket".into(),
        prefix: "test-prefix".into(),
        runtime: tokio::runtime::Handle::current(),
        fetch_count: AtomicU64::new(0),
        fetch_bytes: AtomicU64::new(0),
        put_count: AtomicU64::new(0),
        put_bytes: AtomicU64::new(0),
    }
}

#[cfg(feature = "cloud")]
fn mock_put_response(status: u16, etag: Option<&str>, body: &'static str) -> http::Response<aws_smithy_types::body::SdkBody> {
    let mut builder = http::Response::builder().status(status);
    if let Some(e) = etag {
        builder = builder.header("ETag", e);
    }
    builder.body(aws_smithy_types::body::SdkBody::from(body)).unwrap()
}

#[cfg(feature = "cloud")]
fn mock_request() -> http::Request<aws_smithy_types::body::SdkBody> {
    // StaticReplayClient matches replay events in order and doesn't strictly
    // validate the request; any placeholder request works.
    http::Request::builder()
        .method("PUT")
        .uri("http://mock/unused")
        .body(aws_smithy_types::body::SdkBody::empty())
        .unwrap()
}

#[cfg(feature = "cloud")]
#[tokio::test(flavor = "multi_thread")]
async fn put_manifest_conditional_412_maps_to_precondition_failed() {
    use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};

    let http = StaticReplayClient::new(vec![ReplayEvent::new(
        mock_request(),
        mock_put_response(
            412,
            None,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <Error><Code>PreconditionFailed</Code>\
             <Message>At least one of the pre-conditions you specified did not hold</Message>\
             </Error>",
        ),
    )]);
    let s3 = build_s3_client_with_mock(http);
    let manifest = super::Manifest::empty();

    let result = s3.put_manifest_conditional_async(&manifest, Some("\"stale-etag\"")).await;

    match result {
        Err(super::ManifestCasError::PreconditionFailed) => {}
        Err(super::ManifestCasError::Io(e)) => {
            panic!("expected PreconditionFailed, got Io({}): {:?}", e, e);
        }
        Ok(new_etag) => panic!("expected PreconditionFailed, got Ok({new_etag:?})"),
    }
}

#[cfg(feature = "cloud")]
#[tokio::test(flavor = "multi_thread")]
async fn put_manifest_conditional_200_returns_new_etag() {
    use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};

    let http = StaticReplayClient::new(vec![ReplayEvent::new(
        mock_request(),
        mock_put_response(200, Some("\"new-etag-xyz\""), ""),
    )]);
    let s3 = build_s3_client_with_mock(http);
    let manifest = super::Manifest::empty();

    let result = s3
        .put_manifest_conditional_async(&manifest, Some("\"cached-etag\""))
        .await
        .expect("conditional PUT should succeed on 200");

    assert_eq!(
        result.as_deref(),
        Some("\"new-etag-xyz\""),
        "new ETag from response should propagate to caller"
    );
}

#[cfg(feature = "cloud")]
#[tokio::test(flavor = "multi_thread")]
async fn commit_manifest_updates_etag_cell_on_success_and_clears_on_412() {
    use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};

    // Two replay events: first succeeds with a new ETag; second returns 412.
    let http = StaticReplayClient::new(vec![
        ReplayEvent::new(
            mock_request(),
            mock_put_response(200, Some("\"commit-1-etag\""), ""),
        ),
        ReplayEvent::new(
            mock_request(),
            mock_put_response(
                412,
                None,
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                 <Error><Code>PreconditionFailed</Code></Error>",
            ),
        ),
    ]);
    let s3 = build_s3_client_with_mock(http);
    let manifest = super::Manifest::empty();
    let etag_cell = std::sync::Mutex::new(Some("\"initial-etag\"".to_string()));

    // First commit: succeeds, cell should update to the new ETag.
    // `commit_manifest` is the blocking wrapper; in an async test it must
    // run via `block_in_place` so we don't deadlock the runtime.
    tokio::task::block_in_place(|| {
        s3.commit_manifest(&manifest, &etag_cell).expect("first commit succeeds");
    });
    assert_eq!(
        etag_cell.lock().unwrap().as_deref(),
        Some("\"commit-1-etag\""),
        "etag cell should update to the new ETag after successful commit"
    );

    // Second commit: fails with 412, cell should clear to None.
    let err = tokio::task::block_in_place(|| {
        s3.commit_manifest(&manifest, &etag_cell)
            .expect_err("second commit should surface 412 as io::Error")
    });
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    assert!(
        err.to_string().contains("manifest CAS precondition failed"),
        "error message should be the CAS-precondition text, got: {err}"
    );
    assert!(
        etag_cell.lock().unwrap().is_none(),
        "etag cell should clear on 412 so the next write doesn't blindly retry the stale etag"
    );
}
