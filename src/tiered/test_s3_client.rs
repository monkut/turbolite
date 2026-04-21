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
