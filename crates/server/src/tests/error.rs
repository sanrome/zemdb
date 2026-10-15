use super::*;
use axum::http::header::RETRY_AFTER;

#[test]
fn each_error_maps_to_its_code_and_status() {
    use ErrorCode as C;
    use ServerError::*;
    use StatusCode as S;
    let e = |variant: fn(String) -> ServerError| variant(String::new());
    let io = Io(std::io::Error::other("disk"));
    let version = e(ProtocolVersionMismatch);
    let invalid_sequence = InvalidSequence {
        expected: SequenceNumber::new(10),
        actual: SequenceNumber::new(20),
    };
    // (error, code, status), one row per variant.
    let cases = [
        (io, C::Internal, S::INTERNAL_SERVER_ERROR),
        (e(Wal), C::Internal, S::INTERNAL_SERVER_ERROR),
        (e(WalCorruption), C::Internal, S::INTERNAL_SERVER_ERROR),
        (e(Config), C::Internal, S::INTERNAL_SERVER_ERROR),
        (e(RoomNotFound), C::RoomNotFound, S::NOT_FOUND),
        (e(RoomAlreadyExists), C::RoomAlreadyExists, S::CONFLICT),
        (e(SchemaNotFound), C::SchemaNotFound, S::NOT_FOUND),
        (e(SchemaAlreadyExists), C::SchemaAlreadyExists, S::CONFLICT),
        (e(SchemaViolation), C::SchemaViolation, S::BAD_REQUEST),
        (BehindCompaction, C::BehindCompaction, S::GONE),
        (e(ClientNotRegistered), C::ClientNotRegistered, S::CONFLICT),
        (e(RoomLocked), C::RoomLocked, S::LOCKED),
        (e(Unauthorized), C::Unauthorized, S::UNAUTHORIZED),
        (e(Forbidden), C::Forbidden, S::FORBIDDEN),
        (RateLimited, C::RateLimited, S::TOO_MANY_REQUESTS),
        (e(Serialization), C::Internal, S::INTERNAL_SERVER_ERROR),
        (e(Internal), C::Internal, S::INTERNAL_SERVER_ERROR),
        (e(Unavailable), C::Unavailable, S::SERVICE_UNAVAILABLE),
        (e(Timeout), C::Timeout, S::GATEWAY_TIMEOUT),
        (e(RequestTimeout), C::RequestTimeout, S::REQUEST_TIMEOUT),
        (invalid_sequence, C::InvalidSequence, S::BAD_REQUEST),
        (version, C::ProtocolVersionMismatch, S::BAD_REQUEST),
        (e(BadRequest), C::BadRequest, S::BAD_REQUEST),
        (e(SnapshotSuperseded), C::SnapshotSuperseded, S::CONFLICT),
        (e(RoomReset), C::RoomReset, S::CONFLICT),
    ];
    for (err, code, status) in cases {
        assert_eq!(err.to_error_code(), code, "{err:?}");
        assert_eq!(err.to_status_code(), status, "{err:?}");
    }
}

#[test]
fn decode_errors_are_client_errors() {
    let version: ServerError = DecodeError::UnsupportedVersion {
        expected: 1,
        got: 2,
    }
    .into();
    assert_eq!(version.to_error_code(), ErrorCode::ProtocolVersionMismatch);
    assert_eq!(version.to_status_code(), StatusCode::BAD_REQUEST);

    for err in [
        DecodeError::TooShort { len: 1 },
        DecodeError::TooLarge { len: 1 << 30 },
        DecodeError::InvalidMagic { got: [0, 0] },
        DecodeError::TooManyValues { max: 1 << 20 },
    ] {
        let err: ServerError = err.into();
        assert_eq!(err.to_error_code(), ErrorCode::BadRequest);
        assert_eq!(err.to_status_code(), StatusCode::BAD_REQUEST);
    }
}

#[test]
fn json_unavailable_response_carries_retry_after() {
    let response = ServerError::Unavailable("room restarting".to_string()).into_response();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()[RETRY_AFTER], "1");

    let response = ServerError::Internal("bug".to_string()).into_response();
    assert!(response.headers().get(RETRY_AFTER).is_none());
}
