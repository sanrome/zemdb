use super::*;
use axum::http::header::RETRY_AFTER;

#[test]
fn each_error_maps_to_its_code_and_status() {
    let cases = [
        (
            ServerError::Unauthorized(String::new()),
            ErrorCode::Unauthorized,
            StatusCode::UNAUTHORIZED,
        ),
        (
            ServerError::Forbidden(String::new()),
            ErrorCode::Forbidden,
            StatusCode::FORBIDDEN,
        ),
        (
            ServerError::ClientNotRegistered(String::new()),
            ErrorCode::ClientNotRegistered,
            StatusCode::CONFLICT,
        ),
        (
            ServerError::ProtocolVersionMismatch(String::new()),
            ErrorCode::ProtocolVersionMismatch,
            StatusCode::BAD_REQUEST,
        ),
        (
            ServerError::Unavailable(String::new()),
            ErrorCode::Unavailable,
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            ServerError::Timeout(String::new()),
            ErrorCode::Timeout,
            StatusCode::GATEWAY_TIMEOUT,
        ),
        (
            ServerError::SchemaAlreadyExists(String::new()),
            ErrorCode::SchemaAlreadyExists,
            StatusCode::CONFLICT,
        ),
        (
            ServerError::RequestTimeout(String::new()),
            ErrorCode::RequestTimeout,
            StatusCode::REQUEST_TIMEOUT,
        ),
        (
            ServerError::Internal(String::new()),
            ErrorCode::Internal,
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
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
