use super::*;

fn config_with(auth: &str, admin: &str) -> ServerConfig {
    ServerConfig {
        auth_secret: auth.to_string(),
        admin_secret: admin.to_string(),
        ..ServerConfig::default()
    }
}

const STRONG_AUTH: &str = "9f2c7e1a5b8d3f6e0a4c7b1d9e2f5a8c";
const STRONG_ADMIN: &str = "3b6e9a2d5c8f1e4a7d0b3c6f9e2a5d8b";

#[test]
fn built_in_development_secrets_are_rejected() {
    assert!(ServerConfig::default().validate_secrets().is_err());
    assert!(config_with(STRONG_AUTH, &default_admin_secret())
        .validate_secrets()
        .is_err());
    assert!(config_with(&default_auth_secret(), STRONG_ADMIN)
        .validate_secrets()
        .is_err());
}

#[test]
fn empty_and_short_secrets_are_rejected() {
    assert!(config_with("", STRONG_ADMIN).validate_secrets().is_err());
    assert!(config_with(STRONG_AUTH, "").validate_secrets().is_err());
    let short = "x".repeat(MIN_SECRET_LEN - 1);
    assert!(config_with(&short, STRONG_ADMIN)
        .validate_secrets()
        .is_err());
    assert!(config_with(STRONG_AUTH, &short).validate_secrets().is_err());
}

#[test]
fn identical_auth_and_admin_secrets_are_rejected() {
    assert!(config_with(STRONG_AUTH, STRONG_AUTH)
        .validate_secrets()
        .is_err());
}

#[test]
fn strong_distinct_secrets_are_accepted() {
    assert!(config_with(STRONG_AUTH, STRONG_ADMIN)
        .validate_secrets()
        .is_ok());
}

#[test]
fn rejection_does_not_echo_the_secret() {
    let short = "short-secret-value";
    let err = config_with(short, STRONG_ADMIN)
        .validate_secrets()
        .unwrap_err()
        .to_string();
    assert!(!err.contains(short), "error leaks the secret: {err}");
}
