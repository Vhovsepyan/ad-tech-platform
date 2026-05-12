use axum::http::StatusCode;

/// Liveness probe for Load Balancers (e.g., AWS ALB or Kubernetes)
pub async fn healthz() -> StatusCode {
    StatusCode::OK
}