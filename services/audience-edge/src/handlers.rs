use crate::state::AppState;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
    Json,
};
use axum_extra::extract::cookie::{Cookie, CookieJar};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

const DSP_COOKIE_NAME: &str = "dsp_uid";

#[derive(Deserialize)]
pub struct SyncParams {
    pub exchange: String,
    pub ex_uid: String,
    pub return_url: String,
}

/// Returns true only when `url_str` is an HTTPS URL whose host is in the allowlist.
/// Validated against the raw template (before `${DSP_UID}` substitution) so the
/// placeholder in the query string does not interfere with host extraction.
fn is_allowed_return_url(url_str: &str, allowed_hosts: &HashSet<String>) -> bool {
    let Ok(parsed) = url::Url::parse(url_str) else {
        return false;
    };
    if parsed.scheme() != "https" {
        return false;
    }
    let Some(host) = parsed.host_str() else {
        return false;
    };
    allowed_hosts.contains(host)
}

/// PUBLIC: The Cookie Sync Endpoint
pub async fn cookie_sync(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Query(params): Query<SyncParams>,
) -> Response {
    // 1. Validate return_url against the allowlist before doing anything else.
    if !is_allowed_return_url(&params.return_url, &state.allowed_redirect_hosts) {
        return StatusCode::BAD_REQUEST.into_response();
    }

    // 2. Check if we already know this user
    let (dsp_uid, updated_jar) = match jar.get(DSP_COOKIE_NAME) {
        Some(cookie) => (cookie.value().to_string(), jar),
        None => {
            let new_uid = Uuid::new_v4().to_string();
            let cookie = Cookie::build((DSP_COOKIE_NAME, new_uid.clone()))
                .domain(state.cookie_domain.clone())
                .path("/")
                .secure(true)
                .http_only(true)
                .same_site(axum_extra::extract::cookie::SameSite::None)
                .build();
            (new_uid, jar.add(cookie))
        }
    };

    // 3. Asynchronously map the Exchange ID to our DSP ID
    let store_clone = state.store.clone();
    let exchange = params.exchange.clone();
    let ex_uid = params.ex_uid.clone();
    let dsp_uid_clone = dsp_uid.clone();

    tokio::spawn(async move {
        store_clone.map_user(&exchange, &ex_uid, &dsp_uid_clone).await;
    });

    // 4. Redirect back to the exchange with our DSP ID attached
    let final_url = params.return_url.replace("${DSP_UID}", &dsp_uid);
    (updated_jar, Redirect::temporary(&final_url)).into_response()
}

#[derive(Serialize)]
pub struct AudienceResponse {
    pub segments: Vec<String>,
}

/// INTERNAL: Called by the RTB Engine during an auction
pub async fn lookup_audience(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(dsp_uid): axum::extract::Path<String>,
) -> impl IntoResponse {
    let segments = state.store.get_segments(&dsp_uid).await;

    (StatusCode::OK, Json(AudienceResponse { segments }))
}