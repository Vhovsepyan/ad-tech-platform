use crate::store::AudienceStore;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Redirect},
    Json,
};
use axum_extra::extract::cookie::{Cookie, CookieJar};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

const DSP_COOKIE_NAME: &str = "dsp_uid";

#[derive(Deserialize)]
pub struct SyncParams {
    pub exchange: String,    // e.g., "appnexus"
    pub ex_uid: String,      // The exchange's user ID
    pub return_url: String,  // Where to redirect back to
}

/// PUBLIC: The Cookie Sync Endpoint
pub async fn cookie_sync(
    State(store): State<Arc<AudienceStore>>,
    jar: CookieJar,
    Query(params): Query<SyncParams>,
) -> impl IntoResponse {

    // 1. Check if we already know this user
    let (dsp_uid, updated_jar) = match jar.get(DSP_COOKIE_NAME) {
        Some(cookie) => (cookie.value().to_string(), jar),
        None => {
            // New user! Generate an ID and set a highly-secure tracking cookie
            let new_uid = Uuid::new_v4().to_string();
            let cookie = Cookie::build((DSP_COOKIE_NAME, new_uid.clone()))
                .domain(".yourdsp.com")
                .path("/")
                .secure(true)
                .http_only(true)
                .same_site(axum_extra::extract::cookie::SameSite::None)
                .build();
            (new_uid, jar.add(cookie))
        }
    };

    // 2. Asynchronously map the Exchange ID to our DSP ID
    let store_clone = store.clone();
    let exchange = params.exchange.clone();
    let ex_uid = params.ex_uid.clone();
    let dsp_uid_clone = dsp_uid.clone();

    tokio::spawn(async move {
        store_clone.map_user(&exchange, &ex_uid, &dsp_uid_clone).await;
    });

    // 3. Redirect back to the exchange with our DSP ID attached
    let final_url = params.return_url.replace("${DSP_UID}", &dsp_uid);
    (updated_jar, Redirect::temporary(&final_url))
}

#[derive(Serialize)]
pub struct AudienceResponse {
    pub segments: Vec<String>,
}

/// INTERNAL: Called by the RTB Engine during an auction
pub async fn lookup_audience(
    State(store): State<Arc<AudienceStore>>,
    axum::extract::Path(dsp_uid): axum::extract::Path<String>,
) -> impl IntoResponse {
    let segments = store.get_segments(&dsp_uid).await;

    (StatusCode::OK, Json(AudienceResponse { segments }))
}