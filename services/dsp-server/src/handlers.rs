use crate::{store::CreativeStore, templating::MacroEngine};
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{Html, IntoResponse},
};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::sync::Arc;
use tracing::{error, instrument};

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<CreativeStore>,
    pub macro_engine: Arc<MacroEngine>,
    pub tracker_url: String,
}

#[derive(Deserialize, Debug)]
pub struct RenderParams {
    pub auction_id: String,
    pub price: Option<Decimal>,
    pub r: Option<String>,
}

#[instrument(skip(state), fields(creative = %creative_id))]
pub async fn serve_creative(
    State(state): State<AppState>,
    Path(creative_id): Path<String>,
    Query(params): Query<RenderParams>,
) -> impl IntoResponse {

    // 1. Fetch raw HTML from L1/L2 Cache
    let Some(raw_html) = state.store.get_creative(&creative_id).await else {
        error!("Creative not found in cache");
        return (StatusCode::NOT_FOUND, Html("".to_string())).into_response();
    };

    // 2. Extract Campaign ID (Assuming creative_id format is "crid-<campaign_id>")
    let campaign_id = creative_id.replace("crid-", "");
    let price_str = params.price.unwrap_or(Decimal::ZERO).to_string();
    let redirect_query_param = match &params.r {
        Some(url) => format!("&r={}", url),
        None => "".to_string(),
    };

    // 3. O(n) Macro Substitution
    let rendered_html = state.macro_engine.render(
        &raw_html,
        &params.auction_id,
        &price_str,
        &campaign_id,
        &state.tracker_url,
        &redirect_query_param,
    );

    // 4. Set CDN/Browser Caching Headers
    // We tell the browser it can cache this SPECIFIC rendered ad for 60 seconds
    // to prevent duplicate network calls if the user scrolls up and down.
    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, "public, max-age=60".parse().unwrap());
    headers.insert(header::CONTENT_TYPE, "text/html; charset=utf-8".parse().unwrap());

    (headers, rendered_html).into_response()
}