use crate::state::AppState;
use axum::{
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
};
use core_models::BidRequest;
use std::sync::Arc;

/// Core RTB Endpoint handling OpenRTB 2.6 requests
pub async fn handle_bid(
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> impl IntoResponse {
    match serde_json::from_slice::<BidRequest>(&body) {
        Ok(bid_request) => {
            // FIX: Add `.await` because BiddingStrategy is now async
            match state.bidding_engine.evaluate(&bid_request).await {
                Some(response) => {
                    match serde_json::to_vec(&response) {
                        Ok(json_bytes) => (StatusCode::OK, json_bytes).into_response(),
                        Err(e) => {
                            println!("Serialization error: {}", e);
                            StatusCode::INTERNAL_SERVER_ERROR.into_response()
                        }
                    }
                }
                None => StatusCode::NO_CONTENT.into_response(),
            }
        }
        Err(e) => {
            println!("Failed to parse OpenRTB request: {}", e);
            StatusCode::BAD_REQUEST.into_response()
        }
    }
}