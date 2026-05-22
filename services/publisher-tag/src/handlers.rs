use crate::state::AppState;
use askama::Template;
use axum::{extract::{Query, State}, response::{Html, IntoResponse}};
use core_models::{BidRequest, Device, Impression, Site, User};
use serde::Deserialize;
use std::sync::Arc;
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

#[derive(Deserialize, Debug)]
pub struct AdRequest {
    pub slot_id: String,
    pub dsp_uid: Option<String>,
}

#[derive(Template)]
#[template(path = "ad_markup.html")]
struct AdTemplate {
    tracker_url: String,
    campaign_id: String,
    bid_id: String,
    price: f64,
}

/// The Mock SSP Layer: Translates a standard HTTP request into OpenRTB
#[instrument(skip(state), fields(slot = %params.slot_id))] // Automatically injects trace IDs into logs
pub async fn handle_ad_request(
    State(state): State<Arc<AppState>>,
    Query(params): Query<AdRequest>,
) -> impl IntoResponse {

    let auction_id = Uuid::new_v4().to_string();
    info!(auction_id = %auction_id, "Initiating OpenRTB auction");

    let bid_req = BidRequest {
        id: auction_id.clone(),
        imp: vec![Impression {
            id: params.slot_id.clone(),
            bidfloor: Some(1.00),
        }],
        site: Some(Site {
            id: Some("pub-espn-123".into()),
            domain: Some("espn.com".into()),
        }),
        device: Some(Device {
            ua: Some("Mozilla/5.0 (Windows NT 10.0; Win64; x64)".into()),
            ip: Some("192.168.1.1".into()),
            ifa: None,
        }),
        user: Some(User {
            id: Some("publisher-user-id".into()),
            buyeruid: params.dsp_uid,
        }),
    };

    match state.http_client.post(&state.rtb_url).json(&bid_req).send().await {
        Ok(response) if response.status().is_success() => {
            if let Ok(bid_res) = response.json::<core_models::BidResponse>().await {
                if let Some(seatbid) = bid_res.seatbid.first() {
                    if let Some(bid) = seatbid.bid.first() {

                        let adid = bid.adid.clone().unwrap_or_default();
                        let campaign_id = adid.replace("ad-", "");

                        info!(campaign = %campaign_id, price = %bid.price, "Auction won");

                        let template = AdTemplate {
                            tracker_url: state.tracker_url.clone(),
                            campaign_id,
                            bid_id: bid.id.clone(),
                            price: bid.price,
                        };

                        // Render the HTML cleanly at compile time
                        return Html(template.render().unwrap_or_else(|_| "".into()));
                    }
                }
            }
            warn!("RTB Engine returned 200 but no valid bid was found");
        }
        Ok(response) => warn!(status = %response.status(), "RTB Engine returned non-200 status"),
        Err(e) => error!(error = %e, "RTB Engine request failed (Timeout or Network Error)"),
    }

    info!("No bid returned, returning empty ad slot");
    Html("".to_string())
}