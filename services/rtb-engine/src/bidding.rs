use async_trait::async_trait;
use core_models::{Bid, BidRequest, BidResponse, SeatBid};
use redis_utils::RedisManager;
use rust_decimal::Decimal;
use serde_json::Value;

#[derive(serde::Deserialize)]
struct AudienceResponse {
    segments: Vec<String>,
}

/// Core async trait for pluggable bidding strategies
#[async_trait]
pub trait BiddingStrategy: Send + Sync {
    // Native async trait support requires Rust 1.75+
    async fn evaluate(&self, request: &BidRequest) -> Option<BidResponse>;
}

/// A default pass-through strategy that always returns "No Bid"
pub struct DefaultNoBidStrategy;

#[async_trait]
impl BiddingStrategy for DefaultNoBidStrategy {
    async fn evaluate(&self, _request: &BidRequest) -> Option<BidResponse> {
        None
    }
}

/// The production strategy that checks Redis for active budgets
pub struct ActiveCampaignStrategy {
    redis_manager: RedisManager,
    http_client: reqwest::Client,
    audience_edge_url: String,
}

impl ActiveCampaignStrategy {
    pub fn new(redis_manager: RedisManager, http_client: reqwest::Client, audience_edge_url: String) -> Self {
        Self { redis_manager, http_client, audience_edge_url }
    }

    /// Fetches audience segments for a user from audience-edge.
    /// Times out aggressively to stay well within the bid deadline. Fails open (empty vec).
    async fn fetch_user_segments(&self, dsp_uid: &str) -> Vec<String> {
        let url = format!("{}/internal/audience/{}", self.audience_edge_url, dsp_uid);
        let result = self.http_client
            .get(&url)
            .timeout(std::time::Duration::from_millis(10))
            .send()
            .await;

        match result {
            Ok(resp) if resp.status().is_success() => {
                resp.json::<AudienceResponse>().await
                    .map(|r| r.segments)
                    .unwrap_or_default()
            }
            _ => vec![],
        }
    }
}

#[async_trait]
impl BiddingStrategy for ActiveCampaignStrategy {
    async fn evaluate(&self, request: &BidRequest) -> Option<BidResponse> {
        // 1. Extract the impression and floor price (default to $0.0 if not specified)
        let imp = request.imp.first()?;
        let floor_price = imp.bidfloor.unwrap_or(Decimal::ZERO);

        let mut dsp_uid = None;

        // Try Web Cookie ID first
        if let Some(user) = &request.user {
            if let Some(buyeruid) = &user.buyeruid {
                dsp_uid = Some(buyeruid.clone());
            }
        }

        // Fallback to Mobile Device ID if no cookie exists
        if dsp_uid.is_none() {
            if let Some(device) = &request.device {
                if let Some(ifa) = &device.ifa {
                    dsp_uid = Some(ifa.clone());
                }
            }
        }
        // 2. Fetch all active campaigns from the Redis Hash
        let active_campaigns = self.redis_manager.get_active_campaigns().await.unwrap_or_default();

        if active_campaigns.is_empty() {
            return None; // No active campaigns in memory
        }

        // 3. Enrich with audience segments when a user identity is available.
        //    Fail open: if audience-edge is down, only run-of-network campaigns will match.
        let user_segments: Vec<String> = match &dsp_uid {
            Some(uid) => self.fetch_user_segments(uid).await,
            None => vec![],
        };

        // 4. Find a matching campaign
        let mut selected_campaign_id = None;
        let mut final_bid_price = Decimal::ZERO;

        for campaign_json in active_campaigns {
            if let Ok(parsed) = serde_json::from_str::<Value>(&campaign_json) {
                let id = parsed.get("id").and_then(|v| v.as_str()).unwrap_or("unknown");

                let Some(max_cpm) = parsed.get("max_cpm")
                    .and_then(|v| serde_json::from_value::<Decimal>(v.clone()).ok()) else {
                    continue;
                };

                // Audience targeting: empty target_segments = run-of-network (matches all).
                let target_segments: Vec<String> = parsed
                    .get("target_segments")
                    .and_then(|v| serde_json::from_value(v.clone()).ok())
                    .unwrap_or_default();

                if !target_segments.is_empty()
                    && !user_segments.iter().any(|s| target_segments.contains(s))
                {
                    continue; // User not in this campaign's target audience
                }

                if max_cpm > floor_price {
                    selected_campaign_id = Some(id.to_string());

                    // AdTech Math: Bid slightly above the floor to win the auction safely
                    // (Second-price auction logic)
                    final_bid_price = floor_price + Decimal::new(1, 2);

                    break; // Stop at the first matching campaign to maintain sub-10ms latency
                }
            }
        }

        // 5. Construct the OpenRTB BidResponse
        if let Some(campaign_id) = selected_campaign_id {
            let bid = Bid {
                // Generate a fast, unique bid ID by combining imp ID and campaign ID
                id: format!("bid-{}-{}", imp.id, campaign_id),
                impid: imp.id.clone(),
                price: final_bid_price,
                adid: Some(format!("ad-{}", campaign_id)),
                crid: Some(format!("creative-{}", campaign_id)),
            };

            let seatbid = SeatBid { bid: vec![bid] };

            return Some(BidResponse {
                id: request.id.clone(),
                seatbid: vec![seatbid],
                cur: Some("USD".to_string()),
            });
        }

        // Fallback if no campaigns met the budget requirements
        None
    }
}