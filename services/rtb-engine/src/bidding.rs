use async_trait::async_trait;
use core_models::{Bid, BidRequest, BidResponse, SeatBid};
use redis_utils::RedisManager;
use serde_json::Value;

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
}

impl ActiveCampaignStrategy {
    pub fn new(redis_manager: RedisManager) -> Self {
        Self { redis_manager }
    }
}

#[async_trait]
impl BiddingStrategy for ActiveCampaignStrategy {
    async fn evaluate(&self, request: &BidRequest) -> Option<BidResponse> {
        // 1. Extract the impression and floor price (default to $0.0 if not specified)
        let imp = request.imp.first()?;
        let floor_price = imp.bidfloor.unwrap_or(0.0);

        // 2. Fetch all active campaigns from the Redis Hash
        let active_campaigns = self.redis_manager.get_active_campaigns().await.unwrap_or_default();

        if active_campaigns.is_empty() {
            return None; // No active campaigns in memory
        }

        // 3. Find a matching campaign
        let mut selected_campaign_id = None;
        let mut final_bid_price = 0.0;

        for campaign_json in active_campaigns {
            if let Ok(parsed) = serde_json::from_str::<Value>(&campaign_json) {
                let id = parsed.get("id").and_then(|v| v.as_str()).unwrap_or("unknown");

                // Assuming your campaign-api saves a "max_cpm" or similar budget field.
                // For safety in this phase, we default to $2.00 if the field is missing.
                let max_cpm = parsed.get("max_cpm").and_then(|v| v.as_f64()).unwrap_or(2.0);

                if max_cpm > floor_price {
                    selected_campaign_id = Some(id.to_string());

                    // AdTech Math: Bid slightly above the floor to win the auction safely
                    // (Second-price auction logic)
                    final_bid_price = floor_price + 0.01;

                    break; // Stop at the first matching campaign to maintain sub-10ms latency
                }
            }
        }

        // 4. Construct the OpenRTB BidResponse
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