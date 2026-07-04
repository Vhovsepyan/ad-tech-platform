use async_trait::async_trait;
use chrono::{DateTime, Utc};
use core_models::{AudienceResponse, Bid, BidRequest, BidResponse, SeatBid};
use moka::future::Cache;
use redis_utils::RedisManager;
use rust_decimal::Decimal;
use serde_json::Value;
use std::time::Duration;
use url::Url;

/// Core async trait for pluggable bidding strategies
#[async_trait]
pub trait BiddingStrategy: Send + Sync {
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
    /// Caches user→segments lookups for 60 s to avoid per-bid HTTP round-trips.
    segment_cache: Cache<String, Vec<String>>,
}

impl ActiveCampaignStrategy {
    pub fn new(redis_manager: RedisManager, http_client: reqwest::Client, audience_edge_url: String) -> Self {
        let segment_cache = Cache::builder()
            .max_capacity(50_000)
            .time_to_live(Duration::from_secs(60))
            .build();
        Self { redis_manager, http_client, audience_edge_url, segment_cache }
    }

    /// Returns audience segments for a user, serving from the local cache when possible.
    /// On a cache miss, fetches from audience-edge with a tight timeout. Fails open (empty vec).
    async fn fetch_user_segments(&self, dsp_uid: &str) -> Vec<String> {
        if let Some(cached) = self.segment_cache.get(dsp_uid).await {
            return cached;
        }
        let segments = self.fetch_user_segments_from_origin(dsp_uid).await;
        self.segment_cache.insert(dsp_uid.to_string(), segments.clone()).await;
        segments
    }

    async fn fetch_user_segments_from_origin(&self, dsp_uid: &str) -> Vec<String> {
        // Use Url::path_segments_mut so dsp_uid is percent-encoded as a single path segment.
        // Raw format!() interpolation would allow buyeruid="../admin" to reach other routes.
        let url = match Url::parse(&self.audience_edge_url) {
            Ok(mut u) => {
                match u.path_segments_mut() {
                    Ok(mut segs) => { segs.extend(["internal", "audience", dsp_uid]); }
                    Err(_) => return vec![],
                }
                u
            }
            Err(_) => return vec![],
        };
        let result = self.http_client
            .get(url)
            .timeout(Duration::from_millis(10))
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
        let imp = request.imp.first()?;
        let floor_price = imp.bidfloor.unwrap_or(Decimal::ZERO);

        // Prefer web cookie ID; fall back to mobile device ID.
        let dsp_uid = request.user.as_ref()
            .and_then(|u| u.buyeruid.clone())
            .or_else(|| request.device.as_ref().and_then(|d| d.ifa.clone()));

        let active_campaigns = self.redis_manager.get_active_campaigns().await.unwrap_or_default();
        if active_campaigns.is_empty() {
            return None;
        }

        // Fail open: if audience-edge is down, only run-of-network campaigns will match.
        let user_segments: Vec<String> = match &dsp_uid {
            Some(uid) => self.fetch_user_segments(uid).await,
            None => vec![],
        };

        let now = Utc::now();

        // Collect all eligible bids: (campaign_id, max_cpm)
        let mut eligible: Vec<(String, Decimal)> = Vec::new();

        for campaign_json in &active_campaigns {
            if let Ok(parsed) = serde_json::from_str::<Value>(campaign_json) {
                let id = match parsed.get("id").and_then(|v| v.as_str()) {
                    Some(s) => s.to_string(),
                    None => continue,
                };

                let Some(max_cpm) = parsed.get("max_cpm")
                    .and_then(|v| serde_json::from_value::<Decimal>(v.clone()).ok()) else {
                    continue;
                };

                // Flight date check (hot path safety net; the scheduled expiry job handles the common case)
                if !is_within_flight(&parsed, now) {
                    continue;
                }

                // Audience targeting: empty target_segments = run-of-network (bids on all users)
                let target_segments: Vec<String> = parsed
                    .get("target_segments")
                    .and_then(|v| serde_json::from_value(v.clone()).ok())
                    .unwrap_or_default();

                if !target_segments.is_empty()
                    && !user_segments.iter().any(|s| target_segments.contains(s))
                {
                    continue;
                }

                if max_cpm > floor_price {
                    eligible.push((id, max_cpm));
                }
            }
        }

        if eligible.is_empty() {
            return None;
        }

        // True second-price auction: sort by max_cpm descending, highest wins at second price + $0.01.
        eligible.sort_by(|a, b| b.1.cmp(&a.1));
        let (winner_id, _winner_cpm) = &eligible[0];
        let second_price = eligible.get(1).map(|(_, cpm)| *cpm).unwrap_or(floor_price);
        let clearing_price = second_price + Decimal::new(1, 2);

        // Budget guard: atomically decrement the Redis counter.
        // Spend is clearing_price / 1000 (CPM → per-impression cost).
        let spend = clearing_price / Decimal::from(1000);
        match self.redis_manager.decrement_budget(winner_id, spend).await {
            Ok(remaining) if remaining < Decimal::ZERO => {
                // Over-budget: refund the decrement and return no-bid.
                let _ = self.redis_manager.increment_budget(winner_id, spend).await;
                return None;
            }
            Err(_) => {
                // Redis error: fail open and let the bid proceed.
            }
            _ => {}
        }

        let bid_id = format!("bid-{}-{}", imp.id, winner_id);

        // Store the authoritative clearing price so event-tracker can verify it without
        // trusting the URL parameter (24h TTL covers any reasonable impression delay).
        let _ = self.redis_manager.store_bid_record(&bid_id, clearing_price).await;

        let bid = Bid {
            id: bid_id,
            impid: imp.id.clone(),
            price: clearing_price,
            adid: Some(format!("ad-{}", winner_id)),
            crid: Some(format!("creative-{}", winner_id)),
        };

        Some(BidResponse {
            id: request.id.clone(),
            seatbid: vec![SeatBid { bid: vec![bid] }],
            cur: Some("USD".to_string()),
        })
    }
}

/// Returns false if the campaign has explicit flight dates and now falls outside them.
fn is_within_flight(parsed: &Value, now: DateTime<Utc>) -> bool {
    let start = parsed.get("start_date")
        .and_then(|v| v.as_str())
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&Utc));

    let end = parsed.get("end_date")
        .and_then(|v| v.as_str())
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&Utc));

    match (start, end) {
        (Some(s), Some(e)) => now >= s && now <= e,
        _ => true, // Missing dates: don't block the bid
    }
}
