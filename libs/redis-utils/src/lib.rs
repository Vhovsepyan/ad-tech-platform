use redis::{aio::MultiplexedConnection, AsyncCommands, Client, RedisResult};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

/// Manages high-speed multiplexed connections to Redis
#[derive(Clone)]
pub struct RedisManager {
    connection: MultiplexedConnection,
}

impl RedisManager {
    pub async fn new(redis_url: &str) -> Result<Self, redis::RedisError> {
        let client = Client::open(redis_url)?;
        let connection = client.get_multiplexed_async_connection().await?;
        Ok(Self { connection })
    }

    // ── Active campaigns hash ────────────────────────────────────────────────

    pub async fn save_campaign(&self, campaign_id: &str, campaign_json: &str) -> RedisResult<()> {
        let mut conn = self.connection.clone();
        let _: () = conn.hset("active_campaigns", campaign_id, campaign_json).await?;
        Ok(())
    }

    pub async fn get_active_campaigns(&self) -> RedisResult<Vec<String>> {
        let mut conn = self.connection.clone();
        let campaigns: Vec<String> = conn.hvals("active_campaigns").await?;
        Ok(campaigns)
    }

    pub async fn remove_campaign(&self, campaign_id: &str) -> RedisResult<()> {
        let mut conn = self.connection.clone();
        let _: () = conn.hdel("active_campaigns", campaign_id).await?;
        Ok(())
    }

    // ── Budget counters ──────────────────────────────────────────────────────
    // Stored as micro-dollars (Decimal × 10_000) so DECRBY/INCRBY stay precise.

    fn budget_key(campaign_id: &str) -> String {
        format!("budget:{}", campaign_id)
    }

    fn to_microdollars(amount: Decimal) -> i64 {
        (amount * Decimal::from(10_000)).to_i64().unwrap_or(0)
    }

    fn from_microdollars(microdollars: i64) -> Decimal {
        Decimal::from(microdollars) / Decimal::from(10_000)
    }

    /// Sets the budget counter to the authoritative value (call when campaign goes ACTIVE or after Postgres flush).
    pub async fn init_budget_counter(&self, campaign_id: &str, budget: Decimal) -> RedisResult<()> {
        let mut conn = self.connection.clone();
        let _: () = conn.set(Self::budget_key(campaign_id), Self::to_microdollars(budget)).await?;
        Ok(())
    }

    /// Atomically decrements the budget counter. Returns the remaining balance.
    /// A negative return value means the campaign is over-budget — caller should refund.
    pub async fn decrement_budget(&self, campaign_id: &str, spend: Decimal) -> RedisResult<Decimal> {
        let mut conn = self.connection.clone();
        let remaining: i64 = conn.decr(Self::budget_key(campaign_id), Self::to_microdollars(spend)).await?;
        Ok(Self::from_microdollars(remaining))
    }

    /// Refunds a previously decremented amount (used when a bid wins but budget was insufficient).
    pub async fn increment_budget(&self, campaign_id: &str, amount: Decimal) -> RedisResult<Decimal> {
        let mut conn = self.connection.clone();
        let remaining: i64 = conn.incr(Self::budget_key(campaign_id), Self::to_microdollars(amount)).await?;
        Ok(Self::from_microdollars(remaining))
    }

    /// Removes the budget counter when a campaign is paused or deleted.
    pub async fn delete_budget_counter(&self, campaign_id: &str) -> RedisResult<()> {
        let mut conn = self.connection.clone();
        let _: () = conn.del(Self::budget_key(campaign_id)).await?;
        Ok(())
    }

    // ── Bid records ──────────────────────────────────────────────────────────
    // Stored with a 24-hour TTL so event-tracker can look up the authoritative
    // clearing price without trusting the URL parameter.

    /// Stores the clearing price for a bid ID. Called by rtb-engine after winning a bid.
    pub async fn store_bid_record(&self, bid_id: &str, clearing_price: Decimal) -> RedisResult<()> {
        let mut conn = self.connection.clone();
        let key = format!("bid:{}", bid_id);
        let _: () = conn.set_ex(key, clearing_price.to_string(), 86_400).await?;
        Ok(())
    }

    /// Returns the authoritative clearing price for a bid. Used by event-tracker to verify impression/click events.
    pub async fn get_bid_record(&self, bid_id: &str) -> RedisResult<Option<Decimal>> {
        let mut conn = self.connection.clone();
        let key = format!("bid:{}", bid_id);
        let result: Option<String> = conn.get(key).await?;
        Ok(result.and_then(|s| s.parse().ok()))
    }

    // ── Legacy Kafka offset helpers (kept for backward compatibility) ─────────

    /// Loads the last saved Kafka consumer offset, or 0 if none recorded yet.
    pub async fn load_consumer_offset(&self, key: &str) -> i64 {
        let mut conn = self.connection.clone();
        conn.get::<_, Option<i64>>(key)
            .await
            .unwrap_or(None)
            .unwrap_or(0)
    }

    /// Persists the current Kafka consumer offset so restarts can resume without full replay.
    pub async fn save_consumer_offset(&self, key: &str, offset: i64) {
        let mut conn = self.connection.clone();
        let _: RedisResult<()> = conn.set(key, offset).await;
    }
}
