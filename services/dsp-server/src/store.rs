use moka::future::Cache;
use redis::AsyncCommands;
use std::{sync::Arc, time::Duration};

pub struct CreativeStore {
    l1_cache: Cache<String, String>,
    redis_client: redis::Client,
}

impl CreativeStore {
    pub fn new(redis_url: &str) -> Self {
        let redis_client = redis::Client::open(redis_url).expect("Invalid Redis URL");

        // Moka cache: holds up to 10,000 creatives, expires after 5 minutes of inactivity
        let l1_cache = Cache::builder()
            .max_capacity(10_000)
            .time_to_idle(Duration::from_secs(300))
            .build();

        Self { l1_cache, redis_client }
    }

    /// Writes raw HTML for a creative ID to Redis and invalidates the L1 cache entry.
    pub async fn set_creative(&self, creative_id: &str, html: &str) -> Result<(), redis::RedisError> {
        self.l1_cache.remove(creative_id).await;
        let mut conn = self.redis_client.get_multiplexed_async_connection().await?;
        let key = format!("creative:{}", creative_id);
        conn.set::<_, _, ()>(&key, html).await?;
        Ok(())
    }

    /// Fetches the raw HTML for a creative ID
    pub async fn get_creative(&self, creative_id: &str) -> Option<String> {
        // 1. Try L1 Cache (Nanosecond speed)
        if let Some(html) = self.l1_cache.get(creative_id).await {
            return Some(html);
        }

        // 2. Try L2 Cache (Redis)
        if let Ok(mut conn) = self.redis_client.get_multiplexed_async_connection().await {
            let key = format!("creative:{}", creative_id);
            if let Ok(Some(html)) = conn.get::<_, Option<String>>(&key).await {

                // Backfill the L1 cache for the next request
                self.l1_cache.insert(creative_id.to_string(), html.clone()).await;
                return Some(html);
            }
        }

        None
    }
}