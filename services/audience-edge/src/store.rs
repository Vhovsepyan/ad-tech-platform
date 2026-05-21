use redis::AsyncCommands;
use std::sync::Arc;

#[derive(Clone)]
pub struct AudienceStore {
    client: redis::Client,
}

impl AudienceStore {
    pub fn new(redis_url: &str) -> Result<Self, redis::RedisError> {
        let client = redis::Client::open(redis_url)?;
        Ok(Self { client })
    }

    /// Fetches segment IDs for a user. Takes ~1ms.
    pub async fn get_segments(&self, dsp_user_id: &str) -> Vec<String> {
        let mut conn = match self.client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(_) => return vec![], // Fail open: return no segments if Redis is down
        };

        let key = format!("uid:{}", dsp_user_id);

        // Fetch comma-separated segments (e.g., "seg-1,seg-45")
        let segments_str: Option<String> = conn.get(key).await.unwrap_or(None);

        match segments_str {
            Some(s) => s.split(',').map(|seg| seg.to_string()).collect(),
            None => vec![],
        }
    }

    /// Saves the mapping between an Ad Exchange ID and our DSP ID
    pub async fn map_user(&self, exchange: &str, exchange_uid: &str, dsp_uid: &str) {
        let mut conn = match self.client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(_) => return,
        };

        let key = format!("sync:{}:{}", exchange, exchange_uid);
        // Map expires in 30 days (AdTech standard)
        let _: () = conn.set_ex(key, dsp_uid, 2_592_000).await.unwrap_or(());
    }
}