use redis::{aio::MultiplexedConnection, AsyncCommands, Client};

#[derive(Clone)]
pub struct AudienceStore {
    connection: MultiplexedConnection,
}

impl AudienceStore {
    pub async fn new(redis_url: &str) -> Result<Self, redis::RedisError> {
        let connection = Client::open(redis_url)?
            .get_multiplexed_async_connection()
            .await?;
        Ok(Self { connection })
    }

    /// Fetches segment IDs for a user. Fail-open: returns empty vec if Redis is unavailable.
    pub async fn get_segments(&self, dsp_user_id: &str) -> Vec<String> {
        let mut conn = self.connection.clone();
        let key = format!("uid:{}", dsp_user_id);
        let segments_str: Option<String> = conn.get(key).await.unwrap_or(None);
        match segments_str {
            Some(s) => s.split(',').map(|seg| seg.to_string()).collect(),
            None => vec![],
        }
    }

    /// Saves the mapping between an Ad Exchange ID and our DSP ID.
    pub async fn map_user(&self, exchange: &str, exchange_uid: &str, dsp_uid: &str) {
        let mut conn = self.connection.clone();
        let key = format!("sync:{}:{}", exchange, exchange_uid);
        // Map expires in 30 days (AdTech standard)
        let _: () = conn.set_ex(key, dsp_uid, 2_592_000).await.unwrap_or(());
    }
}