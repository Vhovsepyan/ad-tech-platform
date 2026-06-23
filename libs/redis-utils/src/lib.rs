use redis::{aio::MultiplexedConnection, AsyncCommands, Client, RedisResult};

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