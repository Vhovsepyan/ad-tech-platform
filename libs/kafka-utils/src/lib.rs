use redis_utils::RedisManager;
use rskafka::client::{partition::{PartitionClient, UnknownTopicHandling}, ClientBuilder};
use rust_decimal::Decimal;
use serde_json::Value;
use std::sync::Arc;
use rskafka::client::partition::Compression;
use rskafka::record::Record;
use tokio::sync::mpsc;

pub struct CampaignConsumer {
    partition_client: Arc<PartitionClient>,
    redis_manager: RedisManager,
}

impl CampaignConsumer {
    /// Connects to the Kafka cluster and specific topic.
    /// Always starts from offset 0 to rebuild the active_campaigns hash from the full topic history.
    /// This is safe because campaign-updates contains the complete lifecycle (ACTIVE/PAUSED/DELETED)
    /// for every campaign, so replaying it always converges to the correct state.
    pub async fn new(
        brokers: Vec<String>,
        topic: String,
        redis_manager: RedisManager,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        println!("Connecting to Kafka brokers: {:?}", brokers);

        let client = ClientBuilder::new(brokers).build().await?;
        let partition_client = Arc::new(client.partition_client(
            topic,
            0,
            UnknownTopicHandling::Retry,
        ).await?);

        Ok(Self { partition_client, redis_manager })
    }

    /// The continuous polling loop that runs forever in the background.
    /// Starts from offset 0 and rebuilds Redis state; Kafka topic retention is the durability guarantee.
    pub async fn run(&self) {
        println!("CampaignConsumer started. Rebuilding active campaign cache from offset 0...");

        let mut current_offset: i64 = 0;

        loop {
            match self.partition_client.fetch_records(current_offset, 1..10_000_000, 1000).await {
                Ok((records, _high_watermark)) => {
                    for record_and_offset in records {
                        if let Some(payload_bytes) = record_and_offset.record.value {
                            self.process_message(&payload_bytes).await;
                        }
                        current_offset = record_and_offset.offset + 1;
                    }
                }
                Err(e) => {
                    println!("Kafka fetch error: {:?}. Retrying in 5 seconds...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// Parses the Kafka payload and updates Redis accordingly.
    /// For ACTIVE campaigns: syncs to the active_campaigns hash and initializes the budget counter.
    /// For PAUSED/DELETED campaigns: evicts from the cache and removes the budget counter.
    async fn process_message(&self, payload: &[u8]) {
        let json_str = match String::from_utf8(payload.to_vec()) {
            Ok(s) => s,
            Err(_) => return,
        };
        let parsed = match serde_json::from_str::<Value>(&json_str) {
            Ok(v) => v,
            Err(_) => return,
        };

        let (Some(id), Some(status)) = (
            parsed.get("id").and_then(|i| i.as_str()),
            parsed.get("status").and_then(|s| s.as_str()),
        ) else {
            return;
        };

        match status {
            "ACTIVE" => {
                if let Err(e) = self.redis_manager.save_campaign(id, &json_str).await {
                    println!("Failed to cache active campaign {}: {}", id, e);
                } else {
                    println!("Campaign {} went ACTIVE. Synced to cache.", id);
                }
                // Initialize budget counter from campaign JSON
                let budget: Option<Decimal> = parsed.get("budget")
                    .and_then(|v| serde_json::from_value(v.clone()).ok());
                if let Some(b) = budget {
                    if let Err(e) = self.redis_manager.init_budget_counter(id, b).await {
                        println!("Failed to init budget counter for {}: {}", id, e);
                    }
                }
            }
            "PAUSED" | "DELETED" => {
                if let Err(e) = self.redis_manager.remove_campaign(id).await {
                    println!("Failed to evict campaign {}: {}", id, e);
                } else {
                    println!("Campaign {} {}. Evicted from cache.", id, status);
                }
                let _ = self.redis_manager.delete_budget_counter(id).await;
            }
            _ => {}
        }
    }
}

pub struct AsyncEventProducer {
    sender: mpsc::Sender<Vec<u8>>,
}

impl AsyncEventProducer {
    pub async fn new(brokers: Vec<String>, topic: String) -> Result<Self, Box<dyn std::error::Error>> {
        let client = ClientBuilder::new(brokers).build().await?;

        let partition_client = Arc::new(client.partition_client(
            topic,
            0,
            rskafka::client::partition::UnknownTopicHandling::Retry,
        ).await?);

        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(100_000);

        let pc = partition_client.clone();
        tokio::spawn(async move {
            println!("Kafka Background Producer running...");
            while let Some(payload) = rx.recv().await {
                let record = Record {
                    key: None,
                    value: Some(payload),
                    headers: Default::default(),
                    timestamp: chrono::Utc::now(),
                };

                if let Err(e) = pc.produce(vec![record], Compression::NoCompression).await {
                    println!("Warning: Failed to produce to Kafka: {:?}", e);
                }
            }
        });

        Ok(Self { sender: tx })
    }

    /// Fire and forget. Takes 50 nanoseconds. Never blocks the HTTP thread.
    pub fn emit(&self, payload: Vec<u8>) {
        if self.sender.try_send(payload).is_err() {
            println!("CRITICAL: Kafka queue is full! Dropping event.");
        }
    }
}
