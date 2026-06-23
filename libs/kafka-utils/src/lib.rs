use redis_utils::RedisManager;
use rskafka::client::{partition::{PartitionClient, UnknownTopicHandling}, ClientBuilder};
use serde_json::Value;
use std::sync::Arc;
use rskafka::client::partition::Compression;
use rskafka::record::Record;
use tokio::sync::mpsc;

const CAMPAIGN_CONSUMER_OFFSET_KEY: &str = "campaign_consumer_offset";

pub struct CampaignConsumer {
    partition_client: Arc<PartitionClient>,
    redis_manager: RedisManager,
    initial_offset: i64,
}

impl CampaignConsumer {
    /// Connects to the Kafka cluster and specific topic
    pub async fn new(
        brokers: Vec<String>,
        topic: String,
        redis_manager: RedisManager,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        println!("Connecting to Kafka brokers: {:?}", brokers);

        let client = ClientBuilder::new(brokers).build().await?;
        // In a massively scaled production environment, you would dynamically 
        // assign partitions. For our architecture, Partition 0 is sufficient.
        let partition_client = Arc::new(client.partition_client(
            topic,
            0,
            UnknownTopicHandling::Retry // <-- Tells the engine to gracefully wait if the topic isn't ready
        ).await?);

        let initial_offset = redis_manager
            .load_consumer_offset(CAMPAIGN_CONSUMER_OFFSET_KEY)
            .await;
        println!("CampaignConsumer resuming from Kafka offset {}", initial_offset);

        Ok(Self {
            partition_client,
            redis_manager,
            initial_offset,
        })
    }

    /// The continuous polling loop that runs forever in the background
    pub async fn run(&self) {
        println!("CampaignConsumer started listening for campaign updates...");

        let mut current_offset = self.initial_offset;

        loop {
            // Fetch records: wait up to 1000ms for data, pulling max 10MB at a time
            match self.partition_client.fetch_records(current_offset, 1..10_000_000, 1000).await {
                Ok((records, high_watermark)) => {
                    for record_and_offset in records {
                        if let Some(payload_bytes) = record_and_offset.record.value {
                            self.process_message(&payload_bytes).await;
                        }
                        // Advance the offset so we don't read this message again
                        current_offset = record_and_offset.offset + 1;
                    }

                    // When caught up, persist the offset so the next restart resumes here
                    // instead of replaying the full topic history from 0.
                    if current_offset == high_watermark {
                        self.redis_manager
                            .save_consumer_offset(CAMPAIGN_CONSUMER_OFFSET_KEY, current_offset)
                            .await;
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                }
                Err(e) => {
                    println!("Kafka fetch error: {:?}. Retrying in 5 seconds...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// Parses the Kafka payload and updates Redis accordingly
    async fn process_message(&self, payload: &[u8]) {
        if let Ok(json_str) = String::from_utf8(payload.to_vec()) {
            if let Ok(parsed) = serde_json::from_str::<Value>(&json_str) {

                // Extract the UUID and Status from the JSON payload
                if let (Some(id), Some(status)) = (
                    parsed.get("id").and_then(|i| i.as_str()),
                    parsed.get("status").and_then(|s| s.as_str()),
                ) {
                    match status {
                        "ACTIVE" => {
                            if let Err(e) = self.redis_manager.save_campaign(id, &json_str).await {
                                println!("Failed to cache active campaign {}: {}", id, e);
                            } else {
                                println!("🟢 Campaign {} went ACTIVE. Synced to cache.", id);
                            }
                        }
                        "PAUSED" | "DELETED" => {
                            if let Err(e) = self.redis_manager.remove_campaign(id).await {
                                println!("Failed to evict campaign {}: {}", id, e);
                            } else {
                                println!("🔴 Campaign {} {}. Evicted from cache.", id, status);
                            }
                        }
                        _ => {} // Ignore other statuses like "PENDING_REVIEW"
                    }
                }
            }
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
            rskafka::client::partition::UnknownTopicHandling::Retry
        ).await?);

        // Create a massive in-memory queue capable of holding 100,000 pending events
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(100_000);

        // Spawn the background worker to drain the queue into Kafka
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

                // Fire to Kafka. (Architect Note: In production, we would buffer these
                // and use a BatchProducer, but this is perfect for our current scale).
                if let Err(e) = pc.produce(vec![record], Compression::NoCompression).await {
                    println!("Warning: Failed to produce to Kafka: {:?}", e);
                }
            }
        });

        Ok(Self { sender: tx })
    }

    /// Fire and forget. Takes 50 nanoseconds. Never blocks the HTTP thread.
    pub fn emit(&self, payload: Vec<u8>) {
        if let Err(_) = self.sender.try_send(payload) {
            println!("CRITICAL: Kafka queue is full! Dropping event.");
        }
    }
}