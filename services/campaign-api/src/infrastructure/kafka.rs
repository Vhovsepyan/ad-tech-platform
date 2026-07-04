use rskafka::{
    client::{
        partition::{PartitionClient, UnknownTopicHandling},
        ClientBuilder,
    },
    record::Record,
};
use std::sync::Arc;

pub struct CampaignEventPublisher {
    partition_client: Arc<PartitionClient>,
}

impl CampaignEventPublisher {
    /// Initializes the connection to the Kafka broker and ensures the topic exists
    pub async fn new(brokers: Vec<String>, topic: &str) -> Result<Self, Box<dyn std::error::Error>> {
        tracing::info!("Connecting to Kafka brokers: {:?}", brokers);

        let client = ClientBuilder::new(brokers).build().await?;

        // In AdTech, we typically partition by Campaign ID or Advertiser ID to ensure
        // strict ordering of events for a single entity. For this MVP, we route to Partition 0.
        let partition_client = client
            .partition_client(topic, 0, UnknownTopicHandling::Retry)
            .await?;

        Ok(Self {
            partition_client: Arc::new(partition_client),
        })
    }

    /// Publishes a pre-serialized JSON payload to the Kafka topic.
    /// Used by the outbox poller, which already has the payload from the outbox row.
    pub async fn publish_raw(&self, payload: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        let record = Record {
            key: None,
            value: Some(payload.to_vec()),
            headers: std::collections::BTreeMap::new(),
            timestamp: chrono::Utc::now(),
        };
        self.partition_client.produce(vec![record], Default::default()).await?;
        Ok(())
    }
}