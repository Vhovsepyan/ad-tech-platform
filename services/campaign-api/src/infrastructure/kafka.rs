use crate::domain::campaign::Campaign;
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

    /// Publishes a Campaign to the Kafka topic
    #[tracing::instrument(skip(self, campaign), fields(campaign_id = %campaign.id))]
    pub async fn publish_campaign_event(&self, campaign: &Campaign) -> Result<(), Box<dyn std::error::Error>> {
        // Serialize the full state to JSON for the RTB engine
        let payload = serde_json::to_vec(campaign)?;

        // Use the Campaign ID as the Kafka Key. This ensures that if we scale to multiple
        // partitions later, all updates for a specific campaign go to the same partition.
        let key = campaign.id.as_bytes().to_vec();

        let record = Record {
            key: Some(key),
            value: Some(payload),
            headers: std::collections::BTreeMap::new(),
            timestamp: chrono::Utc::now(),
        };

        self.partition_client.produce(vec![record], Default::default()).await?;

        Ok(())
    }
}