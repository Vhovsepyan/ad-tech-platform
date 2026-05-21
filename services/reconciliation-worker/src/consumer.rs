use rskafka::client::{partition::PartitionClient, partition::UnknownTopicHandling, ClientBuilder};
use std::sync::Arc;

/// Manages the continuous extraction of events from the Kafka broker
pub struct EventConsumer {
    partition_client: Arc<PartitionClient>,
    pub current_offset: i64,
}

impl EventConsumer {
    pub async fn new(brokers: Vec<String>, topic: String) -> Result<Self, Box<dyn std::error::Error>> {
        let client = ClientBuilder::new(brokers).build().await?;
        let partition_client = Arc::new(
            client
                .partition_client(topic, 0, UnknownTopicHandling::Retry)
                .await?,
        );

        Ok(Self {
            partition_client,
            current_offset: 0,
        })
    }

    /// Fetches a chunk of records and returns the raw byte payloads along with a boolean
    /// indicating if the consumer has fully caught up to the head of the topic.
    pub async fn fetch_events(&mut self) -> Result<(Vec<Vec<u8>>, bool), rskafka::client::error::Error> {
        let (records, high_watermark) = self
            .partition_client
            .fetch_records(self.current_offset, 1..10_000_000, 500)
            .await?;

        let mut payloads = Vec::with_capacity(records.len());

        for record_and_offset in records {
            if let Some(payload_bytes) = record_and_offset.record.value {
                payloads.push(payload_bytes);
            }
            self.current_offset = record_and_offset.offset + 1;
        }

        let is_caught_up = self.current_offset == high_watermark;

        Ok((payloads, is_caught_up))
    }
}