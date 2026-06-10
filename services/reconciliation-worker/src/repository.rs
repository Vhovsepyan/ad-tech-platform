use crate::aggregator::CampaignMetrics;
use sqlx::PgPool;
use std::collections::HashMap;
use uuid::Uuid;

const PARTITION: i32 = 0;

/// Handles all persistence logic for reconciliation data
pub struct MetricsRepository {
    pool: PgPool,
}

impl MetricsRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Creates the offset tracking table if it doesn't already exist.
    pub async fn ensure_offset_table(&self) {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS kafka_offsets (
                topic     TEXT NOT NULL,
                partition INT  NOT NULL,
                offset    BIGINT NOT NULL,
                PRIMARY KEY (topic, partition)
            )",
        )
        .execute(&self.pool)
        .await
        .expect("CRITICAL: Failed to create kafka_offsets table");
    }

    /// Returns the last committed offset for a topic, or 0 if none recorded yet.
    pub async fn load_offset(&self, topic: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT offset FROM kafka_offsets WHERE topic = $1 AND partition = $2",
        )
        .bind(topic)
        .bind(PARTITION)
        .fetch_optional(&self.pool)
        .await
        .unwrap_or(None)
        .unwrap_or(0)
    }

    /// Upserts the committed offset for a topic after a successful flush.
    pub async fn save_offset(&self, topic: &str, offset: i64) {
        let result = sqlx::query(
            "INSERT INTO kafka_offsets (topic, partition, offset)
             VALUES ($1, $2, $3)
             ON CONFLICT (topic, partition) DO UPDATE SET offset = EXCLUDED.offset",
        )
        .bind(topic)
        .bind(PARTITION)
        .bind(offset)
        .execute(&self.pool)
        .await;

        if let Err(e) = result {
            println!("Warning: Failed to persist Kafka offset: {}", e);
        }
    }

    /// Executes highly optimized UPDATE queries to sync the batch to Postgres
    pub async fn flush_batch(&self, batch: HashMap<String, CampaignMetrics>) {
        println!("Flushing metrics for {} campaigns to Postgres...", batch.len());

        for (campaign_id, metrics) in batch {
            let parsed_uuid = match Uuid::parse_str(&campaign_id) {
                Ok(id) => id,
                Err(_) => {
                    println!("Warning: Invalid UUID format for campaign {}", campaign_id);
                    continue;
                }
            };

            let result = sqlx::query!(
                r#"
                UPDATE campaigns
                SET
                    impressions = impressions + $1,
                    clicks = clicks + $2,
                    budget = budget - $3
                WHERE id = $4
                "#,
                metrics.impressions,
                metrics.clicks,
                metrics.spend,
                parsed_uuid
            )
                .execute(&self.pool)
                .await;

            match result {
                Ok(_) => println!("💰 Deducted ${:.3} from campaign {}", metrics.spend, campaign_id),
                Err(e) => println!("Error updating campaign {}: {}", campaign_id, e),
            }
        }
    }
}