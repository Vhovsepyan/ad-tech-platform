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

    /// Atomically writes all campaign metric updates and advances the Kafka offset in a
    /// single transaction. Either both commit or neither does — preventing double-spend
    /// on restart and preventing offset advancement when metric writes fail.
    pub async fn flush_and_commit(
        &self,
        batch: HashMap<String, CampaignMetrics>,
        offset: i64,
        topic: &str,
    ) {
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(e) => {
                eprintln!("CRITICAL: Failed to start transaction: {}. Batch will be retried.", e);
                return;
            }
        };

        println!("Flushing metrics for {} campaigns to Postgres...", batch.len());

        for (campaign_id, metrics) in batch {
            let parsed_uuid = match Uuid::parse_str(&campaign_id) {
                Ok(id) => id,
                Err(_) => {
                    eprintln!("Warning: Skipping invalid campaign ID: {}", campaign_id);
                    continue;
                }
            };

            if let Err(e) = sqlx::query!(
                r#"
                UPDATE campaigns
                SET
                    impressions = impressions + $1,
                    clicks      = clicks + $2,
                    budget      = budget - $3
                WHERE id = $4
                "#,
                metrics.impressions,
                metrics.clicks,
                metrics.spend,
                parsed_uuid
            )
            .execute(&mut *tx)
            .await
            {
                eprintln!(
                    "CRITICAL: Campaign UPDATE failed for {}: {}. Rolling back batch.",
                    campaign_id, e
                );
                let _ = tx.rollback().await;
                return;
            }

            println!("Deducted ${:.4} from campaign {}", metrics.spend, campaign_id);
        }

        if let Err(e) = sqlx::query(
            "INSERT INTO kafka_offsets (topic, partition, offset)
             VALUES ($1, $2, $3)
             ON CONFLICT (topic, partition) DO UPDATE SET offset = EXCLUDED.offset",
        )
        .bind(topic)
        .bind(PARTITION)
        .bind(offset)
        .execute(&mut *tx)
        .await
        {
            eprintln!("CRITICAL: Offset save failed: {}. Rolling back batch.", e);
            let _ = tx.rollback().await;
            return;
        }

        if let Err(e) = tx.commit().await {
            eprintln!("CRITICAL: Commit failed: {}. Batch will be retried on restart.", e);
        }
    }
}
