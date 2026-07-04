use crate::aggregator::CampaignMetrics;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::collections::HashMap;
use uuid::Uuid;

const PARTITION: i32 = 0;

pub struct MetricsRepository {
    pool: PgPool,
}

impl MetricsRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

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

    /// Atomically writes all campaign metric updates, advances the Kafka offset, and
    /// returns the new remaining budget for each campaign (for Redis sync).
    pub async fn flush_and_commit(
        &self,
        batch: HashMap<String, CampaignMetrics>,
        offset: i64,
        topic: &str,
    ) -> HashMap<Uuid, Decimal> {
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(e) => {
                eprintln!("CRITICAL: Failed to start transaction: {}. Batch will be retried.", e);
                return HashMap::new();
            }
        };

        println!("Flushing metrics for {} campaigns to Postgres...", batch.len());

        let mut new_budgets: HashMap<Uuid, Decimal> = HashMap::new();

        for (campaign_id, metrics) in batch {
            let parsed_uuid = match Uuid::parse_str(&campaign_id) {
                Ok(id) => id,
                Err(_) => {
                    eprintln!("Warning: Skipping invalid campaign ID: {}", campaign_id);
                    continue;
                }
            };

            // RETURNING budget gives us the authoritative post-flush value to sync back to Redis.
            let result = sqlx::query!(
                r#"
                UPDATE campaigns
                SET
                    impressions = impressions + $1,
                    clicks      = clicks + $2,
                    budget      = budget - $3
                WHERE id = $4
                RETURNING budget
                "#,
                metrics.impressions,
                metrics.clicks,
                metrics.spend,
                parsed_uuid
            )
            .fetch_optional(&mut *tx)
            .await;

            match result {
                Ok(Some(row)) => {
                    new_budgets.insert(parsed_uuid, row.budget);
                    println!("Deducted ${:.4} from campaign {}", metrics.spend, campaign_id);
                }
                Ok(None) => {
                    eprintln!("Warning: Campaign {} not found for budget update.", campaign_id);
                }
                Err(e) => {
                    eprintln!("CRITICAL: Campaign UPDATE failed for {}: {}. Rolling back.", campaign_id, e);
                    let _ = tx.rollback().await;
                    return HashMap::new();
                }
            }
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
            eprintln!("CRITICAL: Offset save failed: {}. Rolling back.", e);
            let _ = tx.rollback().await;
            return HashMap::new();
        }

        if let Err(e) = tx.commit().await {
            eprintln!("CRITICAL: Commit failed: {}. Batch will be retried on restart.", e);
            return HashMap::new();
        }

        new_budgets
    }
}
