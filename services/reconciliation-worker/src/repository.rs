use crate::aggregator::CampaignMetrics;
use sqlx::PgPool;
use std::collections::HashMap;
use uuid::Uuid;

/// Handles all persistence logic for reconciliation data
pub struct MetricsRepository {
    pool: PgPool,
}

impl MetricsRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
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
                    -- FIX: Cast the Rust f64 to a Postgres Float, then to Numeric
                    budget = budget - ($3::FLOAT8)::NUMERIC
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