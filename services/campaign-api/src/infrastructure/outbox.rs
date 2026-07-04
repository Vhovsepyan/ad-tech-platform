use crate::infrastructure::kafka::CampaignEventPublisher;
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use tokio::time;
use uuid::Uuid;

/// Polls the campaign_outbox table every 100ms and publishes unprocessed rows to Kafka.
/// Uses FOR UPDATE SKIP LOCKED so multiple instances can run without duplicate delivery.
pub async fn run_outbox_poller(pool: PgPool, publisher: Arc<CampaignEventPublisher>) {
    let mut interval = time::interval(Duration::from_millis(100));
    loop {
        interval.tick().await;
        if let Err(e) = process_batch(&pool, &publisher).await {
            tracing::error!("Outbox poller error: {:?}", e);
        }
    }
}

async fn process_batch(
    pool: &PgPool,
    publisher: &CampaignEventPublisher,
) -> Result<(), Box<dyn std::error::Error>> {
    let rows = sqlx::query_as::<_, (Uuid, serde_json::Value)>(
        "SELECT id, payload
         FROM campaign_outbox
         WHERE processed_at IS NULL
         ORDER BY created_at
         LIMIT 100
         FOR UPDATE SKIP LOCKED"
    )
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(());
    }

    for (id, payload) in &rows {
        let payload_bytes = serde_json::to_vec(payload)?;

        if let Err(e) = publisher.publish_raw(&payload_bytes).await {
            tracing::error!("Outbox: failed to publish row {}: {:?}", id, e);
            continue;
        }

        sqlx::query("UPDATE campaign_outbox SET processed_at = NOW() WHERE id = $1")
            .bind(id)
            .execute(pool)
            .await?;
    }

    Ok(())
}
