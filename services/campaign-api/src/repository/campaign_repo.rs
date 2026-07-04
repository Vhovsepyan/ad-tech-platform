use crate::domain::campaign::{Campaign, CampaignStatus};
use sqlx::{PgPool, Error};
use uuid::Uuid;

pub struct CampaignRepository {
    pool: PgPool,
}

impl CampaignRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts a new campaign and an outbox row in a single transaction.
    /// The outbox poller publishes the outbox row to Kafka, eliminating the
    /// dual-write split-brain risk between Postgres and Kafka.
    pub async fn create_campaign(&self, campaign: &Campaign) -> Result<Campaign, Error> {
        let mut tx = self.pool.begin().await?;

        let created = sqlx::query_as!(
            Campaign,
            r#"
            INSERT INTO campaigns (id, name, status, budget, max_cpm, target_segments, start_date, end_date)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id, name, status AS "status: CampaignStatus", budget, max_cpm, target_segments, start_date, end_date, created_at, updated_at
            "#,
            campaign.id,
            campaign.name,
            campaign.status as CampaignStatus,
            campaign.budget,
            campaign.max_cpm,
            &campaign.target_segments as &[String],
            campaign.start_date,
            campaign.end_date
        )
        .fetch_one(&mut *tx)
        .await?;

        let payload = serde_json::to_value(&created)
            .map_err(|e| Error::Protocol(e.to_string()))?;

        sqlx::query(
            "INSERT INTO campaign_outbox (aggregate_id, event_type, payload) VALUES ($1, $2, $3)"
        )
        .bind(created.id)
        .bind("campaign.updated")
        .bind(payload)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(created)
    }

    /// Updates the status of a campaign and writes an outbox row in a single transaction.
    pub async fn update_campaign_status(
        &self,
        id: Uuid,
        new_status: CampaignStatus,
    ) -> Result<Campaign, Error> {
        let mut tx = self.pool.begin().await?;

        let updated = sqlx::query_as!(
            Campaign,
            r#"
            UPDATE campaigns
            SET status = $1
            WHERE id = $2 AND status != 'DELETED'
            RETURNING id, name, status AS "status: CampaignStatus", budget, max_cpm, target_segments, start_date, end_date, created_at, updated_at
            "#,
            new_status as CampaignStatus,
            id
        )
        .fetch_one(&mut *tx)
        .await?;

        let payload = serde_json::to_value(&updated)
            .map_err(|e| Error::Protocol(e.to_string()))?;

        sqlx::query(
            "INSERT INTO campaign_outbox (aggregate_id, event_type, payload) VALUES ($1, $2, $3)"
        )
        .bind(updated.id)
        .bind("campaign.updated")
        .bind(payload)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(updated)
    }

    pub async fn list_campaigns(&self, limit: i64, offset: i64) -> Result<Vec<Campaign>, Error> {
        let campaigns = sqlx::query_as!(
            Campaign,
            r#"
            SELECT id, name, status AS "status: CampaignStatus", budget, max_cpm, target_segments, start_date, end_date, created_at, updated_at
            FROM campaigns
            WHERE status != 'DELETED'
            ORDER BY created_at DESC
            LIMIT $1 OFFSET $2
            "#,
            limit,
            offset
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(campaigns)
    }

    pub async fn get_campaign(&self, id: Uuid) -> Result<Option<Campaign>, Error> {
        let campaign = sqlx::query_as!(
            Campaign,
            r#"
            SELECT id, name, status AS "status: CampaignStatus", budget, max_cpm, target_segments, start_date, end_date, created_at, updated_at
            FROM campaigns
            WHERE id = $1 AND status != 'DELETED'
            "#,
            id
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(campaign)
    }

    /// Expires active campaigns whose end_date has passed. Called by the background expiry task.
    /// Returns the updated campaigns so their outbox rows can be processed.
    pub async fn expire_ended_campaigns(&self) -> Result<Vec<Campaign>, Error> {
        let expired = sqlx::query_as!(
            Campaign,
            r#"
            UPDATE campaigns
            SET status = 'PAUSED'
            WHERE status = 'ACTIVE' AND end_date < NOW()
            RETURNING id, name, status AS "status: CampaignStatus", budget, max_cpm, target_segments, start_date, end_date, created_at, updated_at
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        if !expired.is_empty() {
            for campaign in &expired {
                let payload = serde_json::to_value(campaign)
                    .unwrap_or(serde_json::Value::Null);
                let _ = sqlx::query(
                    "INSERT INTO campaign_outbox (aggregate_id, event_type, payload) VALUES ($1, $2, $3)"
                )
                .bind(campaign.id)
                .bind("campaign.updated")
                .bind(payload)
                .execute(&self.pool)
                .await;
            }
        }

        Ok(expired)
    }
}
