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

    /// Inserts a new campaign and returns the fully materialized record from the DB.
    pub async fn create_campaign(&self, campaign: &Campaign) -> Result<Campaign, Error> {
        // sqlx::query_as! validates this SQL against the DB at compile-time!
        let inserted_campaign = sqlx::query_as!(
            Campaign,
            r#"
            INSERT INTO campaigns (id, name, status, budget, start_date, end_date)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, name, status AS "status: CampaignStatus", budget, start_date, end_date, created_at, updated_at
            "#,
            campaign.id,
            campaign.name,
            campaign.status as CampaignStatus,
            campaign.budget,
            campaign.start_date,
            campaign.end_date
        )
            .fetch_one(&self.pool)
            .await?;

        Ok(inserted_campaign)
    }

    /// Lists campaigns. In a production AdTech system, NEVER SELECT * without limits.
    /// We include pagination (limit/offset) to prevent OOM errors.
    pub async fn list_campaigns(&self, limit: i64, offset: i64) -> Result<Vec<Campaign>, Error> {
        let campaigns = sqlx::query_as!(
            Campaign,
            r#"
            SELECT id, name, status AS "status: CampaignStatus", budget, start_date, end_date, created_at, updated_at
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

    /// Fetches a single campaign by ID. Returns None if it doesn't exist or is deleted.
    pub async fn get_campaign(&self, id: Uuid) -> Result<Option<Campaign>, Error> {
        let campaign = sqlx::query_as!(
            Campaign,
            r#"
            SELECT id, name, status AS "status: CampaignStatus", budget, start_date, end_date, created_at, updated_at
            FROM campaigns
            WHERE id = $1 AND status != 'DELETED'
            "#,
            id
        )
            .fetch_optional(&self.pool)
            .await?;

        Ok(campaign)
    }
}