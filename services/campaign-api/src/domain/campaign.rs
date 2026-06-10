use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator::{Validate, ValidationError};
use sqlx::Type;

/// Represents the lifecycle state of a Campaign
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Type)]
#[sqlx(type_name = "campaign_status", rename_all = "SCREAMING_SNAKE_CASE")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CampaignStatus {
    Active,
    Paused,
    Deleted,
}

/// The core Campaign Domain Entity
#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
pub struct Campaign {
    pub id: Uuid,

    #[validate(length(min = 3, message = "Campaign name must be at least 3 characters long."))]
    pub name: String,

    pub status: CampaignStatus,

    #[validate(custom(function = "validate_positive_decimal"))]
    pub budget: Decimal,

    /// Maximum bid price per thousand impressions (CPM) in USD.
    /// The RTB engine will not bid above this price.
    #[validate(custom(function = "validate_positive_decimal"))]
    pub max_cpm: Decimal,

    /// Audience segment IDs this campaign targets (e.g. ["seg-sports", "seg-auto"]).
    /// Empty means run-of-network — the campaign bids on all users.
    pub target_segments: Vec<String>,

    pub start_date: DateTime<Utc>,

    pub end_date: DateTime<Utc>,

    pub created_at: DateTime<Utc>,

    pub updated_at: DateTime<Utc>,
}

fn validate_positive_decimal(value: &Decimal) -> Result<(), ValidationError> {
    if value <= &Decimal::ZERO {
        return Err(ValidationError::new("must_be_positive")
            .with_message("Value must be strictly greater than zero".into()));
    }
    Ok(())
}

impl Campaign {
    /// Domain behavior: Check if the campaign is currently flighting
    pub fn is_active_now(&self) -> bool {
        let now = Utc::now();
        self.status == CampaignStatus::Active && self.start_date <= now && self.end_date >= now
    }
}