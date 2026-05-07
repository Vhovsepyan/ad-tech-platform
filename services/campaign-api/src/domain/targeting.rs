use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator::{Validate, ValidationError};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeviceType {
    Mobile,
    Desktop,
    ConnectedTv,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
pub struct TargetingRules {
    pub campaign_id: Uuid,

    // e.g., ["US", "GB", "CA"]
    pub geo_targets: Vec<String>,

    pub allowed_devices: Vec<DeviceType>,

    // Bid strategy constraints
    #[validate(custom(function = "validate_bid_range"))]
    pub max_cpm_bid: Decimal,

    // Optional: Only target specific publisher domains
    pub domain_allowlist: Option<Vec<String>>,
}

/// Ensure the bid makes logical financial sense
fn validate_bid_range(max_cpm: &Decimal) -> Result<(), ValidationError> {
    if max_cpm <= &Decimal::ZERO {
        return Err(ValidationError::new("invalid_bid")
            .with_message("Max CPM bid must be greater than zero.".into()));
    }
    // AdTech safety check: Prevent "fat finger" errors where a user accidentally bids $10,000 CPM
    if max_cpm > &Decimal::new(100, 0) { // $100.00 CPM limit
        return Err(ValidationError::new("bid_too_high")
            .with_message("Max CPM bid exceeds platform safety limits ($100).".into()));
    }
    Ok(())
}