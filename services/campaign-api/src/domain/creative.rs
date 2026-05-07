use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator::Validate;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CreativeFormat {
    Banner,
    Video,
    Native,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
pub struct Creative {
    pub id: Uuid,

    // Foreign key reference to the parent Campaign
    pub campaign_id: Uuid,

    #[validate(length(min = 3, message = "Creative name must be at least 3 characters."))]
    pub name: String,

    pub format: CreativeFormat,

    #[validate(url(message = "Asset URL must be a valid HTTP/HTTPS url."))]
    pub asset_url: String,

    // Width and Height are optional (e.g., Native ads might not have strict dimensions)
    pub width: Option<i32>,
    pub height: Option<i32>,
}