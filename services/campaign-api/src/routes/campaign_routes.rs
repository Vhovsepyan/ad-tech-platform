use crate::domain::campaign::Campaign;
use crate::repository::campaign_repo::CampaignRepository;
use crate::AppState;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator::Validate;

/// The payload expected when creating a new campaign
#[derive(Debug, Deserialize, Validate)]
pub struct CreateCampaignRequest {
    #[validate(length(min = 3))]
    pub name: String,
    pub budget: rust_decimal::Decimal,
    pub start_date: chrono::DateTime<chrono::Utc>,
    pub end_date: chrono::DateTime<chrono::Utc>,
}

/// Pagination parameters for listing campaigns
#[derive(Debug, Deserialize)]
pub struct PaginationParams {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// Centralized error wrapper for the API
pub enum AppError {
    ValidationError(String),
    DatabaseError(sqlx::Error),
}

// Map internal errors to HTTP responses
impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let (status, error_message) = match self {
            AppError::ValidationError(msg) => (StatusCode::BAD_REQUEST, msg),
            AppError::DatabaseError(err) => {
                // In production, log the exact DB error via tracing, but hide it from the client
                println!("Database error: {:?}", err);
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error".to_string())
            }
        };

        (status, Json(serde_json::json!({ "error": error_message }))).into_response()
    }
}

/// POST /campaigns
async fn create_campaign(
    State(state): State<AppState>,
    Json(payload): Json<CreateCampaignRequest>,
) -> Result<(StatusCode, Json<Campaign>), AppError> {
    // 1. Validate the incoming request
    if let Err(e) = payload.validate() {
        return Err(AppError::ValidationError(e.to_string()));
    }
    
    let now = chrono::Utc::now();
    // 2. Map the request to our Domain Entity
    let new_campaign = Campaign {
        id: Uuid::new_v4(),
        name: payload.name,
        status: crate::domain::campaign::CampaignStatus::Paused, // Default to Paused for safety
        budget: payload.budget,
        start_date: payload.start_date,
        end_date: payload.end_date,
        created_at: now,
        updated_at: now,
    };

    // 3. Persist to DB
    let repo = CampaignRepository::new(state.db_pool);
    let created = repo
        .create_campaign(&new_campaign)
        .await
        .map_err(AppError::DatabaseError)?;

    // 4. Return 201 Created
    Ok((StatusCode::CREATED, Json(created)))
}

/// GET /campaigns
async fn list_campaigns(
    State(state): State<AppState>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<Vec<Campaign>>, AppError> {
    let limit = params.limit.unwrap_or(50); // Default AdTech limit
    let offset = params.offset.unwrap_or(0);

    let repo = CampaignRepository::new(state.db_pool);
    let campaigns = repo
        .list_campaigns(limit, offset)
        .await
        .map_err(AppError::DatabaseError)?;

    Ok(Json(campaigns))
}

/// Export the router to be nested in main.rs
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", post(create_campaign).get(list_campaigns))
}