use crate::domain::campaign::Campaign;
use crate::repository::campaign_repo::CampaignRepository;
use crate::AppState;
use axum::{
    extract::{Query, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize};
use uuid::Uuid;
use validator::Validate;

#[derive(Debug, Deserialize, Validate)]
pub struct CreateCampaignRequest {
    #[validate(length(min = 3))]
    pub name: String,
    pub budget: rust_decimal::Decimal,
    pub start_date: chrono::DateTime<chrono::Utc>,
    pub end_date: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateStatusRequest {
    pub status: crate::domain::campaign::CampaignStatus,
}

/// Pagination parameters for listing campaigns
#[derive(Debug, Deserialize)]
pub struct PaginationParams {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// Upgraded Production Error Enum
pub enum AppError {
    ValidationError(String),
    DatabaseError(sqlx::Error),
    NotFound(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let (status, error_message) = match self {
            AppError::ValidationError(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            AppError::DatabaseError(err) => {
                // We use structured tracing here to log the real error for developers,
                // but we hide the SQL error from the client to prevent data leaks.
                tracing::error!("Database error occurred: {:?}", err);
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error".to_string())
            }
        };

        (status, Json(serde_json::json!({ "error": error_message }))).into_response()
    }
}

/// POST /campaigns
// We can use the tracing macro to automatically log the inputs of this function
#[tracing::instrument(skip(state))]
async fn create_campaign(
    State(state): State<AppState>,
    Json(payload): Json<CreateCampaignRequest>,
) -> Result<(StatusCode, Json<Campaign>), AppError> {

    if let Err(e) = payload.validate() {
        return Err(AppError::ValidationError(e.to_string()));
    }

    let now = chrono::Utc::now();
    let new_campaign = Campaign {
        id: Uuid::new_v4(),
        name: payload.name,
        status: crate::domain::campaign::CampaignStatus::Paused,
        budget: payload.budget,
        start_date: payload.start_date,
        end_date: payload.end_date,
        created_at: now,
        updated_at: now,
    };

    let repo = CampaignRepository::new(state.db_pool);
    let created = repo
        .create_campaign(&new_campaign)
        .await
        .map_err(AppError::DatabaseError)?;

    // 2. Real-time Kafka Sync
    // We pass the fully hydrated 'created' object (with real DB timestamps) to Kafka
    if let Err(err) = state.kafka_publisher.publish_campaign_event(&created).await {
        // We do not fail the HTTP request if Kafka fails, because the DB commit succeeded.
        // In a true Outbox pattern, this mismatch wouldn't happen. For now, we aggressively log it.
        tracing::error!("CRITICAL: Failed to publish campaign {} to Kafka: {:?}", created.id, err);
    } else {
        tracing::info!("Successfully created and published campaign: {}", created.id);
    }
    Ok((StatusCode::CREATED, Json(created)))
}

/// PATCH /campaigns/{id}/status
#[tracing::instrument(skip(state))]
async fn update_status(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(payload): Json<UpdateStatusRequest>,
) -> Result<Json<Campaign>, AppError> {

    let repo = CampaignRepository::new(state.db_pool.clone());

    // 1. Update the DB
    let updated = repo
        .update_campaign_status(id, payload.status.clone())
        .await
        .map_err(AppError::DatabaseError)?;

    // 2. Real-time Kafka Sync (Only when status changes!)
    // If it turns Active, the RTB engine caches it and starts bidding.
    // If it turns Paused, the RTB engine evicts it from cache and stops bidding.
    if let Err(err) = state.kafka_publisher.publish_campaign_event(&updated).await {
        tracing::error!("CRITICAL: Failed to publish status update for {} to Kafka: {:?}", updated.id, err);
    } else {
        // As discussed, we let the route handler do the logging!
        tracing::info!("Campaign {} status changed to {:?} and broadcasted", updated.id, updated.status);
    }

    Ok(Json(updated))
}

/// GET /campaigns/{id}
#[tracing::instrument(skip(state))]
async fn get_campaign(
    State(state): State<AppState>,
    Path(id): Path<Uuid>, // Extracts the UUID directly from the URL path
) -> Result<Json<Campaign>, AppError> {

    let repo = CampaignRepository::new(state.db_pool);
    let campaign = repo
        .get_campaign(id)
        .await
        .map_err(AppError::DatabaseError)?
        .ok_or_else(|| AppError::NotFound(format!("Campaign with ID {} not found", id)))?;

    Ok(Json(campaign))
}

/// Get All campaigns
#[tracing::instrument(skip(state))]
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

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", post(create_campaign).get(list_campaigns))
        .route("/:id", get(get_campaign))
        .route("/:id/status", axum::routing::patch(update_status))
}