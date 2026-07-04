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
use serde::Deserialize;
use uuid::Uuid;
use validator::Validate;

#[derive(Debug, Deserialize, Validate)]
pub struct CreateCampaignRequest {
    #[validate(length(min = 3))]
    pub name: String,
    pub budget: rust_decimal::Decimal,
    pub max_cpm: rust_decimal::Decimal,
    pub target_segments: Option<Vec<String>>,
    pub start_date: chrono::DateTime<chrono::Utc>,
    pub end_date: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateStatusRequest {
    pub status: crate::domain::campaign::CampaignStatus,
}

#[derive(Debug, Deserialize)]
pub struct PaginationParams {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

pub enum AppError {
    ValidationError(String),
    DatabaseError(sqlx::Error),
    NotFound(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let (status, error_message) = match self {
            AppError::ValidationError(msg) => (StatusCode::BAD_REQUEST, msg),
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            AppError::DatabaseError(err) => {
                tracing::error!("Database error occurred: {:?}", err);
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error".to_string())
            }
        };
        (status, Json(serde_json::json!({ "error": error_message }))).into_response()
    }
}

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
        max_cpm: payload.max_cpm,
        target_segments: payload.target_segments.unwrap_or_default(),
        start_date: payload.start_date,
        end_date: payload.end_date,
        created_at: now,
        updated_at: now,
    };

    let repo = CampaignRepository::new(state.db_pool);
    // Writes campaign + outbox row atomically; the outbox poller publishes to Kafka.
    let created = repo.create_campaign(&new_campaign).await.map_err(AppError::DatabaseError)?;

    tracing::info!("Created campaign {} (outbox row queued for Kafka)", created.id);
    Ok((StatusCode::CREATED, Json(created)))
}

#[tracing::instrument(skip(state))]
async fn update_status(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(payload): Json<UpdateStatusRequest>,
) -> Result<Json<Campaign>, AppError> {
    let repo = CampaignRepository::new(state.db_pool);
    // Writes status + outbox row atomically.
    let updated = repo
        .update_campaign_status(id, payload.status.clone())
        .await
        .map_err(AppError::DatabaseError)?;

    tracing::info!("Campaign {} status → {:?} (outbox row queued)", updated.id, updated.status);
    Ok(Json(updated))
}

#[tracing::instrument(skip(state))]
async fn get_campaign(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Campaign>, AppError> {
    let repo = CampaignRepository::new(state.db_pool);
    let campaign = repo
        .get_campaign(id)
        .await
        .map_err(AppError::DatabaseError)?
        .ok_or_else(|| AppError::NotFound(format!("Campaign with ID {} not found", id)))?;
    Ok(Json(campaign))
}

#[tracing::instrument(skip(state))]
async fn list_campaigns(
    State(state): State<AppState>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<Vec<Campaign>>, AppError> {
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);
    let repo = CampaignRepository::new(state.db_pool);
    let campaigns = repo.list_campaigns(limit, offset).await.map_err(AppError::DatabaseError)?;
    Ok(Json(campaigns))
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", post(create_campaign).get(list_campaigns))
        .route("/:id", get(get_campaign))
        .route("/:id/status", axum::routing::patch(update_status))
}
