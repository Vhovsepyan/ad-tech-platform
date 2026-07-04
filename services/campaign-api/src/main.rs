mod domain;
mod repository;
mod routes;
mod infrastructure;

use axum::{routing::get, Router};
use infrastructure::kafka::CampaignEventPublisher;
use repository::campaign_repo::CampaignRepository;
use sqlx::postgres::PgPoolOptions;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tower_http::{
    request_id::{MakeRequestUuid, SetRequestIdLayer},
    trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer},
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Clone)]
pub struct AppState {
    pub db_pool: sqlx::PgPool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info,campaign_api=debug,tower_http=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    dotenvy::dotenv().ok();

    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://adtech:secretpassword@localhost:5432/adtech_db".to_string());

    let kafka_broker = env::var("KAFKA_BROKER")
        .unwrap_or_else(|_| "localhost:19092".to_string());

    let kafka_campaign_topic = env::var("KAFKA_CAMPAIGN_TOPIC")
        .unwrap_or_else(|_| "campaign-updates".to_string());

    tracing::info!("Connecting to database...");

    let pool = PgPoolOptions::new()
        .max_connections(50)
        .acquire_timeout(Duration::from_secs(3))
        .idle_timeout(Duration::from_secs(600))
        .connect(&database_url)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;
    tracing::info!("Database migrations applied successfully.");

    let publisher = Arc::new(
        CampaignEventPublisher::new(vec![kafka_broker], &kafka_campaign_topic).await?
    );

    // Spawn the outbox poller — reads unprocessed outbox rows and publishes to Kafka.
    let outbox_pool = pool.clone();
    let outbox_publisher = publisher.clone();
    tokio::spawn(async move {
        infrastructure::outbox::run_outbox_poller(outbox_pool, outbox_publisher).await;
    });

    // Spawn the flight expiry job — pauses ACTIVE campaigns whose end_date has passed.
    let expiry_pool = pool.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            let repo = CampaignRepository::new(expiry_pool.clone());
            match repo.expire_ended_campaigns().await {
                Ok(expired) if !expired.is_empty() => {
                    tracing::info!("Expired {} campaigns past their end_date", expired.len());
                }
                Err(e) => tracing::error!("Flight expiry job error: {:?}", e),
                _ => {}
            }
        }
    });

    let state = AppState { db_pool: pool };

    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .nest("/api/v1/campaigns", routes::campaign_routes::router())
        .with_state(state)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().include_headers(true))
                .on_response(DefaultOnResponse::new().include_headers(true)),
        )
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    tracing::info!("Campaign API listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;

    Ok(())
}
