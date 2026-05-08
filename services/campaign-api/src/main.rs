mod domain;
mod repository;
mod routes;

use axum::{routing::get, Router};
use sqlx::postgres::PgPoolOptions;
use std::env;
use std::time::Duration;
use tower_http::{
    cors::{Any, CorsLayer},
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
    // 1. Initialize Structured Tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info,campaign_api=debug,tower_http=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://adtech:secretpassword@localhost:5432/adtech_db".to_string());

    tracing::info!("Connecting to database...");

    let pool = PgPoolOptions::new()
        .max_connections(50)
        .acquire_timeout(Duration::from_secs(3))
        .idle_timeout(Duration::from_secs(600))
        .connect(&database_url)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;
    tracing::info!("Database migrations applied successfully.");

    let state = AppState { db_pool: pool };

    // 2. Configure CORS (Cross-Origin Resource Sharing)
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // 3. Construct Axum Router with Middleware
    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .nest("/api/v1/campaigns", routes::campaign_routes::router())
        .with_state(state)
        // Add tracing to log every incoming request and outgoing response
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().include_headers(true))
                .on_response(DefaultOnResponse::new().include_headers(true)),
        )
        // Attach a unique UUID to every request for distributed tracing
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
        // Attach CORS rules
        .layer(cors);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    tracing::info!("Campaign API listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;

    Ok(())
}