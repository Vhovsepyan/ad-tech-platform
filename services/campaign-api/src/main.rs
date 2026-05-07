mod domain;
mod repository;
mod routes; // We will build this next

use axum::{routing::get, Router};
use sqlx::postgres::PgPoolOptions;
use std::env;
use std::time::Duration;

#[derive(Clone)]
pub struct AppState {
    pub db_pool: sqlx::PgPool,
    // Future additions:
    // pub kafka_producer: Arc<KafkaProducer>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Load connection string (Ensure DATABASE_URL is in your .env file)
    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://adtech:secretpassword@localhost:5432/adtech_db".to_string());

    println!("Connecting to database...");

    // 2. Configure a production-ready Connection Pool
    let pool = PgPoolOptions::new()
        .max_connections(50) // AdTech gets high concurrency, prep the pool
        .acquire_timeout(Duration::from_secs(3)) // Fail fast if DB is unreachable
        .idle_timeout(Duration::from_secs(600)) // Clean up dead connections
        .connect(&database_url)
        .await?;

    // Run database migrations automatically on startup (Clever pattern for CD pipelines)
    sqlx::migrate!("./migrations").run(&pool).await?;
    println!("Database migrations applied successfully.");

    // 3. Build application state
    let state = AppState { db_pool: pool };

    // 4. Construct Axum Router (Injecting state)
    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .nest("/api/v1/campaigns", routes::campaign_routes::router())
        .with_state(state);

    // 5. Start the server
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    println!("Campaign API listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;

    Ok(())
}