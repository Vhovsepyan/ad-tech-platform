mod bidding;
mod handlers;
mod state;

use axum::{routing::{get, post}, Router};
use bidding::DefaultNoBidStrategy;
use state::AppState;
use std::sync::Arc;
use tokio::signal;

#[tokio::main]
async fn main() {
    // 1. Load Environment Variables
    dotenvy::dotenv().ok();

    // 2. Establish Infrastructure Connections (Fail Fast)
    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL missing");
    let redis_manager = redis_utils::RedisManager::new(&redis_url)
        .await
        .expect("CRITICAL: Failed to connect to Redis.");
    println!("Redis connected successfully.");

    // --- NEW: 3. Setup Kafka Consumer ---
    let kafka_brokers = std::env::var("KAFKA_BROKERS").expect("KAFKA_BROKERS missing");
    let kafka_topic = std::env::var("KAFKA_CAMPAIGN_TOPIC").expect("KAFKA_CAMPAIGN_TOPIC missing");

    let consumer = kafka_utils::CampaignConsumer::new(
        vec![kafka_brokers],
        kafka_topic,
        redis_manager.clone(), // We clone the multiplexer here!
    )
        .await
        .expect("CRITICAL: Failed to connect to Kafka.");

    // Spawn the infinite consumer loop as a detached background task
    tokio::spawn(async move {
        consumer.run().await;
    });
    // ------------------------------------

    // 4. Initialize State
    let active_strategy = bidding::ActiveCampaignStrategy::new(redis_manager.clone());

    let state = Arc::new(AppState {
        bidding_engine: Arc::new(active_strategy),
        redis_manager,
    });

    // 5. Configure the Router
    let app = Router::new()
        .route("/healthz", get(handlers::health::healthz))
        .route("/bid", post(handlers::bid::handle_bid))
        .with_state(state);

    // 6. Bind and Serve
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8081").await.unwrap();
    println!("RTB Engine HTTP Server listening on {}", listener.local_addr().unwrap());

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
}

/// Graceful Shutdown Handler
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    println!("Shutdown signal received, draining connections...");
}