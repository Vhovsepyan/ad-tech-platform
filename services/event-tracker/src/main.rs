use axum::{
    extract::{Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Redirect},
    routing::get,
    Router,
};
use core_models::{AdEvent, EventType};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::{sync::Arc, time::{SystemTime, UNIX_EPOCH}};
use tokio::signal;

const TRANSPARENT_GIF: &[u8] = &[
    0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00, 0x80, 0x00, 0x00, 0xff, 0xff, 0xff,
    0x00, 0x00, 0x00, 0x21, 0xf9, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00, 0x2c, 0x00, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x01, 0x00, 0x00, 0x02, 0x02, 0x44, 0x01, 0x00, 0x3b,
];

#[derive(Deserialize, Debug)]
pub struct TrackingParams {
    pub campaign_id: String,
    pub bid_id: String,
    pub r: Option<String>,
}

pub struct AppState {
    pub producer: kafka_utils::AsyncEventProducer,
    pub redis: redis_utils::RedisManager,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    println!("Initializing Event Tracker...");

    let kafka_brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:19092".into());
    let kafka_topic = std::env::var("KAFKA_EVENTS_TOPIC").unwrap_or_else(|_| "ad_events".into());
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());

    let producer = kafka_utils::AsyncEventProducer::new(vec![kafka_brokers], kafka_topic)
        .await
        .expect("CRITICAL: Failed to connect Producer to Kafka.");

    let redis = redis_utils::RedisManager::new(&redis_url)
        .await
        .expect("CRITICAL: Failed to connect to Redis.");

    let state = Arc::new(AppState { producer, redis });

    let app = Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route("/track/impression", get(track_impression))
        .route("/track/click", get(track_click))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8082").await.unwrap();
    println!("Event Tracker listening on {}", listener.local_addr().unwrap());

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal()).await.unwrap();
}

fn now_ms() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
}

/// Looks up the authoritative clearing price for a bid_id from Redis.
/// Returns None if the bid record is missing (e.g., expired or never stored).
async fn resolve_clearing_price(state: &AppState, bid_id: &str) -> Option<Decimal> {
    state.redis.get_bid_record(bid_id).await.ok().flatten()
}

async fn track_impression(
    State(state): State<Arc<AppState>>,
    Query(params): Query<TrackingParams>,
) -> impl IntoResponse {
    let clearing_price = resolve_clearing_price(&state, &params.bid_id).await;

    let event = AdEvent {
        event_type: EventType::Impression,
        campaign_id: params.campaign_id,
        bid_id: params.bid_id,
        clearing_price,
        timestamp_ms: now_ms(),
    };

    if let Ok(json_bytes) = serde_json::to_vec(&event) {
        state.producer.emit(json_bytes);
    }

    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, "image/gif".parse().unwrap());
    headers.insert(header::CACHE_CONTROL, "no-store, no-cache, must-revalidate".parse().unwrap());
    headers.insert(header::PRAGMA, "no-cache".parse().unwrap());

    (headers, TRANSPARENT_GIF)
}

async fn track_click(
    State(state): State<Arc<AppState>>,
    Query(params): Query<TrackingParams>,
) -> impl IntoResponse {
    let destination = params.r.unwrap_or_else(|| "https://google.com".to_string());

    let clearing_price = resolve_clearing_price(&state, &params.bid_id).await;

    let event = AdEvent {
        event_type: EventType::Click,
        campaign_id: params.campaign_id,
        bid_id: params.bid_id,
        clearing_price,
        timestamp_ms: now_ms(),
    };

    if let Ok(json_bytes) = serde_json::to_vec(&event) {
        state.producer.emit(json_bytes);
    }

    Redirect::temporary(&destination).into_response()
}

async fn shutdown_signal() {
    let ctrl_c = async { signal::ctrl_c().await.expect("failed to install Ctrl+C handler"); };
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate()).expect("failed to install handler").recv().await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = terminate => {}, }
    println!("Shutdown signal received, draining connections...");
}
