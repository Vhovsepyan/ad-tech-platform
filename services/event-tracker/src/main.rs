use axum::{
    extract::{Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Redirect},
    routing::get,
    Router,
};
use core_models::{AdEvent, EventType};
use serde::Deserialize;
use std::{sync::Arc, time::{SystemTime, UNIX_EPOCH}};
use tokio::signal;

// Replace the TRANSPARENT_GIF constant with this:
const DEBUG_SVG: &[u8] = b"<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"50\" height=\"50\"><rect width=\"50\" height=\"50\" fill=\"red\"/></svg>";
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
    pub price: Option<f64>,
}

// 1. Define the Application State
pub struct AppState {
    pub producer: kafka_utils::AsyncEventProducer,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    println!("Initializing Event Tracker on Edge...");

    // 2. Setup Kafka connection
    let kafka_brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:19092".into());
    // We create a NEW topic for tracking data, separate from campaign updates
    let kafka_topic = std::env::var("KAFKA_EVENTS_TOPIC").unwrap_or_else(|_| "ad_events".into());

    let producer = kafka_utils::AsyncEventProducer::new(vec![kafka_brokers], kafka_topic)
        .await
        .expect("CRITICAL: Failed to connect Producer to Kafka.");

    let state = Arc::new(AppState { producer });

    // 3. Mount routes with state
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

/// Helper to get current time
fn now_ms() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
}

async fn track_impression(
    State(state): State<Arc<AppState>>,
    Query(params): Query<TrackingParams>,
) -> impl IntoResponse {

    // Create the structured event
    let event = AdEvent {
        event_type: EventType::Impression,
        campaign_id: params.campaign_id,
        bid_id: params.bid_id,
        clearing_price: params.price,
        timestamp_ms: now_ms(),
    };

    // Fire and forget (sub-millisecond operation)
    if let Ok(json_bytes) = serde_json::to_vec(&event) {
        state.producer.emit(json_bytes);
    }

    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, "image/gif".parse().unwrap());
    headers.insert(header::CACHE_CONTROL, "no-store, no-cache, must-revalidate".parse().unwrap());
    headers.insert(header::PRAGMA, "no-cache".parse().unwrap());

    (headers, DEBUG_SVG)
}

async fn track_click(
    State(state): State<Arc<AppState>>,
    Query(params): Query<TrackingParams>,
) -> impl IntoResponse {

    let destination = params.r.unwrap_or_else(|| "https://google.com".to_string());

    let event = AdEvent {
        event_type: EventType::Click,
        campaign_id: params.campaign_id,
        bid_id: params.bid_id,
        clearing_price: params.price,
        timestamp_ms: now_ms(),
    };

    // Fire and forget
    if let Ok(json_bytes) = serde_json::to_vec(&event) {
        state.producer.emit(json_bytes);
    }

    Redirect::temporary(&destination).into_response()
}

/// Graceful Shutdown Handler (Standardized across our monorepo)
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