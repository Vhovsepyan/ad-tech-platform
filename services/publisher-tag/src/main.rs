mod handlers;
mod state;

use axum::{routing::get, Router};
use state::AppState;
use std::{net::SocketAddr, sync::Arc};
use tower_http::{services::ServeDir, trace::TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    // Initialize structured JSON/Console logging
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "publisher_tag=info,tower_http=debug".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("Starting Professional Publisher & SSP Server...");

    let state = Arc::new(AppState::new());
    let static_files = ServeDir::new("services/publisher-tag/public");

    let app = Router::new()
        .nest_service("/", static_files)
        .route("/ssp/ad", get(handlers::handle_ad_request))
        .with_state(state)
        // Automatically logs every HTTP request and its duration
        .layer(TraceLayer::new_for_http());

    let addr = SocketAddr::from(([0, 0, 0, 0], 8084));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    tracing::info!("Server listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}