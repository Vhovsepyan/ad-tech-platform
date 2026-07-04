mod handlers;
mod store;
mod templating;

use axum::{routing::get, Router};
use handlers::AppState;
use std::{env, net::SocketAddr, sync::Arc};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "dsp_server=info,tower_http=debug".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("Starting DSP Creative Ad Server...");

    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());

    let state = AppState {
        store: Arc::new(store::CreativeStore::new(&redis_url)),
        macro_engine: Arc::new(templating::MacroEngine::new()),
        tracker_url: env::var("EVENT_TRACKER_URL").unwrap_or_else(|_| "http://localhost:8082".into()),
    };

    let app = Router::new()
        .route("/render/:creative_id", get(handlers::serve_creative))
        .route("/creative/:creative_id", axum::routing::post(handlers::upsert_creative))
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    let addr = SocketAddr::from(([0, 0, 0, 0], 8085));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    tracing::info!("Creative Ad Server listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}