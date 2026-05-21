mod handlers;
mod store;

use axum::{routing::get, Router};
use std::{env, sync::Arc};
use store::AudienceStore;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    println!("Starting Audience Edge Service...");

    let redis_url = env::var("AUDIENCE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let store = AudienceStore::new(&redis_url).expect("Failed to connect to Audience Redis");

    let app = Router::new()
        .route("/sync", get(handlers::cookie_sync))
        .route("/internal/audience/:uid", get(handlers::lookup_audience))
        .with_state(Arc::new(store));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8083").await.unwrap();
    println!("Audience Edge listening on {}", listener.local_addr().unwrap());

    axum::serve(listener, app).await.unwrap();
}