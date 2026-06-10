mod handlers;
mod state;
mod store;

use axum::{routing::get, Router};
use state::AppState;
use std::{collections::HashSet, env, sync::Arc};
use store::AudienceStore;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    println!("Starting Audience Edge Service...");

    let redis_url = env::var("AUDIENCE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let store = AudienceStore::new(&redis_url).await.expect("Failed to connect to Audience Redis");

    let allowed_redirect_hosts: HashSet<String> = env::var("ALLOWED_REDIRECT_HOSTS")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if allowed_redirect_hosts.is_empty() {
        eprintln!("WARNING: ALLOWED_REDIRECT_HOSTS is not set — all cookie sync redirects will be rejected.");
    } else {
        println!("Allowed redirect hosts: {:?}", allowed_redirect_hosts);
    }

    let cookie_domain = env::var("COOKIE_DOMAIN")
        .expect("COOKIE_DOMAIN is required (e.g. .yourdsp.com)");

    let state = Arc::new(AppState {
        store: Arc::new(store),
        allowed_redirect_hosts,
        cookie_domain,
    });

    let app = Router::new()
        .route("/sync", get(handlers::cookie_sync))
        .route("/internal/audience/:uid", get(handlers::lookup_audience))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8083").await.unwrap();
    println!("Audience Edge listening on {}", listener.local_addr().unwrap());

    axum::serve(listener, app).await.unwrap();
}