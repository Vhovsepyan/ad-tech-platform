use reqwest::Client;
use std::time::Duration;

#[derive(Clone)]
pub struct AppState {
    pub http_client: Client,
    pub rtb_url: String,
    pub tracker_url: String,
}

impl AppState {
    pub fn new() -> Self {
        // Build a highly optimized connection pool
        let http_client = Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(100)
            .timeout(Duration::from_millis(150)) // Strict SLA timeout
            .build()
            .expect("Failed to build HTTP client");

        Self {
            http_client,
            rtb_url: std::env::var("RTB_ENGINE_URL").unwrap_or_else(|_| "http://127.0.0.1:8081/bid".into()),
            tracker_url: std::env::var("EVENT_TRACKER_URL").unwrap_or_else(|_| "http://127.0.0.1:8082".into()),
        }
    }
}