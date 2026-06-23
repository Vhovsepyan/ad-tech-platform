mod aggregator;
mod consumer;
mod repository;

use aggregator::BatchAggregator;
use consumer::EventConsumer;
use repository::MetricsRepository;
use sqlx::postgres::PgPoolOptions;
use std::{env, time::Duration};
use tokio::time::Instant;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    println!("Starting Professional Reconciliation Worker...");

    // 1. Initialize Persistence (Postgres)
    let db_url = env::var("DATABASE_URL").expect("CRITICAL: DATABASE_URL missing");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .expect("CRITICAL: Failed to connect to Postgres.");
    let repository = MetricsRepository::new(pool);
    repository.ensure_offset_table().await;

    // 2. Initialize Infrastructure (Kafka)
    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:19092".into());
    let topic = env::var("KAFKA_EVENTS_TOPIC").unwrap_or_else(|_| "ad_events".into());

    let initial_offset = repository.load_offset(&topic).await;
    println!("Resuming from Kafka offset {}", initial_offset);

    let mut consumer = EventConsumer::new(vec![brokers], topic.clone(), initial_offset)
        .await
        .expect("CRITICAL: Failed to connect to Kafka.");

    // 3. Initialize Domain State
    let mut aggregator = BatchAggregator::new();
    let mut last_flush = Instant::now();

    println!("✅ Worker fully initialized. Listening for events...");

    // 4. The Orchestration Loop
    loop {
        match consumer.fetch_events().await {
            Ok((payloads, is_caught_up)) => {
                // Delegate parsing and math to the aggregator
                for payload in payloads {
                    aggregator.process_event(&payload);
                }

                // Yield to save CPU if we are at the end of the topic
                if is_caught_up {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
            Err(e) => {
                println!("⚠️ Kafka fetch error: {:?}. Retrying in 2s...", e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }

        // 5. Check trigger conditions for a database flush (Every 5s OR if memory batch gets large)
        if (last_flush.elapsed() >= Duration::from_secs(5) && !aggregator.is_empty()) || aggregator.len() > 5000 {
            let batch = aggregator.drain_batch();
            repository.flush_and_commit(batch, consumer.current_offset, &topic).await;
            last_flush = Instant::now();
        }
    }
}