mod aggregator;
mod clickhouse_writer;
mod consumer;
mod repository;

use aggregator::BatchAggregator;
use clickhouse_writer::ClickHouseWriter;
use consumer::EventConsumer;
use core_models::AdEvent;
use repository::MetricsRepository;
use sqlx::postgres::PgPoolOptions;
use std::{env, time::Duration};
use tokio::time::Instant;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    println!("Starting Reconciliation Worker...");

    // 1. Postgres
    let db_url = env::var("DATABASE_URL").expect("CRITICAL: DATABASE_URL missing");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .expect("CRITICAL: Failed to connect to Postgres.");
    let repository = MetricsRepository::new(pool);
    repository.ensure_offset_table().await;

    // 2. Kafka — partition is configurable; defaults to 0.
    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:19092".into());
    let topic = env::var("KAFKA_EVENTS_TOPIC").unwrap_or_else(|_| "ad_events".into());
    let partition: i32 = env::var("KAFKA_PARTITION")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let initial_offset = repository.load_offset(&topic).await;
    println!("Resuming from Kafka offset {}", initial_offset);

    let mut consumer = EventConsumer::new(vec![brokers], topic.clone(), partition, initial_offset)
        .await
        .expect("CRITICAL: Failed to connect to Kafka.");

    // 3. Redis (for budget counter sync after each Postgres flush)
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let redis = redis_utils::RedisManager::new(&redis_url)
        .await
        .expect("CRITICAL: Failed to connect to Redis.");

    // 4. ClickHouse (for real-time analytics; failures are non-fatal)
    let ch_url = env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".into());
    let ch_db = env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "adtech_events".into());
    let ch_user = env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "adtech".into());
    let ch_password = env::var("CLICKHOUSE_PASSWORD").unwrap_or_else(|_| "secretpassword".into());
    let clickhouse = ClickHouseWriter::new(&ch_url, &ch_db, &ch_user, &ch_password);
    clickhouse.ensure_table().await;

    // 5. Domain state
    let mut aggregator = BatchAggregator::new();
    let mut clickhouse_buffer: Vec<clickhouse_writer::AdEventRow> = Vec::new();
    let mut last_flush = Instant::now();

    println!("Worker fully initialized. Listening for events...");

    // 6. Orchestration loop
    loop {
        match consumer.fetch_events().await {
            Ok((payloads, is_caught_up)) => {
                for payload in payloads {
                    // Buffer raw events for ClickHouse before aggregation
                    if let Ok(event) = serde_json::from_slice::<AdEvent>(&payload) {
                        clickhouse_buffer.push(clickhouse_writer::to_row(&event));
                    }
                    aggregator.process_event(&payload);
                }

                if is_caught_up {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
            Err(e) => {
                println!("Kafka fetch error: {:?}. Retrying in 2s...", e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }

        let should_flush = (last_flush.elapsed() >= Duration::from_secs(5) && !aggregator.is_empty())
            || aggregator.len() > 5000;

        if should_flush {
            let batch = aggregator.drain_batch();
            let new_budgets = repository.flush_and_commit(batch, consumer.current_offset, &topic).await;

            // Sync Redis budget counters with the authoritative Postgres values.
            for (campaign_id, budget) in &new_budgets {
                if let Err(e) = redis.init_budget_counter(&campaign_id.to_string(), *budget).await {
                    eprintln!("WARNING: Failed to sync Redis budget for {}: {:?}", campaign_id, e);
                }
            }

            // Write raw events to ClickHouse for analytics (non-fatal if it fails).
            let ch_batch = std::mem::take(&mut clickhouse_buffer);
            clickhouse.write_batch(&ch_batch).await;

            last_flush = Instant::now();
        }
    }
}
