use clickhouse::{Client, Row};
use core_models::{AdEvent, EventType};
use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;

#[derive(Debug, Row, Serialize)]
pub struct AdEventRow {
    pub event_type: String,
    pub campaign_id: String,
    pub bid_id: String,
    pub clearing_price: Option<f64>,
    pub timestamp_ms: u64,
}

pub struct ClickHouseWriter {
    client: Client,
}

impl ClickHouseWriter {
    pub fn new(url: &str, database: &str, user: &str, password: &str) -> Self {
        let client = Client::default()
            .with_url(url)
            .with_database(database)
            .with_user(user)
            .with_password(password);
        Self { client }
    }

    pub async fn ensure_table(&self) {
        let result = self.client.query(
            "CREATE TABLE IF NOT EXISTS ad_events (
                event_type    LowCardinality(String),
                campaign_id   String,
                bid_id        String,
                clearing_price Nullable(Float64),
                timestamp_ms  UInt64,
                created_at    DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (campaign_id, timestamp_ms)"
        )
        .execute()
        .await;

        match result {
            Ok(_) => println!("ClickHouse ad_events table ready."),
            Err(e) => eprintln!("WARNING: Could not create ClickHouse table: {:?}", e),
        }
    }

    pub async fn write_batch(&self, rows: &[AdEventRow]) {
        if rows.is_empty() {
            return;
        }
        let result: Result<_, _> = async {
            let mut insert = self.client.insert("ad_events")?;
            for row in rows {
                insert.write(row).await?;
            }
            insert.end().await
        }.await;

        if let Err(e) = result {
            eprintln!("WARNING: ClickHouse write error (non-fatal): {:?}", e);
        }
    }
}

pub fn to_row(event: &AdEvent) -> AdEventRow {
    AdEventRow {
        event_type: match event.event_type {
            EventType::Impression => "IMPRESSION".to_string(),
            EventType::Click => "CLICK".to_string(),
        },
        campaign_id: event.campaign_id.clone(),
        bid_id: event.bid_id.clone(),
        clearing_price: event.clearing_price.and_then(|p| p.to_f64()),
        timestamp_ms: event.timestamp_ms as u64,
    }
}
