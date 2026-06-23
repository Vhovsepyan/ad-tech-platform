use core_models::AdEvent;
use core_models::EventType;
use rust_decimal::Decimal;
use std::collections::HashMap;

/// Represents the financial and engagement metrics for a single campaign
#[derive(Default, Debug, Clone)]
pub struct CampaignMetrics {
    pub impressions: i32,
    pub clicks: i32,
    pub spend: Decimal,
}

/// Accumulates high-velocity events into memory-efficient micro-batches
#[derive(Default)]
pub struct BatchAggregator {
    metrics: HashMap<String, CampaignMetrics>,
}

impl BatchAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Parses a raw JSON payload and applies the AdTech pricing rules
    pub fn process_event(&mut self, payload: &[u8]) {
        if let Ok(event) = serde_json::from_slice::<AdEvent>(payload) {
            let metrics = self.metrics.entry(event.campaign_id).or_default();

            match event.event_type {
                EventType::Impression => {
                    metrics.impressions += 1;
                    let cpm = event.clearing_price.unwrap_or(Decimal::new(2, 0));
                    metrics.spend += cpm / Decimal::from(1000);
                }
                EventType::Click => {
                    metrics.clicks += 1;
                    metrics.spend += Decimal::new(5, 2); // $0.05 CPC
                }
                // No wildcard: exhaustive matching forces this file to be updated
                // whenever a new EventType variant is added to core-models.
            }
        }
    }

    /// Checks if the current batch has data
    pub fn is_empty(&self) -> bool {
        self.metrics.is_empty()
    }

    /// Returns the count of unique campaigns in the current batch
    pub fn len(&self) -> usize {
        self.metrics.len()
    }

    /// Extracts the current batch for flushing, leaving an empty map behind
    pub fn drain_batch(&mut self) -> HashMap<String, CampaignMetrics> {
        std::mem::take(&mut self.metrics)
    }
}