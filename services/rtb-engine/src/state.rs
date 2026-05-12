use crate::bidding::BiddingStrategy;
use redis_utils::RedisManager;
use std::sync::Arc;

pub struct AppState {
    pub bidding_engine: Arc<dyn BiddingStrategy>,
    pub redis_manager: RedisManager,
    // Notice we do NOT wrap RedisManager in an Arc.
    // The MultiplexedConnection inside it is already designed to be cloned cheaply.
}