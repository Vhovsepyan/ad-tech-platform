use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

// ==========================================
// OpenRTB 2.5/2.6 Simplified Bid Request
// ==========================================
#[derive(Debug, Deserialize, Serialize)]
pub struct BidRequest {
    pub id: String,
    pub imp: Vec<Impression>,
    pub site: Option<Site>,
    pub device: Option<Device>,
    pub user: Option<User>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Impression {
    pub id: String,
    #[serde(with = "rust_decimal::serde::float_option")]
    pub bidfloor: Option<Decimal>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Site {
    pub id: Option<String>,
    pub domain: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Device {
    pub ua: Option<String>,
    pub ip: Option<String>,
    pub ifa: Option<String>, //Mobile hardware ID (IDFA/GAID)
}

#[derive(Debug, Deserialize, Serialize)]
pub struct User {
    pub id: Option<String>,
    pub buyeruid: Option<String>,
}

// ==========================================
// OpenRTB Simplified Bid Response
// ==========================================
#[derive(Debug, Deserialize, Serialize)]
pub struct BidResponse {
    pub id: String,
    pub seatbid: Vec<SeatBid>,
    pub cur: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SeatBid {
    pub bid: Vec<Bid>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Bid {
    pub id: String,
    pub impid: String,
    #[serde(with = "rust_decimal::serde::float")]
    pub price: Decimal,
    pub adid: Option<String>,
    pub crid: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AdEvent {
    pub event_type: EventType,
    pub campaign_id: String,
    pub bid_id: String,
    #[serde(with = "rust_decimal::serde::float_option")]
    pub clearing_price: Option<Decimal>,
    pub timestamp_ms: u128,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum EventType {
    Impression,
    Click,
    // Future proof: Easy to add VideoStart, Conversion, etc.
}
