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
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Impression {
    pub id: String,
    pub bidfloor: Option<f64>,
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
    pub price: f64,
    pub adid: Option<String>,
    pub crid: Option<String>,
}