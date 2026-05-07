pub mod campaign;
pub mod creative;
pub mod targeting;

// Re-export the core structs for easier importing across the app
pub use campaign::{Campaign, CampaignStatus};