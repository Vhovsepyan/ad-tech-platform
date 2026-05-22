use aho_corasick::AhoCorasick;
use std::sync::Arc;

pub struct MacroEngine {
    ac: AhoCorasick,
}

impl MacroEngine {
    pub fn new() -> Self {
        // Define the exact macros our DSP supports
        let patterns = &[
            "[AUCTION_ID]",
            "[CLEARING_PRICE]",
            "[CAMPAIGN_ID]",
            "[DSP_TRACKER_URL]",
            "[OPTIONAL_REDIRECT]",
        ];

        Self {
            ac: AhoCorasick::new(patterns).expect("Failed to build AhoCorasick automaton"),
        }
    }

    /// Replaces all macros in the raw creative HTML in a single pass O(n)
    pub fn render(&self, raw_html: &str, auction_id: &str, price: &str, campaign_id: &str, tracker_url: &str, render_url: &str) -> String {
        let replacements = &[auction_id, price, campaign_id, tracker_url, render_url];
        self.ac.replace_all(raw_html, replacements)
    }
}