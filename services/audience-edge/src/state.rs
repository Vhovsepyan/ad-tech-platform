use crate::store::AudienceStore;
use std::collections::HashSet;
use std::sync::Arc;

pub struct AppState {
    pub store: Arc<AudienceStore>,
    /// Allowlisted hostnames that `return_url` may redirect to (e.g. "ib.adnxs.com").
    /// Loaded from ALLOWED_REDIRECT_HOSTS at startup. Empty set rejects all redirects.
    pub allowed_redirect_hosts: HashSet<String>,
    /// Domain attribute for the DSP tracking cookie (e.g. ".yourdsp.com").
    /// Loaded from COOKIE_DOMAIN at startup.
    pub cookie_domain: String,
}
