use crate::store::AudienceStore;
use std::collections::HashSet;
use std::sync::Arc;

pub struct AppState {
    pub store: Arc<AudienceStore>,
    /// Allowlisted hostnames that `return_url` may redirect to (e.g. "ib.adnxs.com").
    /// Loaded from ALLOWED_REDIRECT_HOSTS at startup. Empty set rejects all redirects.
    pub allowed_redirect_hosts: HashSet<String>,
}
