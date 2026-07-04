CREATE TABLE campaign_outbox (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id UUID NOT NULL,
    event_type   TEXT NOT NULL DEFAULT 'campaign.updated',
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);

-- Partial index: only unprocessed rows are queried by the poller, so the index stays small.
CREATE INDEX idx_campaign_outbox_unprocessed ON campaign_outbox (created_at)
    WHERE processed_at IS NULL;
