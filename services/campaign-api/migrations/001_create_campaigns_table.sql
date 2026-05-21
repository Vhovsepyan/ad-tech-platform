-- 1. Create the trigger function for auto-updating timestamps
CREATE OR REPLACE FUNCTION trigger_set_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. Create a native Postgres ENUM for campaign status
CREATE TYPE campaign_status AS ENUM ('ACTIVE', 'PAUSED', 'DELETED');

-- 3. Create the campaigns table
CREATE TABLE campaigns (
                           id UUID PRIMARY KEY,
                           name VARCHAR(255) NOT NULL,
                           status campaign_status NOT NULL DEFAULT 'PAUSED',
    -- NUMERIC(19,4) gives us 15 digits of whole dollars and 4 digits of fractional cents
                           budget NUMERIC(19, 4) NOT NULL CHECK (budget > 0),
                           start_date TIMESTAMPTZ NOT NULL,
                           end_date TIMESTAMPTZ NOT NULL,
                           created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                           updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4. Attach the trigger to the table
CREATE TRIGGER set_timestamp_campaigns
    BEFORE UPDATE ON campaigns
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_timestamp();

-- 5. Create an index on dates and status (Crucial for querying active campaigns fast)
CREATE INDEX idx_campaigns_status_dates ON campaigns(status, start_date, end_date);