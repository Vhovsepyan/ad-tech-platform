ALTER TABLE campaigns
    ADD COLUMN IF NOT EXISTS target_segments TEXT[] NOT NULL DEFAULT '{}';
