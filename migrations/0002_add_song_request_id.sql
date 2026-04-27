ALTER TABLE songs ADD COLUMN request_id TEXT;

CREATE INDEX IF NOT EXISTS idx_songs_request_id ON songs (request_id);

