ALTER TABLE songs ADD COLUMN cover_art_object_key TEXT;
ALTER TABLE songs ADD COLUMN cover_art_prompt TEXT;
ALTER TABLE songs ADD COLUMN cover_art_model TEXT;
ALTER TABLE songs ADD COLUMN cover_art_created_at INTEGER;

CREATE INDEX IF NOT EXISTS idx_songs_cover_art_missing ON songs (cover_art_object_key, completed_at DESC);

