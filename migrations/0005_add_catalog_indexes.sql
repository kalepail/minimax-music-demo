ALTER TABLE songs ADD COLUMN genre_key TEXT;

UPDATE songs
SET genre_key = lower(trim(replace(replace(replace(replace(primary_genre, '/', ' '), '-', ' '), '_', ' '), '  ', ' ')))
WHERE primary_genre IS NOT NULL AND trim(primary_genre) != '';

CREATE INDEX IF NOT EXISTS idx_songs_title_nocase ON songs (title COLLATE NOCASE, completed_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_stations_updated_at ON stations (updated_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_songs_genre_key_completed ON songs (genre_key, completed_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_songs_station_genre_key_completed ON songs (station_id, genre_key, completed_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_songs_station_mood_completed ON songs (station_id, mood, completed_at DESC, id DESC);

PRAGMA optimize;
