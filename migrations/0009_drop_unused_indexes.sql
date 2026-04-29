-- Drop indexes not used by any query; saves 3 row writes per song INSERT.

-- Superseded by idx_songs_genre_key_completed (all queries filter on genre_key, not primary_genre).
DROP INDEX IF EXISTS idx_songs_genre_completed;

-- No query filters on cover_art_object_key.
DROP INDEX IF EXISTS idx_songs_cover_art_missing;

-- No query filters on request_id.
DROP INDEX IF EXISTS idx_songs_request_id;
