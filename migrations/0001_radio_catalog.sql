CREATE TABLE IF NOT EXISTS songs (
	id TEXT PRIMARY KEY,
	station_id TEXT NOT NULL DEFAULT 'main',
	title TEXT NOT NULL,
	prompt TEXT NOT NULL,
	request_text TEXT,
	format TEXT NOT NULL,
	audio_object_key TEXT NOT NULL,
	audio_content_type TEXT NOT NULL,
	primary_genre TEXT,
	mood TEXT,
	energy INTEGER,
	bpm_min INTEGER,
	bpm_max INTEGER,
	vocal_style TEXT,
	created_at INTEGER NOT NULL,
	completed_at INTEGER NOT NULL,
	duration_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_songs_completed_at ON songs (completed_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_songs_station_completed ON songs (station_id, completed_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_songs_genre_completed ON songs (primary_genre, completed_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_songs_mood_completed ON songs (mood, completed_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_songs_energy_completed ON songs (energy DESC, completed_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS song_tags (
	song_id TEXT NOT NULL,
	tag TEXT NOT NULL,
	PRIMARY KEY (song_id, tag),
	FOREIGN KEY (song_id) REFERENCES songs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_song_tags_tag ON song_tags (tag, song_id);

CREATE TABLE IF NOT EXISTS stations (
	id TEXT PRIMARY KEY,
	name TEXT NOT NULL,
	description TEXT,
	genre_filter TEXT,
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stations_genre ON stations (genre_filter);
