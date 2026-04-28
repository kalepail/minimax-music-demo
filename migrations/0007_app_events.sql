CREATE TABLE IF NOT EXISTS app_events (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	created_at INTEGER NOT NULL,
	level TEXT NOT NULL,
	event TEXT NOT NULL,
	operation TEXT,
	model TEXT,
	song_id TEXT,
	request_id TEXT,
	workflow_instance_id TEXT,
	job_id TEXT,
	status_code INTEGER,
	duration_ms INTEGER,
	message TEXT,
	details_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_app_events_created_at ON app_events (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_app_events_level_created ON app_events (level, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_app_events_event_created ON app_events (event, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_app_events_model_created ON app_events (model, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_app_events_song_created ON app_events (song_id, created_at DESC);
