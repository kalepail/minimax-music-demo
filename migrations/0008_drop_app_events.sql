DROP TABLE IF EXISTS app_events;

-- Remove generation detail blobs from songs; data now lives in R2.
UPDATE songs SET generation_input_json = NULL, prompt_plan_json = NULL;
