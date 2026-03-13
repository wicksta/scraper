ALTER TABLE app_ingest_jobs
  ADD COLUMN user_id INT NULL AFTER id,
  ADD INDEX idx_app_ingest_jobs_user_id (user_id);
