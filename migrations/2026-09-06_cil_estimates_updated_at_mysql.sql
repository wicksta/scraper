-- Adds an accurate last-saved timestamp for the nGISt MySQL CIL estimate list.
-- Existing estimates retain their original created_at value as their initial updated_at value.

ALTER TABLE cil_estimates
  ADD COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER created_at;

UPDATE cil_estimates
SET updated_at = created_at;

-- Rollback (run only after confirming no dependent code requires the column):
-- ALTER TABLE cil_estimates DROP COLUMN updated_at;
