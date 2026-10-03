-- MySQL migration: optional reMarkable notebook destination per project.
-- Rollback: ALTER TABLE personal_projects DROP COLUMN remarkable_document;
ALTER TABLE personal_projects
  ADD COLUMN remarkable_document VARCHAR(512) NULL AFTER category;
