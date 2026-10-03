-- MySQL migration: canonical project categories and aliases.
-- Target DB: ngist/MySQL app database (MYSQL_DATABASE in runtime env).
--
-- Rollback:
--   DROP TABLE IF EXISTS personal_project_aliases;
--   ALTER TABLE personal_projects DROP COLUMN category;

ALTER TABLE personal_projects
  ADD COLUMN category VARCHAR(16) NOT NULL DEFAULT 'work' AFTER status,
  ADD KEY idx_personal_projects_category_archived (category, archived_at);

UPDATE personal_projects
SET category = CASE
  WHEN LOWER(name) IN ('boat', 'home') THEN 'home'
  ELSE 'work'
END;

CREATE TABLE personal_project_aliases (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  canonical_project_id BIGINT UNSIGNED NOT NULL,
  alias VARCHAR(255) NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  UNIQUE KEY uq_personal_project_aliases_alias (alias),
  KEY idx_personal_project_aliases_canonical (canonical_project_id, alias),
  CONSTRAINT fk_personal_project_aliases_canonical
    FOREIGN KEY (canonical_project_id) REFERENCES personal_projects (id)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
