-- MySQL migration: Newmark initials resolver lookup table
-- Target DB: wickhams_monitor (or MYSQL_DATABASE in runtime env)

DROP TABLE IF EXISTS `newmark_initial_resolver`;

CREATE TABLE `newmark_initial_resolver` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `initial_token` VARCHAR(16) NOT NULL,
  `person_name` VARCHAR(255) NOT NULL,
  `person_name_norm` VARCHAR(255) NOT NULL,
  `first_name` VARCHAR(120) NULL,
  `last_name` VARCHAR(120) NULL,
  `user_id` BIGINT NULL,
  `confidence` ENUM('high', 'medium', 'low') NOT NULL DEFAULT 'low',
  `score` DECIMAL(8,4) NOT NULL DEFAULT 0,
  `evidence_count` INT NOT NULL DEFAULT 0,
  `sources_json` JSON NOT NULL,
  `is_locked` TINYINT(1) NOT NULL DEFAULT 0,
  `locked_by` VARCHAR(120) NULL,
  `locked_at` DATETIME NULL,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `last_seen_at` DATETIME NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_initial_token` (`initial_token`),
  KEY `idx_initial_token` (`initial_token`),
  KEY `idx_user_id` (`user_id`),
  KEY `idx_conf_score` (`confidence`, `score`),
  KEY `idx_locked` (`is_locked`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
