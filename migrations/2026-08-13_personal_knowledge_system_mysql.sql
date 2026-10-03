-- MySQL migration: Personal source documents, tasks, notes, projects, people, tags, and event audit layer.
-- Target DB: ngist/MySQL app database (MYSQL_DATABASE in runtime env).
--
-- Rollback, in dependency order:
--   DROP TABLE IF EXISTS personal_note_tags;
--   DROP TABLE IF EXISTS personal_project_tags;
--   DROP TABLE IF EXISTS personal_task_tags;
--   DROP TABLE IF EXISTS personal_note_tasks;
--   DROP TABLE IF EXISTS personal_note_people;
--   DROP TABLE IF EXISTS personal_task_people;
--   DROP TABLE IF EXISTS personal_task_projects;
--   DROP TABLE IF EXISTS personal_project_documents;
--   DROP TABLE IF EXISTS personal_event_links;
--   DROP TABLE IF EXISTS personal_event_targets;
--   DROP TABLE IF EXISTS personal_events;
--   DROP TABLE IF EXISTS personal_notes;
--   DROP TABLE IF EXISTS personal_tasks;
--   DROP TABLE IF EXISTS personal_tags;
--   DROP TABLE IF EXISTS personal_people;
--   DROP TABLE IF EXISTS personal_projects;
--   DROP TABLE IF EXISTS personal_document_pages;
--   DROP TABLE IF EXISTS personal_source_documents;

CREATE TABLE IF NOT EXISTS `personal_source_documents` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `uuid` CHAR(36) NOT NULL DEFAULT (UUID()),
  `source_type` VARCHAR(40) NOT NULL,
  `original_filename` VARCHAR(512) NULL,
  `document_title` VARCHAR(512) NULL,
  `mime_type` VARCHAR(160) NULL,
  `storage_ref` VARCHAR(1024) NOT NULL,
  `sha256` CHAR(64) NULL,
  `received_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `document_created_at` DATETIME(6) NULL,
  `transcription` MEDIUMTEXT NULL,
  `cleaned_text` MEDIUMTEXT NULL,
  `summary` TEXT NULL,
  `processing_status` VARCHAR(40) NOT NULL DEFAULT 'received',
  `metadata_json` JSON NULL,
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_personal_source_documents_uuid` (`uuid`),
  UNIQUE KEY `uq_personal_source_documents_sha256` (`sha256`),
  KEY `idx_personal_source_documents_status_received` (`processing_status`, `received_at`),
  KEY `idx_personal_source_documents_source_type` (`source_type`),
  KEY `idx_personal_source_documents_document_created` (`document_created_at`),
  KEY `idx_personal_source_documents_title` (`document_title`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `personal_document_pages` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `uuid` CHAR(36) NOT NULL DEFAULT (UUID()),
  `document_id` BIGINT UNSIGNED NOT NULL,
  `page_number` INT UNSIGNED NOT NULL,
  `transcription` MEDIUMTEXT NULL,
  `cleaned_text` MEDIUMTEXT NULL,
  `rendered_ref` VARCHAR(1024) NULL,
  `metadata_json` JSON NULL,
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_personal_document_pages_uuid` (`uuid`),
  UNIQUE KEY `uq_personal_document_pages_doc_page` (`document_id`, `page_number`),
  KEY `idx_personal_document_pages_document` (`document_id`),
  CONSTRAINT `fk_personal_document_pages_document`
    FOREIGN KEY (`document_id`) REFERENCES `personal_source_documents` (`id`)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `personal_projects` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `uuid` CHAR(36) NOT NULL DEFAULT (UUID()),
  `name` VARCHAR(255) NOT NULL,
  `description` TEXT NULL,
  `status` VARCHAR(40) NOT NULL DEFAULT 'active',
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  `archived_at` DATETIME(6) NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_personal_projects_uuid` (`uuid`),
  KEY `idx_personal_projects_status_archived` (`status`, `archived_at`),
  KEY `idx_personal_projects_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `personal_people` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `uuid` CHAR(36) NOT NULL DEFAULT (UUID()),
  `name` VARCHAR(255) NOT NULL,
  `organisation` VARCHAR(255) NULL,
  `email` VARCHAR(320) NULL,
  `metadata_json` JSON NULL,
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_personal_people_uuid` (`uuid`),
  KEY `idx_personal_people_name` (`name`),
  KEY `idx_personal_people_email` (`email`),
  KEY `idx_personal_people_organisation` (`organisation`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `personal_tags` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `uuid` CHAR(36) NOT NULL DEFAULT (UUID()),
  `name` VARCHAR(120) NOT NULL,
  `slug` VARCHAR(140) NOT NULL,
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_personal_tags_uuid` (`uuid`),
  UNIQUE KEY `uq_personal_tags_slug` (`slug`),
  KEY `idx_personal_tags_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `personal_tasks` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `uuid` CHAR(36) NOT NULL DEFAULT (UUID()),
  `task_ref` VARCHAR(32) NOT NULL,
  `title` VARCHAR(512) NOT NULL,
  `description` MEDIUMTEXT NULL,
  `status` VARCHAR(40) NOT NULL DEFAULT 'open',
  `priority` VARCHAR(40) NULL,
  `due_at` DATETIME(6) NULL,
  `project_id` BIGINT UNSIGNED NULL,
  `source_document_id` BIGINT UNSIGNED NULL,
  `source_page_id` BIGINT UNSIGNED NULL,
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  `completed_at` DATETIME(6) NULL,
  `archived_at` DATETIME(6) NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_personal_tasks_uuid` (`uuid`),
  UNIQUE KEY `uq_personal_tasks_task_ref` (`task_ref`),
  KEY `idx_personal_tasks_status_due` (`status`, `due_at`),
  KEY `idx_personal_tasks_due_at` (`due_at`),
  KEY `idx_personal_tasks_project_status` (`project_id`, `status`, `due_at`),
  KEY `idx_personal_tasks_completed` (`completed_at`),
  KEY `idx_personal_tasks_source_document` (`source_document_id`),
  KEY `idx_personal_tasks_source_page` (`source_page_id`),
  KEY `idx_personal_tasks_title` (`title`),
  CONSTRAINT `fk_personal_tasks_project`
    FOREIGN KEY (`project_id`) REFERENCES `personal_projects` (`id`)
    ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT `fk_personal_tasks_source_document`
    FOREIGN KEY (`source_document_id`) REFERENCES `personal_source_documents` (`id`)
    ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT `fk_personal_tasks_source_page`
    FOREIGN KEY (`source_page_id`) REFERENCES `personal_document_pages` (`id`)
    ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `personal_notes` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `uuid` CHAR(36) NOT NULL DEFAULT (UUID()),
  `title` VARCHAR(512) NOT NULL,
  `body` MEDIUMTEXT NULL,
  `project_id` BIGINT UNSIGNED NULL,
  `source_document_id` BIGINT UNSIGNED NULL,
  `source_page_id` BIGINT UNSIGNED NULL,
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  `archived_at` DATETIME(6) NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_personal_notes_uuid` (`uuid`),
  KEY `idx_personal_notes_project` (`project_id`, `archived_at`),
  KEY `idx_personal_notes_source_document` (`source_document_id`),
  KEY `idx_personal_notes_source_page` (`source_page_id`),
  KEY `idx_personal_notes_title` (`title`),
  CONSTRAINT `fk_personal_notes_project`
    FOREIGN KEY (`project_id`) REFERENCES `personal_projects` (`id`)
    ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT `fk_personal_notes_source_document`
    FOREIGN KEY (`source_document_id`) REFERENCES `personal_source_documents` (`id`)
    ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT `fk_personal_notes_source_page`
    FOREIGN KEY (`source_page_id`) REFERENCES `personal_document_pages` (`id`)
    ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `personal_events` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `uuid` CHAR(36) NOT NULL DEFAULT (UUID()),
  `event_type` VARCHAR(80) NOT NULL,
  `source_document_id` BIGINT UNSIGNED NULL,
  `source_page_id` BIGINT UNSIGNED NULL,
  `target_entity_type` VARCHAR(40) NULL,
  `target_entity_id` BIGINT UNSIGNED NULL,
  `payload_json` JSON NOT NULL,
  `status` VARCHAR(40) NOT NULL DEFAULT 'proposed',
  `applied_at` DATETIME(6) NULL,
  `error_message` TEXT NULL,
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_personal_events_uuid` (`uuid`),
  KEY `idx_personal_events_type_status` (`event_type`, `status`, `created_at`),
  KEY `idx_personal_events_source_document` (`source_document_id`, `created_at`),
  KEY `idx_personal_events_source_page` (`source_page_id`, `created_at`),
  KEY `idx_personal_events_target` (`target_entity_type`, `target_entity_id`, `created_at`),
  KEY `idx_personal_events_status_created` (`status`, `created_at`),
  CONSTRAINT `fk_personal_events_source_document`
    FOREIGN KEY (`source_document_id`) REFERENCES `personal_source_documents` (`id`)
    ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT `fk_personal_events_source_page`
    FOREIGN KEY (`source_page_id`) REFERENCES `personal_document_pages` (`id`)
    ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `personal_event_targets` (
  `event_id` BIGINT UNSIGNED NOT NULL,
  `entity_type` VARCHAR(40) NOT NULL,
  `entity_id` BIGINT UNSIGNED NOT NULL,
  `relation` VARCHAR(40) NOT NULL DEFAULT 'affected',
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`event_id`, `entity_type`, `entity_id`, `relation`),
  KEY `idx_personal_event_targets_entity` (`entity_type`, `entity_id`, `event_id`),
  CONSTRAINT `fk_personal_event_targets_event`
    FOREIGN KEY (`event_id`) REFERENCES `personal_events` (`id`)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `personal_event_links` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `event_id` BIGINT UNSIGNED NOT NULL,
  `entity_type` VARCHAR(40) NOT NULL,
  `entity_id` BIGINT UNSIGNED NOT NULL,
  `link_type` VARCHAR(40) NOT NULL,
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_personal_event_links` (`event_id`, `entity_type`, `entity_id`, `link_type`),
  KEY `idx_personal_event_links_entity` (`entity_type`, `entity_id`, `link_type`, `event_id`),
  CONSTRAINT `fk_personal_event_links_event`
    FOREIGN KEY (`event_id`) REFERENCES `personal_events` (`id`)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `personal_project_documents` (
  `project_id` BIGINT UNSIGNED NOT NULL,
  `document_id` BIGINT UNSIGNED NOT NULL,
  `relevance_note` VARCHAR(512) NULL,
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`project_id`, `document_id`),
  KEY `idx_personal_project_documents_document` (`document_id`, `project_id`),
  CONSTRAINT `fk_personal_project_documents_project`
    FOREIGN KEY (`project_id`) REFERENCES `personal_projects` (`id`)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_personal_project_documents_document`
    FOREIGN KEY (`document_id`) REFERENCES `personal_source_documents` (`id`)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `personal_task_projects` (
  `task_id` BIGINT UNSIGNED NOT NULL,
  `project_id` BIGINT UNSIGNED NOT NULL,
  `is_primary` TINYINT(1) NOT NULL DEFAULT 0,
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`task_id`, `project_id`),
  KEY `idx_personal_task_projects_project` (`project_id`, `task_id`),
  CONSTRAINT `fk_personal_task_projects_task`
    FOREIGN KEY (`task_id`) REFERENCES `personal_tasks` (`id`)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_personal_task_projects_project`
    FOREIGN KEY (`project_id`) REFERENCES `personal_projects` (`id`)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `personal_task_people` (
  `task_id` BIGINT UNSIGNED NOT NULL,
  `person_id` BIGINT UNSIGNED NOT NULL,
  `role` VARCHAR(80) NULL,
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`task_id`, `person_id`),
  KEY `idx_personal_task_people_person` (`person_id`, `task_id`),
  CONSTRAINT `fk_personal_task_people_task`
    FOREIGN KEY (`task_id`) REFERENCES `personal_tasks` (`id`)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_personal_task_people_person`
    FOREIGN KEY (`person_id`) REFERENCES `personal_people` (`id`)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `personal_note_people` (
  `note_id` BIGINT UNSIGNED NOT NULL,
  `person_id` BIGINT UNSIGNED NOT NULL,
  `role` VARCHAR(80) NULL,
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`note_id`, `person_id`),
  KEY `idx_personal_note_people_person` (`person_id`, `note_id`),
  CONSTRAINT `fk_personal_note_people_note`
    FOREIGN KEY (`note_id`) REFERENCES `personal_notes` (`id`)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_personal_note_people_person`
    FOREIGN KEY (`person_id`) REFERENCES `personal_people` (`id`)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `personal_note_tasks` (
  `note_id` BIGINT UNSIGNED NOT NULL,
  `task_id` BIGINT UNSIGNED NOT NULL,
  `relation` VARCHAR(80) NULL,
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`note_id`, `task_id`),
  KEY `idx_personal_note_tasks_task` (`task_id`, `note_id`),
  CONSTRAINT `fk_personal_note_tasks_note`
    FOREIGN KEY (`note_id`) REFERENCES `personal_notes` (`id`)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_personal_note_tasks_task`
    FOREIGN KEY (`task_id`) REFERENCES `personal_tasks` (`id`)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `personal_task_tags` (
  `task_id` BIGINT UNSIGNED NOT NULL,
  `tag_id` BIGINT UNSIGNED NOT NULL,
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`task_id`, `tag_id`),
  KEY `idx_personal_task_tags_tag` (`tag_id`, `task_id`),
  CONSTRAINT `fk_personal_task_tags_task`
    FOREIGN KEY (`task_id`) REFERENCES `personal_tasks` (`id`)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_personal_task_tags_tag`
    FOREIGN KEY (`tag_id`) REFERENCES `personal_tags` (`id`)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `personal_project_tags` (
  `project_id` BIGINT UNSIGNED NOT NULL,
  `tag_id` BIGINT UNSIGNED NOT NULL,
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`project_id`, `tag_id`),
  KEY `idx_personal_project_tags_tag` (`tag_id`, `project_id`),
  CONSTRAINT `fk_personal_project_tags_project`
    FOREIGN KEY (`project_id`) REFERENCES `personal_projects` (`id`)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_personal_project_tags_tag`
    FOREIGN KEY (`tag_id`) REFERENCES `personal_tags` (`id`)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `personal_note_tags` (
  `note_id` BIGINT UNSIGNED NOT NULL,
  `tag_id` BIGINT UNSIGNED NOT NULL,
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`note_id`, `tag_id`),
  KEY `idx_personal_note_tags_tag` (`tag_id`, `note_id`),
  CONSTRAINT `fk_personal_note_tags_note`
    FOREIGN KEY (`note_id`) REFERENCES `personal_notes` (`id`)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_personal_note_tags_tag`
    FOREIGN KEY (`tag_id`) REFERENCES `personal_tags` (`id`)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
