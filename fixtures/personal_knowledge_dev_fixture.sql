-- Development-only fixture for the personal knowledge schema.
-- Do not run this against production data.
--
-- Demonstrates:
--   one source document, two pages, one project, two tasks, one note,
--   one person, and events showing task creation and completion.

START TRANSACTION;

INSERT INTO `personal_source_documents` (
  `uuid`,
  `source_type`,
  `original_filename`,
  `document_title`,
  `mime_type`,
  `storage_ref`,
  `sha256`,
  `received_at`,
  `document_created_at`,
  `transcription`,
  `cleaned_text`,
  `summary`,
  `processing_status`,
  `metadata_json`
) VALUES (
  '11111111-1111-4111-8111-111111111111',
  'remarkable',
  '2026-08-13-meeting.pdf',
  'Meeting notes 13 Aug 2026',
  'application/pdf',
  '/dev-fixtures/remarkable/2026-08-13-meeting.pdf',
  'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
  '2026-08-13 09:00:00.000000',
  '2026-08-13 08:30:00.000000',
  'Page 1: call Alex. Page 2: draft note and mark T184 complete.',
  'Call Alex about CIL appeal. Draft meeting note. Complete T184.',
  'Meeting notes containing two tasks and one note.',
  'applied',
  JSON_OBJECT('fixture', TRUE)
);
SET @doc_id = LAST_INSERT_ID();

INSERT INTO `personal_document_pages` (
  `uuid`,
  `document_id`,
  `page_number`,
  `transcription`,
  `cleaned_text`,
  `rendered_ref`,
  `metadata_json`
) VALUES
(
  '22222222-2222-4222-8222-222222222221',
  @doc_id,
  1,
  'Call Alex about CIL appeal by Friday.',
  'Call Alex about CIL appeal by Friday.',
  '/dev-fixtures/remarkable/2026-08-13-meeting/page-1.png',
  JSON_OBJECT('fixture', TRUE)
),
(
  '22222222-2222-4222-8222-222222222222',
  @doc_id,
  2,
  'Draft short note. Tick T184.',
  'Draft short note. Complete T184.',
  '/dev-fixtures/remarkable/2026-08-13-meeting/page-2.png',
  JSON_OBJECT('fixture', TRUE)
);
SET @page1_id = (SELECT `id` FROM `personal_document_pages` WHERE `document_id` = @doc_id AND `page_number` = 1);
SET @page2_id = (SELECT `id` FROM `personal_document_pages` WHERE `document_id` = @doc_id AND `page_number` = 2);

INSERT INTO `personal_projects` (
  `uuid`,
  `name`,
  `description`,
  `status`
) VALUES (
  '33333333-3333-4333-8333-333333333333',
  'CIL Appeals',
  'Development fixture project for appeal-related tasks.',
  'active'
);
SET @project_id = LAST_INSERT_ID();

INSERT INTO `personal_people` (
  `uuid`,
  `name`,
  `organisation`,
  `email`,
  `metadata_json`
) VALUES (
  '44444444-4444-4444-8444-444444444444',
  'Alex Example',
  'Example Planning',
  'alex@example.invalid',
  JSON_OBJECT('fixture', TRUE)
);
SET @person_id = LAST_INSERT_ID();

INSERT INTO `personal_tasks` (
  `uuid`,
  `task_ref`,
  `title`,
  `description`,
  `status`,
  `priority`,
  `due_at`,
  `project_id`,
  `source_document_id`,
  `source_page_id`
) VALUES
(
  '55555555-5555-4555-8555-555555555184',
  'T184',
  'Call Alex about CIL appeal',
  'Follow up on the appeal points discussed in handwritten meeting notes.',
  'completed',
  'normal',
  '2026-08-14 17:00:00.000000',
  @project_id,
  @doc_id,
  @page1_id
),
(
  '55555555-5555-4555-8555-555555555185',
  'T185',
  'Draft short meeting note',
  'Create a concise written note from the page 2 discussion.',
  'open',
  'normal',
  NULL,
  @project_id,
  @doc_id,
  @page2_id
);
SET @task184_id = (SELECT `id` FROM `personal_tasks` WHERE `task_ref` = 'T184');
SET @task185_id = (SELECT `id` FROM `personal_tasks` WHERE `task_ref` = 'T185');

UPDATE `personal_tasks`
SET `completed_at` = '2026-08-13 10:00:00.000000'
WHERE `id` = @task184_id;

INSERT INTO `personal_notes` (
  `uuid`,
  `title`,
  `body`,
  `project_id`,
  `source_document_id`,
  `source_page_id`
) VALUES (
  '66666666-6666-4666-8666-666666666666',
  'Meeting note context',
  'The handwritten note records that Alex should be contacted before drafting the short appeal note.',
  @project_id,
  @doc_id,
  @page2_id
);
SET @note_id = LAST_INSERT_ID();

INSERT INTO `personal_task_projects` (`task_id`, `project_id`, `is_primary`)
VALUES
  (@task184_id, @project_id, 1),
  (@task185_id, @project_id, 1);

INSERT INTO `personal_project_documents` (`project_id`, `document_id`, `relevance_note`)
VALUES (@project_id, @doc_id, 'Fixture source document for extracted task/note provenance.');

INSERT INTO `personal_task_people` (`task_id`, `person_id`, `role`)
VALUES (@task184_id, @person_id, 'contact');

INSERT INTO `personal_note_people` (`note_id`, `person_id`, `role`)
VALUES (@note_id, @person_id, 'mentioned');

INSERT INTO `personal_note_tasks` (`note_id`, `task_id`, `relation`)
VALUES (@note_id, @task184_id, 'context');

INSERT INTO `personal_events` (
  `uuid`,
  `event_type`,
  `source_document_id`,
  `source_page_id`,
  `target_entity_type`,
  `target_entity_id`,
  `payload_json`,
  `status`,
  `applied_at`
) VALUES
(
  '77777777-7777-4777-8777-777777777184',
  'create_task',
  @doc_id,
  @page1_id,
  'task',
  @task184_id,
  JSON_OBJECT('task_ref', 'T184', 'title', 'Call Alex about CIL appeal'),
  'applied',
  '2026-08-13 09:30:00.000000'
);
SET @event_create_task184 = LAST_INSERT_ID();

INSERT INTO `personal_event_links` (`event_id`, `entity_type`, `entity_id`, `link_type`)
VALUES (@event_create_task184, 'task', @task184_id, 'created');

INSERT INTO `personal_event_targets` (`event_id`, `entity_type`, `entity_id`, `relation`)
VALUES (@event_create_task184, 'task', @task184_id, 'created');

INSERT INTO `personal_events` (
  `uuid`,
  `event_type`,
  `source_document_id`,
  `source_page_id`,
  `target_entity_type`,
  `target_entity_id`,
  `payload_json`,
  `status`,
  `applied_at`
) VALUES
(
  '77777777-7777-4777-8777-777777777185',
  'create_task',
  @doc_id,
  @page2_id,
  'task',
  @task185_id,
  JSON_OBJECT('task_ref', 'T185', 'title', 'Draft short meeting note'),
  'applied',
  '2026-08-13 09:35:00.000000'
);
SET @event_create_task185 = LAST_INSERT_ID();

INSERT INTO `personal_event_links` (`event_id`, `entity_type`, `entity_id`, `link_type`)
VALUES (@event_create_task185, 'task', @task185_id, 'created');

INSERT INTO `personal_event_targets` (`event_id`, `entity_type`, `entity_id`, `relation`)
VALUES (@event_create_task185, 'task', @task185_id, 'created');

INSERT INTO `personal_events` (
  `uuid`,
  `event_type`,
  `source_document_id`,
  `source_page_id`,
  `target_entity_type`,
  `target_entity_id`,
  `payload_json`,
  `status`,
  `applied_at`
) VALUES
(
  '88888888-8888-4888-8888-888888888184',
  'complete_task',
  @doc_id,
  @page2_id,
  'task',
  @task184_id,
  JSON_OBJECT('task_ref', 'T184', 'evidence_text', 'Tick T184'),
  'applied',
  '2026-08-13 10:00:00.000000'
);
SET @event_complete_task184 = LAST_INSERT_ID();

INSERT INTO `personal_event_links` (`event_id`, `entity_type`, `entity_id`, `link_type`)
VALUES (@event_complete_task184, 'task', @task184_id, 'completed');

INSERT INTO `personal_event_targets` (`event_id`, `entity_type`, `entity_id`, `relation`)
VALUES (@event_complete_task184, 'task', @task184_id, 'completed');

COMMIT;
