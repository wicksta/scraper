# Personal Knowledge Schema

This schema is the durable database layer for a future personal system that turns source documents, including handwritten reMarkable notes, into structured tasks, notes, projects, people, tags, and event history.

The architectural rule is: capture source documents, interpret them into proposed events, apply approved events to structured state, and retain provenance throughout.

## Tables

`personal_source_documents` stores every received source document. It keeps source type, original filename, title, MIME type, storage reference/path, SHA-256 hash, received/document-created timestamps, transcription, cleaned text, summary, processing status, and flexible metadata. Large binaries are not stored in MySQL.

`personal_document_pages` stores first-class page records for source documents. Tasks, notes, and events can point to a page as well as the parent document.

`personal_projects` stores projects or areas. Projects are archived with `archived_at`; they should not be destructively deleted in normal use.

`personal_tasks` stores structured tasks. It uses an internal integer `id`, an external `uuid`, and a separate human-visible `task_ref` such as `T184`. `source_document_id` and `source_page_id` are direct provenance shortcuts for common queries.

`personal_notes` stores extracted or manually created notes that are not tasks. Notes can belong to a project and can trace back to a source document/page.

`personal_people` stores lightweight person references: name, organisation, optional email, and flexible metadata.

`personal_tags` stores lightweight tags. Tags are attached through join tables rather than a taxonomy model.

`personal_events` stores interpreted actions such as `create_task`, `update_task`, `complete_task`, `cancel_task`, `create_note`, `update_note`, `assign_project`, and `associate_person`. GPT or later interpretation code should produce events rather than directly editing arbitrary state.

`personal_event_targets` stores additional entities affected by an event. `personal_events.target_entity_type` and `target_entity_id` remain as the primary target for simple queries.

`personal_event_links` links events to entities with a semantic `link_type`, for example `created`, `completed`, `updated`, or `associated`.

Relationship tables are deliberately explicit:

- `personal_project_documents`
- `personal_task_projects`
- `personal_task_people`
- `personal_note_people`
- `personal_note_tasks`
- `personal_task_tags`
- `personal_project_tags`
- `personal_note_tags`

## UUIDs, IDs, and Task References

Internal joins use `BIGINT UNSIGNED` primary keys for compact indexes.

Every durable entity has a `uuid CHAR(36)` for stable external references. Application code can provide UUIDs explicitly; otherwise MySQL generates them with `UUID()`.

Tasks also have `task_ref`, a stable human-visible reference intended for handwritten updates such as `T184`. This value is unique and must not be reused. The schema enforces uniqueness but does not prescribe the allocator; application code should allocate references transactionally.

## Status Conventions

Status columns use `VARCHAR`, not `ENUM`, so workflows can evolve without type migrations.

Initial task statuses:

- `open`
- `completed`
- `waiting`
- `cancelled`

Initial event statuses:

- `proposed`
- `applied`
- `rejected`
- `failed`

Initial document processing statuses:

- `received`
- `transcribed`
- `interpreted`
- `applied`
- `failed`

## Source Document Lifecycle

1. A document is captured and stored outside MySQL.
2. A row is inserted into `personal_source_documents` with `storage_ref`, `source_type`, filename/title, MIME type, and `sha256` where available.
3. Page rows are inserted into `personal_document_pages` when page boundaries are known.
4. Transcription and cleaned text are added at document and/or page level.
5. Interpretation produces rows in `personal_events` with `status = 'proposed'`.
6. Applied events update structured tables and move to `status = 'applied'`.

The unique SHA-256 key supports deduplication. MySQL permits multiple `NULL` hashes, so documents without a hash can still be recorded.

## Event Lifecycle

An event starts as a proposed interpreted action with a JSON payload and source document/page pointers where known.

When the event is accepted and applied, application code updates the target structured table, records any event target/link rows, sets `status = 'applied'`, and sets `applied_at`.

If the event is rejected or cannot be applied, it remains available for audit with `status = 'rejected'` or `status = 'failed'` plus `error_message`.

## Provenance Example

For task `T184`, provenance is reconstructed by:

1. Read `personal_tasks.task_ref = 'T184'`.
2. Follow `source_page_id` to `personal_document_pages`.
3. Follow `document_id` or `source_document_id` to `personal_source_documents`.
4. Use `storage_ref` to locate the original file.
5. Read `personal_event_links` or `personal_events.target_entity_type = 'task'` and `target_entity_id = personal_tasks.id` to see which events created, updated, or completed the task.

This preserves both a fast direct provenance path and a full event/audit path.

## Timestamps and Deletion

Tables use `created_at` and `updated_at` consistently where records are mutable. Events are append-style and only have `created_at` plus `applied_at`.

User-generated structured state should be archived with `archived_at` rather than deleted. Relationship rows may cascade when their parent structured entity is intentionally removed, but ordinary product behaviour should prefer archival.

## Common Query Shapes

Open tasks: filter `personal_tasks.status = 'open'` and `archived_at IS NULL`.

Today and next-seven-day tasks: filter `due_at` ranges with `status IN ('open', 'waiting')`.

Project tasks: use `personal_tasks.project_id` for the primary project or `personal_task_projects` for all project links.

Source provenance: filter tasks, notes, and events by `source_document_id` or `source_page_id`.

People views: use `personal_task_people` and `personal_note_people`.

Event history for a task: query `personal_events` by primary target, or `personal_event_links` / `personal_event_targets` by `entity_type = 'task'` and `entity_id`.

## Relationship Map

Source capture:

```text
personal_source_documents
  -> personal_document_pages
```

Structured state:

```text
personal_projects
  -> personal_tasks.project_id
  -> personal_notes.project_id

personal_tasks
  -> personal_source_documents via source_document_id
  -> personal_document_pages via source_page_id

personal_notes
  -> personal_source_documents via source_document_id
  -> personal_document_pages via source_page_id
```

Many-to-many relationships:

```text
personal_tasks    <-> personal_projects via personal_task_projects
personal_projects <-> source documents   via personal_project_documents
personal_tasks    <-> personal_people    via personal_task_people
personal_notes    <-> personal_people    via personal_note_people
personal_notes    <-> personal_tasks     via personal_note_tasks
personal_tasks    <-> personal_tags      via personal_task_tags
personal_projects <-> personal_tags      via personal_project_tags
personal_notes    <-> personal_tags      via personal_note_tags
```

Event/audit relationships:

```text
personal_events
  -> source document/page that caused the action
  -> primary target via target_entity_type + target_entity_id
  -> additional targets via personal_event_targets
  -> semantic links via personal_event_links
```

Entity types stored in `target_entity_type`, `personal_event_targets.entity_type`, and `personal_event_links.entity_type` should use short lowercase names such as `task`, `note`, `project`, `person`, `source_document`, and `document_page`.

## Example Provenance Query

```sql
SELECT
  t.task_ref,
  t.title,
  e.event_type,
  e.status AS event_status,
  p.page_number,
  d.original_filename,
  d.storage_ref
FROM personal_tasks t
LEFT JOIN personal_event_links el
  ON el.entity_type = 'task'
 AND el.entity_id = t.id
LEFT JOIN personal_events e ON e.id = el.event_id
LEFT JOIN personal_document_pages p ON p.id = t.source_page_id
LEFT JOIN personal_source_documents d ON d.id = COALESCE(t.source_document_id, p.document_id)
WHERE t.task_ref = 'T184'
ORDER BY e.created_at;
```

## Operational Files

Migration:

```text
/opt/scraper/migrations/2026-08-13_personal_knowledge_system_mysql.sql
```

Development-only fixture:

```text
/opt/scraper/fixtures/personal_knowledge_dev_fixture.sql
```

Rollback-only verifier:

```bash
node scripts/verify_personal_knowledge_schema.js
```

The verifier inserts the development fixture inside a transaction, checks foreign keys and provenance queries, and rolls the fixture rows back.
