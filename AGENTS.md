# AGENTS.md

## Project Context
- Repository: `/opt/scraper`
- Runtime: Node.js (ES modules)
- Database: PostgreSQL (`docs_db`) with `pgvector` and PostGIS objects present.

## Environment Variables
Use `.env` (loaded by `bootstrap.js`) for DB connectivity.

Required keys:
- `PGHOST`
- `PGPORT`
- `PGDATABASE`
- `PGUSER`
- `PGPASSWORD`

Optional alternative:
- `DATABASE_URL`

Rules:
- Never hardcode credentials.
- Prefer `DATABASE_URL` when set, otherwise build config from `PG*` vars.

## Database Schema (public)

### `documents`
Purpose: canonical document record plus search/vector metadata.

Columns:
- `id uuid not null default gen_random_uuid()` (PK)
- `source_file text`
- `sha256 text`
- `bytes bigint`
- `mime_type text`
- `pages integer`
- `title text`
- `application_ref text`
- `document_type text`
- `local_authority text`
- `originator text`
- `document_date date`
- `meta jsonb`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`
- `full_text text`
- `doc_vec vector(1536)`
- `token_count integer`
- `provenance jsonb not null default '[]'::jsonb`
- `fts tsvector`
- `lpa_code text`
- `site_point geometry(Point,4326)`
- `original_filename text`

Indexes:
- `documents_pkey` (unique btree on `id`)
- `documents_sha256_uq` (unique btree on `sha256` where not null)
- `documents_sha256_idx` (btree on `sha256`)
- `documents_docdate_idx` (btree on `document_date`)
- `documents_fts_gin` (GIN on `fts`)
- `documents_full_text_trgm` (GIN trigram on `full_text`)
- `documents_meta_gin` (GIN on `meta`)
- `documents_provenance_gin` (GIN `jsonb_path_ops` on `provenance`)
- `idx_documents_doc_vec` (IVFFlat on `doc_vec` with cosine ops)

### `chunks`
Purpose: chunked document text + embeddings.

Columns:
- `id bigint not null default nextval('chunks_id_seq')` (PK)
- `doc_id uuid not null` (FK -> `documents.id`)
- `kind text not null`
- `page integer`
- `position_json jsonb`
- `summary text`
- `text text not null`
- `embedding vector(1536)`
- `natural_key text`

Constraints:
- `chunks_pkey` primary key (`id`)
- `chunks_doc_id_fkey` foreign key (`doc_id`) references `documents(id)` on delete cascade
- `chunks_doc_id_natural_key_key` unique (`doc_id`, `natural_key`)
- `chunks_doc_key_unique` unique (`doc_id`, `natural_key`)

Indexes:
- `chunks_docid_idx` (btree on `doc_id`)
- `chunks_doc_kind_page` (btree on `doc_id`, `kind`, `page`)
- `chunks_kind_idx` (btree on `kind`)
- `chunks_page_idx` (btree on `page`)
- `chunks_embedding_hnsw` (HNSW on `embedding` cosine)
- `chunks_embedding_ivfflat` (IVFFlat on `embedding` cosine)

Note:
- Two unique constraints exist for the same key pair (`doc_id`, `natural_key`). Keep in mind during migrations/cleanup.

### `llm_outputs`
Purpose: per-document LLM result payloads by endpoint.

Columns:
- `doc_id uuid not null` (FK -> `documents.id`)
- `endpoint text not null`
- `json_output jsonb not null`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`
- `job_id integer`

Constraints:
- PK: (`doc_id`, `endpoint`)
- FK: `doc_id` -> `documents.id` on delete cascade

Indexes:
- `llm_outputs_pkey` (unique btree on `doc_id`, `endpoint`)
- `llm_outputs_endpoint_idx` (btree on `endpoint`)
- `idx_llm_outputs_job_id` (btree on `job_id`)

### `scrape_jobs`
Purpose: queue/work tracking for scraper jobs.

Columns:
- `id bigint not null default nextval('scrape_jobs_id_seq')` (PK)
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`
- `source text not null default 'ngist'`
- `requested_by text`
- `job_type text not null`
- `ons_code text`
- `application_ref text`
- `params jsonb not null`
- `mapper text`
- `status text not null default 'queued'`
- `locked_at timestamptz`
- `locked_by text`
- `attempts integer not null default 0`
- `max_attempts integer not null default 3`
- `result jsonb`
- `error text`
- `logs text`
- `idempotency_key text`

Constraints:
- `scrape_jobs_pkey` primary key (`id`)
- `scrape_jobs_idempotency_key_key` unique (`idempotency_key`)

Indexes:
- `scrape_jobs_created_at_idx` (btree on `created_at`)
- `scrape_jobs_status_idx` (btree on `status`)
- `scrape_jobs_ons_code_idx` (btree on `ons_code`)
- `scrape_jobs_status_created_idx` (btree on `status`, `created_at`)

### `lpa_scrape_configs`
Purpose: per-LPA scraper configuration resolved by worker at runtime.

Columns:
- `ons_code text not null` (PK)
- `site_url text not null`
- `scraper_entrypoint text not null` (repo-relative path, e.g. `scraper.cjs`)
- `mapper_path text not null` (repo-relative path under `mappers/`, `.cjs`)
- `enabled boolean not null default true`
- `notes text`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

Indexes:
- `lpa_scrape_configs_enabled_idx` (btree on `enabled`)

### `extract_jobs`
Purpose: extraction/OCR pipeline job tracking.

Columns:
- `id bigint not null default nextval('extract_jobs_id_seq')` (PK)
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`
- `status text not null`
- `progress integer not null default 0`
- `source_mode text`
- `source_info jsonb`
- `pdf_sha256 text`
- `pdf_bytes bigint`
- `pdf_path text`
- `text_result text`
- `ocr_used boolean`
- `ocr_pages integer[]`
- `error_message text`

Constraints:
- `extract_jobs_pkey` primary key (`id`)

### `applications`
Purpose: long-term canonical storage for scraped planning applications (separate from `scrape_jobs`), with a compatibility view for legacy MySQL consumers.

Columns (selected):
- `id bigserial` (PK)
- `ons_code text not null`
- `reference text not null`
- many legacy-aligned fields (dates, status/decision, parties, etc.)
- `unified_json jsonb`, `planit_json jsonb` (raw payload retention)
- `source_url text`, `keyval text`
- `date_added date not null default current_date`
- `first_seen_at timestamptz not null default now()`
- `scraped_at timestamptz not null default now()`

Indexes:
- Unique `(ons_code, reference)`

Related objects:
- `applications_legacy` (VIEW): exposes legacy column names with spaces/case for merge/export compatibility.

### `application_backfill_state`
Purpose: per-ONS cursor for week-by-week backfill discovery.

Columns:
- `ons_code text` (PK)
- `mode text` (`backfill` | `steady_state`)
- `cursor_end date`
- `cutoff_date date`
- `updated_at timestamptz`

### `application_discovery_runs`
Purpose: audit log for validated-range discovery windows and outcomes.

Columns:
- `id bigserial` (PK)
- `ons_code text`
- `window_start date`, `window_end date`
- `status text`, `n_refs integer`, `error text`
- `created_at`, `updated_at` (timestamptz)

### `application_ingest_log`
Purpose: idempotency + error log for materializing `scrape_jobs` into `applications`.

Columns:
- `scrape_job_id bigint` (PK)
- `ons_code text`
- `application_ref text`
- `ingested_at timestamptz`
- `error text`

### PostGIS metadata/view objects
Objects present in `public`:
- `geometry_columns` (view)
- `geography_columns` (view)
- `spatial_ref_sys` (table, PK `srid`)

## Worker/Queue Notes
- `worker_listen.js` subscribes to PostgreSQL `LISTEN` channel `scrape_job_created`.
- Notify payload includes: `job_id`, `job_type`, `status`, `ons_code`, `application_ref`.
- Worker claims jobs atomically with `FOR UPDATE SKIP LOCKED`.
- Worker only claims queued jobs where `ons_code` and `application_ref` are both present.
- Worker resolves `site_url`/`scraper_entrypoint`/`mapper_path` from `lpa_scrape_configs`.
- Worker rejects absolute paths, parent traversal, and out-of-repo paths.
- Worker currently allowlists scraper entrypoints to `scraper.cjs` and `scraper_camden_socrata.cjs`.
- Worker currently requires mapper paths to be `mappers/*.cjs`.
- Import `./bootstrap.js` first in scripts that require `.env` variables.
- If connection drops, listener reconnect logic is required because `LISTEN` state is per-connection.

## Operational Guidance For Agents
- Treat `documents` as parent entity for `chunks` and `llm_outputs`.
- Preserve idempotency semantics around `scrape_jobs.idempotency_key`.
- For vector search changes, keep `vector(1536)` dimension unchanged unless a coordinated embedding migration is planned.
- Avoid schema mutations without explicit migration scripts and rollback notes.
- Remind user to update the AGENTS.md if and whenever the database schema is changed.
