# AGENTS.md

## Project Context
- Repository: `/opt/scraper`
- Runtime: Node.js (ES modules)
- Database: PostgreSQL (`docs_db`) with `pgvector` and PostGIS objects present.

## Deployment Topology
- `/opt/scraper` and `/var/www/html` are on the HZ server alongside PostgreSQL.
- `ngist/public_html` is on a different server and is only visible here via an SSH-mounted filesystem.
- `ngist/public_html` represents the frontend/app server environment. In normal operation, user-facing PHP pages there proxy deeper AI/document work to Otso.
- Do not assume PHP pages under `ngist/public_html` can access local HZ-server paths such as `/var/www/html/...` or local loopback-hosted endpoints as if they were on the same machine.
- When integrating `ngist/public_html` with services under `/var/www/html`, treat them as cross-server `ngist -> Otso` calls unless the user explicitly says otherwise.

Server names in current use:
- `Otso`: the Hetzner/Helsinki server hosting `/opt/scraper`, `/var/www/html`, PostgreSQL, and the deeper AI/extraction/worker services.
- `Klaus`: the Germany-based box primarily doing scraping work. Treat it as separate from normal `ngist -> Otso` document, drafting, and AI workflows unless the task is explicitly about scraping or scrape ingestion.

## Shared nGISt Job Picker
- The shared type-ahead implementation is `/mnt/ngist/public_html/partials/job_finder.js`, using `/api.php?endpoint=letters/job_number_lookup`.
- The compact header-style picker uses `data-header-job-lookup`; use this inline picker for pages that need to select a job without opening the standalone/modal `partials/job_finder.php` UI.
- Add `data-select-only="true"` when selection must stay on the current page. The shared script then emits a bubbling `header-job-lookup-selected` event with `event.detail.job`, rather than navigating to `/jobs/job.php`.
- The CIL calculator uses this select-only mode to populate its hidden file reference plus client and site fields. The shared lookup resolves a job's stored postcode through ONSPD and returns its `lad_code` / `ons_code`; CIL uses that code to select the charging authority.

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

MySQL keys used by some scripts:
- `MYSQL_HOST`
- `MYSQL_PORT`
- `MYSQL_DATABASE`
- `MYSQL_USER`
- `MYSQL_PASSWORD`

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

### `planning_statement_workspaces`
Purpose: canonical workspace record for planning statement drafting tied to a job/application.

Columns:
- `id uuid not null default gen_random_uuid()` (PK)
- `job_number text not null`
- `application_ref text not null`
- `application_id bigint`
- `synthetic_id text`
- `title text`
- `site_address text`
- `client_name text`
- `local_authority text`
- `facts_json jsonb not null default '{}'::jsonb`
- `style_guidance_json jsonb not null default '{}'::jsonb`
- `created_by integer`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

Indexes:
- `planning_statement_workspaces_job_number_idx` (btree on `job_number`)
- `planning_statement_workspaces_application_ref_idx` (btree on `application_ref`)
- `planning_statement_workspaces_application_id_idx` (btree on `application_id`)
- `planning_statement_workspaces_synthetic_id_idx` (btree on `synthetic_id`)
- `planning_statement_workspaces_facts_gin` (GIN on `facts_json`)
- `planning_statement_workspaces_style_guidance_gin` (GIN on `style_guidance_json`)

### `planning_statement_sections`
Purpose: ordered section definitions plus saved draft output and reusable section memory for a workspace.

Columns:
- `id uuid not null default gen_random_uuid()` (PK)
- `workspace_id uuid not null` (FK -> `planning_statement_workspaces.id`)
- `section_key text not null`
- `title text not null`
- `position integer not null`
- `is_selected boolean not null default true`
- `status text not null default 'not_started'`
- `draft_text text`
- `draft_summary text`
- `prompt_context jsonb not null default '{}'::jsonb`
- `generation_meta jsonb not null default '{}'::jsonb`
- `last_drafted_at timestamptz`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

Constraints:
- `planning_statement_sections_pkey` primary key (`id`)
- `planning_statement_sections_workspace_key_uq` unique (`workspace_id`, `section_key`)

Indexes:
- `planning_statement_sections_workspace_position_idx` (btree on `workspace_id`, `position`)
- `planning_statement_sections_workspace_status_idx` (btree on `workspace_id`, `status`)
- `planning_statement_sections_prompt_context_gin` (GIN on `prompt_context`)
- `planning_statement_sections_generation_meta_gin` (GIN on `generation_meta`)

### `condition_discharge_workspaces`
Purpose: persistent drafting workspaces for discharging a selected condition from a tracked planning-permission decision notice, linked to an ngist job.

Columns:
- `id uuid not null default gen_random_uuid()` (PK)
- `job_number text not null`
- `condition_tracker_id bigint not null`
- `decision_notice_doc_id uuid not null` (FK -> `documents.id`, `ON DELETE RESTRICT`)
- `selected_condition_json jsonb not null default '{}'::jsonb`
- `title text`
- `site_address text`
- `client_name text`
- `local_authority text`
- `development_description text`
- `recipient_name text`
- `recipient_address text`
- `specific_instructions text`
- `portal_submission_json jsonb not null default '{}'::jsonb` (user-entered Planning Portal submission-pack answers)
- `status text not null default 'not_started'`
- `draft_json jsonb not null default '{}'::jsonb`
- `draft_text text`
- `generation_job_id bigint`
- `generation_meta jsonb not null default '{}'::jsonb`
- `created_by integer`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

Indexes:
- `condition_discharge_workspaces_job_number_idx` (btree on `job_number`, `updated_at desc`)
- `condition_discharge_workspaces_tracker_idx` (btree on `condition_tracker_id`)
- `condition_discharge_workspaces_decision_notice_idx` (btree on `decision_notice_doc_id`)

### `condition_discharge_workspace_documents`
Purpose: durable links between a condition-discharge workspace and the job documents selected as supporting material for its letter workflow.

Columns:
- `workspace_id uuid not null` (FK -> `condition_discharge_workspaces.id`, `ON DELETE CASCADE`)
- `doc_id uuid not null` (FK -> `documents.id`, `ON DELETE CASCADE`)
- `display_title text`
- `notes text`
- `created_at timestamptz not null default now()`

### `planning_portal_agent_profiles`
Purpose: centrally managed shared Planning Portal agent identity, keyed by profile name and stored as JSON.

Columns:
- `profile_key text` (PK)
- `profile_json jsonb not null default '{}'::jsonb`
- `updated_by integer`
- `updated_at timestamptz not null default now()`

Constraints / indexes:
- PK: (`workspace_id`, `doc_id`)
- `condition_discharge_workspace_documents_doc_idx` (btree on `doc_id`)

### `planning_statement_workspace_documents`
Purpose: workspace-level attachment table linking drafting workspaces to supporting documents in `public.documents`.

Columns:
- `workspace_id uuid not null` (FK -> `planning_statement_workspaces.id`)
- `doc_id uuid not null` (FK -> `documents.id`)
- `display_title text`
- `notes text`
- `created_at timestamptz not null default now()`

Constraints:
- PK: (`workspace_id`, `doc_id`)

Indexes:
- `planning_statement_workspace_documents_doc_idx` (btree on `doc_id`)

### `planning_statement_section_documents`
Purpose: many-to-many link assigning supporting documents to specific planning statement sections.

Columns:
- `section_id uuid not null` (FK -> `planning_statement_sections.id`)
- `doc_id uuid not null` (FK -> `documents.id`)
- `relevance_note text`
- `created_at timestamptz not null default now()`

Constraints:
- PK: (`section_id`, `doc_id`)

Indexes:
- `planning_statement_section_documents_doc_idx` (btree on `doc_id`)

### `policy_document_guidance`
Purpose: per-policy-document human guidance captured before running policy-unit analysis.

Columns:
- `doc_id uuid not null` (PK, FK -> `documents.id`)
- `explanation_text text not null default ''`
- `custom_prompt_text text not null default ''`
- `test_page_start integer`
- `test_page_end integer`
- `last_test_job_id bigint`
- `last_execute_job_id bigint`
- `created_by integer`
- `updated_by integer`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

Indexes:
- `policy_document_guidance_updated_at_idx` (btree on `updated_at desc`)

### `policy_units`
Purpose: normalized structured policy blocks extracted from policy documents for cleaner retrieval than raw page chunks.

Columns:
- `id bigserial` (PK)
- `doc_id uuid not null` (FK -> `documents.id`)
- `unit_key text not null`
- `unit_type text not null`
- `section_title text`
- `heading_path_json jsonb not null default '[]'::jsonb`
- `policy_number text`
- `policy_title text`
- `policy_text text`
- `supporting_text text`
- `keywords_json jsonb not null default '[]'::jsonb`
- `topics_json jsonb not null default '[]'::jsonb`
- `page_start integer`
- `page_end integer`
- `prev_unit_id bigint null` (self-FK -> `policy_units.id`)
- `next_unit_id bigint null` (self-FK -> `policy_units.id`)
- `source_meta_json jsonb not null default '{}'::jsonb`
- `unit_vec vector(1536)`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

Constraints:
- `policy_units_pkey` primary key (`id`)
- unique (`doc_id`, `unit_key`)
- FK `doc_id` -> `documents.id` on delete cascade

Indexes:
- `policy_units_doc_id_idx` (btree on `doc_id`)
- `policy_units_unit_type_idx` (btree on `unit_type`)
- `policy_units_policy_number_idx` (btree on `policy_number`)
- `policy_units_page_start_idx` (btree on `doc_id`, `page_start`, `page_end`)
- `policy_units_heading_path_gin` (GIN on `heading_path_json`)
- `policy_units_source_meta_gin` (GIN on `source_meta_json`)
- `policy_units_unit_vec_hnsw` (HNSW on `unit_vec` cosine)

### `policy_chapters`
Purpose: top-level chapter segmentation for policy documents, separate from finer-grained `policy_units`.

Columns:
- `id bigserial` (PK)
- `doc_id uuid not null` (FK -> `documents.id`)
- `chapter_key text not null`
- `chapter_order integer not null`
- `chapter_number text`
- `chapter_title text not null`
- `chapter_text text`
- `page_start integer not null`
- `page_end integer not null`
- `heading_path_json jsonb not null default '[]'::jsonb`
- `source_meta_json jsonb not null default '{}'::jsonb`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

Constraints:
- `policy_chapters_pkey` primary key (`id`)
- unique (`doc_id`, `chapter_key`)
- FK `doc_id` -> `documents.id` on delete cascade

Indexes:
- `policy_chapters_doc_order_idx` (btree on `doc_id`, `chapter_order`)
- `policy_chapters_doc_page_idx` (btree on `doc_id`, `page_start`, `page_end`)
- `policy_chapters_heading_path_gin` (GIN on `heading_path_json`)
- `policy_chapters_source_meta_gin` (GIN on `source_meta_json`)

### `applications`
Purpose: long-term canonical storage for scraped planning applications (separate from `scrape_jobs`), with a compatibility view for legacy MySQL consumers.

Columns (selected):
- `id bigserial` (PK)
- `ons_code text not null`
- `reference text not null`
- many legacy-aligned fields (dates, status/decision, parties, etc.)
- `unified_json jsonb`, `planit_json jsonb` (raw payload retention)
- `source_url text`, `keyval text`
- `wfs_geometry jsonb` (raw WFS geometry payload, e.g. `wkt` + `bbox`)
- `wfs_geom geometry(Geometry,27700)` (native PostGIS geometry for spatial queries)
- `date_added date not null default current_date`
- `first_seen_at timestamptz not null default now()`
- `scraped_at timestamptz not null default now()`

Indexes:
- Unique `(ons_code, reference)`
- `applications_wfs_geom_gix` (GIST on `wfs_geom`)

Related objects:
- `applications_legacy` (VIEW): exposes legacy column names with spaces/case for merge/export compatibility.

### `application_relationship_families`
Purpose: canonical cached relationship-analysis result for one planning-permission family.

Columns:
- `id bigserial` (PK)
- `ons_code text not null`
- `root_reference text`
- `family_hash text not null`
- `graph_json jsonb not null`
- `relationship_json jsonb not null`
- `model text`
- `graph_version text not null`
- `prompt_version text not null`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

Indexes / constraints:
- unique (`ons_code`, `family_hash`, `graph_version`, `prompt_version`)
- btree on (`ons_code`, `root_reference`)
- GIN on `graph_json`
- GIN on `relationship_json`

### `application_relationship_family_members`
Purpose: maps each application reference in a relationship family to the cached canonical family record.

Columns:
- `family_id bigint not null` (FK -> `application_relationship_families.id`)
- `ons_code text not null`
- `reference text not null`
- `role_hint text`
- `created_at timestamptz not null default now()`

Constraints / indexes:
- PK (`family_id`, `reference`)
- unique (`ons_code`, `reference`)
- btree on (`ons_code`, `reference`)

### `wcc_retrofit_lbc_applications`
Purpose: Westminster-only cached analysis of `/LBC` applications for first-cut environmental retrofit relevance, including timing / route summary fields and document-inventory scrape status.

Columns:
- `id bigserial` (PK)
- `ons_code text not null`
- `reference text not null`
- `keyval text`
- `application_received date`
- `application_validated date`
- `decision_made_date date`
- `decision_issued_date date`
- `decision text`
- `actual_decision_level text`
- `proposal text`
- `address text`
- `application_type text`
- `source_snapshot_json jsonb not null default '{}'::jsonb`
- `prefilter_status text not null default 'new'`
- `prefilter_keywords jsonb not null default '[]'::jsonb`
- `analysis_status text not null default 'new'`
- `is_candidate boolean not null default false`
- `confidence text`
- `reasoning text`
- `retrofit_themes jsonb not null default '[]'::jsonb`
- `model text`
- `prompt_version text`
- `raw_model_json jsonb`
- `determination_days integer`
- `approval_status text`
- `decision_route text`
- `documents_url text`
- `documents_scrape_status text not null default 'not_started'`
- `documents_scraped_at timestamptz`
- `documents_error text`
- `document_count integer`
- `first_classified_at timestamptz`
- `last_classified_at timestamptz`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

Indexes / constraints:
- unique (`ons_code`, `reference`)
- btree on (`ons_code`, `is_candidate`, `analysis_status`)
- btree on (`documents_scrape_status`, `last_classified_at desc`)
- GIN on `retrofit_themes`
- GIN on `source_snapshot_json`

### `wcc_retrofit_lbc_document_inventory`
Purpose: Westminster document-tab inventory rows scraped for matched retrofit-related `/LBC` applications.

Columns:
- `id bigserial` (PK)
- `application_analysis_id bigint not null` (FK -> `wcc_retrofit_lbc_applications.id`)
- `ons_code text not null`
- `reference text not null`
- `documents_url text`
- `document_url text not null`
- `raw_href text`
- `description text`
- `document_type text`
- `published_date_raw text`
- `published_date date`
- `normalized_class text`
- `raw_source_json jsonb not null default '{}'::jsonb`
- `scraped_at timestamptz not null default now()`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

Indexes / constraints:
- unique (`reference`, `document_url`)
- btree on (`application_analysis_id`, `normalized_class`)
- btree on (`ons_code`, `reference`)
- GIN on `raw_source_json`

### `wcc_retrofit_lbc_document_pack_analysis`
Purpose: cleaned applicant-side document-pack analysis for Westminster retrofit-related `/LBC` inventories, separating substantive submission documents from procedural or council-generated material.

Columns:
- `inventory_id bigint` (PK, FK -> `wcc_retrofit_lbc_document_inventory.id`)
- `application_analysis_id bigint not null` (FK -> `wcc_retrofit_lbc_applications.id`)
- `ons_code text not null`
- `reference text not null`
- `analysis_status text not null default 'new'`
- `include_in_pack boolean`
- `exclusion_reason text`
- `normalized_pack_class text`
- `normalized_pack_label text`
- `classifier_stage text`
- `model_confidence text`
- `model_reasoning text`
- `prompt_version text`
- `model text`
- `raw_model_json jsonb`
- `analyzed_at timestamptz`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

Indexes / constraints:
- PK on `inventory_id`
- btree on (`ons_code`, `reference`)
- btree on (`analysis_status`, `include_in_pack`, `normalized_pack_class`)
- btree on (`application_analysis_id`, `normalized_pack_class`)
- GIN on `raw_model_json`

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

### LVMF 2026 PostGIS dataset
Purpose: a separate 2026 consultation-draft LVMF model. It does **not** replace the legacy MySQL `lvmf` table or the existing `ngist` LVMF viewer.

Tables:
- `lvmf_2026_datasets`, `lvmf_2026_views`, `lvmf_2026_control_points`
- `lvmf_2026_areas`, `lvmf_2026_area_vertices`, `lvmf_2026_control_lines`
- `lvmf_2026_assessment_paths`, `lvmf_2026_assessment_path_points`
- `lvmf_2026_threshold_rules`

Notes:
- Canonical protected-vista geometry is Appendix E (`ngist/public_html/lvmf/all_views.csv`) in EPSG:27700. Appendix C assessment paths are a separate display layer.
- Appendix E plan footprints use `a-c-e` for the Viewing Corridor, `a-b-c` for the left LAA, and `a-e-f` for the right LAA. Point `d` is not a corridor-boundary vertex; it remains the Appendix E control endpoint used for the separately defined A→D curvature/control-surface calculation.
- LVMF 2026 VC calculations use Appendix E points `a` and `d` with the Appendix F curvature/refraction coefficient `0.0673`; do not substitute Appendix C representative points or Appendix D photographic bearings.
- Load/reload with `/opt/scraper/scripts/import_lvmf_2026_postgis.js` after applying `/opt/scraper/migrations/2026-09-13_lvmf_2026_postgis.sql`.
- Legacy comparison objects are `lvmf_legacy_snapshots`, `lvmf_legacy_comparison_matches`, `lvmf_legacy_comparison_areas`, and `lvmf_legacy_comparison_results`. They snapshot the old MySQL viewer and must retain its literal A→B/legacy special-case behaviour; refresh with `scripts/compare_lvmf_legacy.js`.
- Materialised change-layer objects are `lvmf_legacy_comparison_change_sets`, `lvmf_legacy_comparison_changes`, and `lvmf_legacy_comparison_height_changes`. They are derived only: red `newly_protected` is Appendix E minus the legacy constraint union, green `released` is the reverse (including legacy-only `2B.1`), and yellow `changed` is shared land without a like-for-like area category. Rebuild after refreshing the comparison with `scripts/materialize_lvmf_legacy_change_layers.js`; the nGISt comparison map reads the saved layer through `lvmf_legacy_changes.php`.
- The legacy MySQL `lvmf` fixed-column table remains compatibility-only for the existing viewer. The normalised 2012 corridor source is held in MySQL `lvmf_2012_geometry_datasets`, `lvmf_2012_corridors`, and `lvmf_2012_corridor_control_points`; load it from `LVMF_2012_viewing_corridor_geometry.csv` using `/opt/scraper/scripts/import_lvmf_2012_geometry_mysql.js` after applying `2026-09-13_lvmf_2012_normalized_geometry_mysql.sql`. Calculation definitions are corridor-specific JSON: ordinary corridors retain A→B curvature/refraction, while 5A.2 VC1 is A→M and VC2 is a vertex control surface (`n,c,d,o`) with defining point `b`. Background-area comparisons use the 2012 5A.2 BCSA/BWSCA source controls, not a 2016 dataset.
- There is no separate 2016 LVMF geometry dataset. The legacy comparison baseline is the 2012 `5A.2` Greenwich Park view and its Background Setting/Assessment Area (`v-y-z-w`); use the same sampled coordinates from that area when comparing against the 2026 `5B` BAA. Do not describe the legacy baseline as 2016 geometry.

## MySQL Schema Notes

### `sm_update_cleaned`
Purpose: dashboard Standard Method targets, London Plan targets, and historic delivery averages, keyed by `ONS Code`.

- `londonPlanTarget` is the existing annual 2021 London Plan target.
- `londonPlan2026Target` is a nullable unsigned integer storing the **ten-year total** from Draft London Plan 2026 Table 3.1 for 2027/28–2036/37. Divide by 10 for the annual chart comparison; it is not a phased delivery trajectory.
- Source extract: `/opt/scraper/data/london_plan/2026_table_3_1_targets.json`; migration: `migrations/2026-10-03_london_plan_2026_targets_mysql.sql`; validated importer: `scripts/import_london_plan_2026_targets_mysql.js` (dry run by default; `--apply` writes).
- All 33 borough rows are populated. OPDC's 11,335 ten-year target is retained in the extract but has no matching borough ONS row in this table.

### `lpa_codes`
Purpose: canonical lookup table for LPAs used by scraper/runtime integration and external dataset matching.

Selected columns:
- `mhclg_code varchar(10)`
- `ons_code varchar(10)`
- `lpa_name varchar(255)`
- `full_name varchar(255)`
- `short_ref tinytext`
- `local_planning_authority varchar(10)` (`E600...` planning.data.gov.uk LPA code where present)
- `organisation_entity int`
- `planit_area_id int`
- `pld_name varchar(100)`
- `planit_area varchar(100)`
- `datastore_id tinyint unsigned null`
- `postal_address varchar(500) null` (single CSV-style correspondence address, including organisation name)

Notes:
- `datastore_id` maps London Datastore ArcGIS borough services `planning_local_plan_data_XX` to `lpa_codes` rows.
- Current mapping is populated for London borough ONS codes `E09000001` through `E09000033`, corresponding to service ids `1` through `33`.
- `LLDC` and `OPDC` do not currently have matching rows in `lpa_codes`, so no `datastore_id` values are stored for service ids `34` or `35`.
- `postal_address` is a maintained correspondence-address cache. Use it before a web lookup when an LPA has been resolved; for example Westminster is stored as `Westminster City Council, Westminster City Hall, 64 Victoria Street, London, SW1E 6QP`.

### `mbul_datasets`, `mbul_layers`, `mbul_features`
Purpose: MySQL-backed map store for London Datastore “London Plan evidence - MBUL” GeoPackage layers used by `ngist/public_html/maps/mbul_map.php`.

Notes:
- Raw `.gpkg` files are stored privately under `ngist/private/data/mbul_gpkg/`.
- Import/re-import is handled by `/opt/scraper/scripts/import_mbul_gpkg_to_mysql.js`.
- `mbul_features.geom` is stored as SRID 4326 geometry with a spatial index, while original non-geometry attributes are retained in `properties` JSON.
- Use bbox-constrained reads through `ngist/public_html/maps/mbul_geo.php`; do not load whole large MBUL layers into the browser by default.


### `app_ingest_jobs`
Purpose: MySQL-backed background job tracking used by `ngist` upload, extraction, and worker flows.

Selected columns:
- `id` (PK)
- `user_id int null`
- `kind`
- `stage`
- `status`
- `percentage_complete`
- `current_action`
- `final_output`
- `error_message`
- `created_at`
- `updated_at`

Notes:
- `user_id` was added on March 12, 2026 so jobs can be queried per logged-in user.
- New job creation helpers in `ngist/public_html/job_logging_library.php` now populate `user_id` from the session where available.

### `cil_estimates`
Purpose: MySQL-backed saved CIL-calculator estimates for the `ngist` application.

Selected columns:
- `id` (PK), `user_id`, `file_number`, `estimate_name`, `cil_json`
- `created_at`, `updated_at`

Notes:
- `cil_json` is the canonical saved calculator payload. The estimates list derives job/site/authority, the reported CIL total and warning badges from it.
- `updated_at` was added on September 6, 2026 and is maintained by MySQL whenever an estimate is changed.

### `personal_*` personal knowledge tables
Purpose: MySQL/InnoDB schema for the personal handwritten-notes system. It is deliberately only the durable data model for now; it does not imply reMarkable ingestion, email handling, OpenAI calls, PDF generation, cron jobs, or UI automation.

Design principle:
- `capture -> interpret -> events -> structured state`
- Original source documents remain traceable through document/page provenance and event history.

Documentation:
- Full schema notes live in `/opt/scraper/docs/personal_knowledge_schema.md`.
- Migration: `/opt/scraper/migrations/2026-08-13_personal_knowledge_system_mysql.sql`.
- Development-only fixture: `/opt/scraper/fixtures/personal_knowledge_dev_fixture.sql`.
- Rollback-only verifier: `/opt/scraper/scripts/verify_personal_knowledge_schema.js`.

Core tables:
- `personal_source_documents`: every retained source document, with `source_type`, filename/title, MIME type, `storage_ref`, SHA-256 dedupe hash, received/document-created timestamps, transcription/cleaned text/summary, processing status, and metadata JSON. Do not store PDF binaries in MySQL.
- `personal_document_pages`: page-level records for source documents, including page transcription/cleaned text and optional rendered image/file reference.
- `personal_projects`: projects/areas with status and soft archive.
- `personal_tasks`: structured tasks with internal integer `id`, external `uuid`, separate human-visible `task_ref` such as `T184`, status/priority/due fields, optional primary project, and source document/page pointers.
- `personal_notes`: non-task notes with optional project and source document/page pointers.
- `personal_people`: lightweight people records with name, organisation, optional email, and metadata JSON.
- `personal_tags`: lightweight tags.
- `personal_events`: interpreted actions/audit log, e.g. `create_task`, `update_task`, `complete_task`, `cancel_task`, `create_note`, `update_note`, `assign_project`, `associate_person`.
- `personal_event_targets`: additional entities affected by an event.
- `personal_event_links`: semantic event/entity links such as `created`, `updated`, `completed`, or `associated`.

Relationship tables:
- `personal_project_documents`
- `personal_task_projects`
- `personal_task_people`
- `personal_note_people`
- `personal_note_tasks`
- `personal_task_tags`
- `personal_project_tags`
- `personal_note_tags`

Status conventions:
- Task statuses are `VARCHAR`; current expected values are `open`, `completed`, `waiting`, `cancelled`.
- Event statuses are `VARCHAR`; current expected values are `proposed`, `applied`, `rejected`, `failed`.
- Source document processing statuses are `VARCHAR`; current expected values include `received`, `transcribed`, `interpreted`, `applied`, `failed`.

Provenance notes:
- Prefer recording both direct provenance (`source_document_id`, `source_page_id`) and event provenance (`personal_events`, `personal_event_links`) when applying interpreted actions.
- Example chain: task `T184` -> `personal_event_links` / `personal_events` -> `personal_document_pages` -> `personal_source_documents.storage_ref`.
- Keep `task_ref` stable and never reuse it; later handwritten updates may refer to it directly.
- Normal lifecycle should use statuses and `archived_at`, not destructive deletes.

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

## Operational Reminders
- Hetzner `php8.3-fpm` previously failed with `Result: oom-kill` and stayed down until manually restarted.
- The live unit is now hardened with `OOMPolicy=continue` plus restart settings (`Restart=always`, `RestartSec=5s`, `StartLimitBurst=20`).
- If PHP-FPM OOM behavior is revisited, check `systemctl show php8.3-fpm -p OOMPolicy -p Restart -p RestartSec -p StartLimitBurst` and `systemctl cat php8.3-fpm` before assuming the hardening is missing.

## Operational Guidance For Agents
- Treat `documents` as parent entity for `chunks` and `llm_outputs`.
- Preserve idempotency semantics around `scrape_jobs.idempotency_key`.
- For `ngist` MySQL planning records (`planit_applications` / `app_combined_nmrk_planit`): treat `status` as decision/outcome text and `app_state` as workflow/state text.
- For document extraction, drafting, conditions, and similar AI/document workflows, assume the path is `browser -> ngist -> Otso`. Do not attribute those flows to `Klaus` unless the issue is explicitly scrape-pipeline related.
- For report-style outputs that already exist in Markdown and need to be turned into a Newmark-style Word note, reuse `/opt/scraper/scripts/export_markdown_note_docx.js`. It converts Markdown into the existing `note_template.docx` render pipeline via `/opt/scraper/workers/document_render_word.php`.
- Westminster Detailed Stats dashboard caches are intentionally Westminster-only. The `query_cache` keys used by `ngist/public_html/wcc/wcc_stats.php` should be generated from `public.applications` with `ons_code = 'E09000033'`, not from all LPAs.
- For direct MySQL checks against the legacy `ngist` app DB, do not rely on `ngist/public_html/connect.php` if you only need MySQL. That bootstrap also opens Postgres and may fail before MySQL is available. Prefer a minimal PHP snippet that loads `/opt/scraper/ngist/.env`, reads `MYSQL_HOST` / `MYSQL_DB` or `MYSQL_DATABASE` / `MYSQL_USER` / `MYSQL_PASS` or `MYSQL_PASSWORD`, and then creates a standalone MySQL PDO connection.
- For direct DB checks/queries (Postgres/MySQL), proceed when needed; if sandbox networking blocks access, rerun outside sandbox via escalation and request user consent.
- For vector search changes, keep `vector(1536)` dimension unchanged unless a coordinated embedding migration is planned.
- Avoid schema mutations without explicit migration scripts and rollback notes.
- Remind user to update the AGENTS.md if and whenever the database schema is changed.

## Communication Preference
- User prefers a friendly, warm tone while keeping technical guidance concise and practical.
- Avoid stiff or overly blunt replies when a lighter, more personable phrasing will do.
