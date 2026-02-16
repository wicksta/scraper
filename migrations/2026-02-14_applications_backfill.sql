-- Applications long-term storage + legacy compatibility view + backfill bookkeeping.
-- Safe to run multiple times.

BEGIN;

-- Canonical long-term application record (Postgres-native).
CREATE TABLE IF NOT EXISTS public.applications (
  id bigserial PRIMARY KEY,
  ons_code text NOT NULL,
  reference text NOT NULL,

  alternative_reference text,
  district_reference text,

  application_received date,
  application_validated date,

  address text,
  proposal text,

  status text,
  decision text,
  appeal_status text,
  appeal_decision text,

  application_type text,
  expected_decision_level text,
  actual_decision_level text,

  case_officer text,
  parish text,
  ward text,
  amenity_society text,

  applicant_name text,
  applicant_address text,

  agent_name text,
  agent_company_name text,
  agent_address text,

  environmental_assessment_requested text,

  -- Legacy date fields (nullable; Idox coverage varies).
  actual_committee_date date,
  agreed_expiry_date date,
  last_advertised_in_press_date date,
  latest_advertisement_expiry_date date,
  last_site_notice_posted_date date,
  latest_site_notice_expiry_date date,
  decision_made_date date,
  decision_issued_date date,
  target_date date,
  temporary_permission_expiry_date date,

  lat numeric(12, 8),
  lon numeric(12, 8),

  date_added date NOT NULL DEFAULT current_date,
  major text,
  spare2 smallint,
  last_look date,
  removed date,

  keyval text,

  source_url text,
  unified_json jsonb,
  planit_json jsonb,
  scrape_job_id bigint,

  first_seen_at timestamptz NOT NULL DEFAULT now(),
  scraped_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS applications_ons_ref_uq
  ON public.applications (ons_code, reference);

CREATE INDEX IF NOT EXISTS applications_validated_idx
  ON public.applications (application_validated);

CREATE INDEX IF NOT EXISTS applications_received_idx
  ON public.applications (application_received);

-- Bookkeeping: what validated-date window to discover next.
CREATE TABLE IF NOT EXISTS public.application_backfill_state (
  ons_code text PRIMARY KEY,
  mode text NOT NULL DEFAULT 'backfill', -- 'backfill' | 'steady_state'
  cursor_end date NOT NULL,
  cutoff_date date NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- Bookkeeping: record each discovery run (window -> refs).
CREATE TABLE IF NOT EXISTS public.application_discovery_runs (
  id bigserial PRIMARY KEY,
  ons_code text NOT NULL,
  window_start date NOT NULL,
  window_end date NOT NULL,
  status text NOT NULL DEFAULT 'running', -- 'running' | 'completed' | 'failed'
  n_refs integer NOT NULL DEFAULT 0,
  error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS application_discovery_runs_ons_created_idx
  ON public.application_discovery_runs (ons_code, created_at);

-- Bookkeeping: ensure materializer is idempotent.
CREATE TABLE IF NOT EXISTS public.application_ingest_log (
  scrape_job_id bigint PRIMARY KEY,
  ons_code text,
  application_ref text,
  ingested_at timestamptz NOT NULL DEFAULT now(),
  error text
);

-- Compatibility view: expose the legacy MySQL applications schema (column names + rough types).
-- Note: Postgres allows spaces/case in column names when quoted.
CREATE OR REPLACE VIEW public.applications_legacy AS
SELECT
  a.reference::varchar(15) AS "Reference",
  COALESCE(a.alternative_reference, '')::varchar(20) AS "Alternative Reference",
  COALESCE(a.application_received, a.application_validated, a.date_added) AS "Application Received",
  COALESCE(a.address, '')::varchar(500) AS "Address",
  COALESCE(a.proposal, '')::varchar(2000) AS "Proposal",
  COALESCE(a.status, '')::varchar(30) AS "Status",
  COALESCE(a.decision, '')::varchar(30) AS "Decision",
  COALESCE(a.appeal_status, '')::varchar(30) AS "Appeal Status",
  COALESCE(a.appeal_decision, '')::varchar(30) AS "Appeal Decision",
  COALESCE(a.application_type, '')::varchar(30) AS "Application Type",
  COALESCE(a.expected_decision_level, '')::varchar(30) AS "Expected Decision Level",
  COALESCE(a.case_officer, '')::varchar(50) AS "Case Officer",
  COALESCE(a.parish, '')::varchar(50) AS "Parish",
  COALESCE(a.ward, '')::varchar(50) AS "Ward",
  COALESCE(a.amenity_society, '')::varchar(50) AS "Amenity Society",
  COALESCE(a.district_reference, '')::varchar(30) AS "District Reference",
  COALESCE(a.applicant_name, '')::varchar(100) AS "Applicant Name",
  COALESCE(a.agent_name, '')::varchar(50) AS "Agent Name",
  COALESCE(a.agent_company_name, '')::varchar(50) AS "Agent Company Name",
  COALESCE(a.agent_address, '')::varchar(200) AS "Agent Address",
  COALESCE(a.environmental_assessment_requested, '')::varchar(50) AS "Environmental Assessment Requested",

  -- Legacy duplicates: keep both column names mapping to same underlying fields.
  COALESCE(a.application_received, a.application_validated, a.date_added) AS "Application Received Date",
  COALESCE(a.application_validated, a.application_received, a.date_added) AS "Application Validated",
  COALESCE(a.application_validated, a.application_received, a.date_added) AS "Application Validated Date",

  a.actual_committee_date AS "Actual Committee Date",
  a.agreed_expiry_date AS "Agreed Expiry Date",
  a.last_advertised_in_press_date AS "Last Advertised In Press Date",
  a.latest_advertisement_expiry_date AS "Latest Advertisement Expiry Date",
  a.last_site_notice_posted_date AS "Last Site Notice Posted Date",
  a.latest_site_notice_expiry_date AS "Latest Site Notice Expiry Date",
  a.decision_made_date AS "Decision Made Date",
  a.decision_issued_date AS "Decision Issued Date",
  a.target_date AS "Target Date",
  COALESCE(a.actual_decision_level, '')::varchar(50) AS "Actual Decision Level",

  COALESCE(a.lat, 0)::numeric(12, 8) AS "lat",
  COALESCE(a.lon, 0)::numeric(12, 8) AS "lon",
  a.date_added AS "date_added",
  COALESCE(a.major, '')::varchar(50) AS "Major",
  COALESCE(a.spare2, 0)::smallint AS "spare2",
  a.temporary_permission_expiry_date AS "Temporary Permission Expiry Date",
  COALESCE(a.applicant_address, '')::varchar(200) AS "Applicant Address",
  a.last_look AS "last_look",
  a.removed AS "Removed",
  a.keyval AS "keyval"
FROM public.applications a;

-- Ensure the worker LISTEN gets notified on inserts into scrape_jobs.
-- The notify function is created in 2026-02-14_scrape_config_and_jobs.sql.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'public' AND p.proname = 'notify_scrape_job_created'
  ) THEN
    IF NOT EXISTS (
      SELECT 1
      FROM pg_trigger
      WHERE tgname = 'trg_scrape_jobs_notify_created'
        AND tgrelid = 'public.scrape_jobs'::regclass
    ) THEN
      CREATE TRIGGER trg_scrape_jobs_notify_created
      AFTER INSERT ON public.scrape_jobs
      FOR EACH ROW
      EXECUTE FUNCTION public.notify_scrape_job_created();
    END IF;
  END IF;
END $$;

COMMIT;
