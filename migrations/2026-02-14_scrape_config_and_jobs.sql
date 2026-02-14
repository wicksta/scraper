-- Add LPA-level scrape configuration and job resolution fields.
-- Safe to run multiple times.

BEGIN;

CREATE TABLE IF NOT EXISTS public.lpa_scrape_configs (
  ons_code text PRIMARY KEY,
  site_url text NOT NULL,
  scraper_key text NOT NULL,
  mapper_key text NOT NULL,
  enabled boolean NOT NULL DEFAULT true,
  notes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS lpa_scrape_configs_enabled_idx
  ON public.lpa_scrape_configs (enabled);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.routines
    WHERE routine_schema = 'public'
      AND routine_name = 'set_updated_at'
  ) THEN
    IF NOT EXISTS (
      SELECT 1
      FROM pg_trigger
      WHERE tgname = 'trg_lpa_scrape_configs_updated_at'
        AND tgrelid = 'public.lpa_scrape_configs'::regclass
    ) THEN
      CREATE TRIGGER trg_lpa_scrape_configs_updated_at
      BEFORE UPDATE ON public.lpa_scrape_configs
      FOR EACH ROW
      EXECUTE FUNCTION public.set_updated_at();
    END IF;
  END IF;
END $$;

ALTER TABLE public.scrape_jobs
  ADD COLUMN IF NOT EXISTS ons_code text,
  ADD COLUMN IF NOT EXISTS application_ref text;

CREATE INDEX IF NOT EXISTS scrape_jobs_ons_code_idx
  ON public.scrape_jobs (ons_code);

CREATE INDEX IF NOT EXISTS scrape_jobs_status_created_idx
  ON public.scrape_jobs (status, created_at);

CREATE OR REPLACE FUNCTION public.notify_scrape_job_created()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
declare
  payload text;
begin
  payload := json_build_object(
    'job_id', NEW.id,
    'job_type', NEW.job_type,
    'status', NEW.status,
    'ons_code', NEW.ons_code,
    'application_ref', NEW.application_ref
  )::text;

  perform pg_notify('scrape_job_created', payload);

  return NEW;
end;
$function$;

COMMIT;
