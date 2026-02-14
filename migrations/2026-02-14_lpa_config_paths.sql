-- Move lpa_scrape_configs to DB-owned executable paths.
-- Safe to run multiple times.

BEGIN;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'lpa_scrape_configs'
      AND column_name = 'scraper_key'
  ) THEN
    ALTER TABLE public.lpa_scrape_configs
      RENAME COLUMN scraper_key TO scraper_entrypoint;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'lpa_scrape_configs'
      AND column_name = 'mapper_key'
  ) THEN
    ALTER TABLE public.lpa_scrape_configs
      RENAME COLUMN mapper_key TO mapper_path;
  END IF;
END $$;

UPDATE public.lpa_scrape_configs
SET scraper_entrypoint = 'scraper.cjs'
WHERE scraper_entrypoint IN ('idox', 'scraper.js');

UPDATE public.lpa_scrape_configs
SET mapper_path = 'mappers/westminster.js'
WHERE mapper_path = 'westminster';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'lpa_scrape_configs_scraper_entrypoint_nonempty'
      AND conrelid = 'public.lpa_scrape_configs'::regclass
  ) THEN
    ALTER TABLE public.lpa_scrape_configs
      ADD CONSTRAINT lpa_scrape_configs_scraper_entrypoint_nonempty
      CHECK (length(btrim(scraper_entrypoint)) > 0);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'lpa_scrape_configs_mapper_path_nonempty'
      AND conrelid = 'public.lpa_scrape_configs'::regclass
  ) THEN
    ALTER TABLE public.lpa_scrape_configs
      ADD CONSTRAINT lpa_scrape_configs_mapper_path_nonempty
      CHECK (length(btrim(mapper_path)) > 0);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'lpa_scrape_configs_scraper_entrypoint_relative'
      AND conrelid = 'public.lpa_scrape_configs'::regclass
  ) THEN
    ALTER TABLE public.lpa_scrape_configs
      ADD CONSTRAINT lpa_scrape_configs_scraper_entrypoint_relative
      CHECK (scraper_entrypoint NOT LIKE '/%' AND scraper_entrypoint NOT LIKE '%..%');
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'lpa_scrape_configs_mapper_path_relative'
      AND conrelid = 'public.lpa_scrape_configs'::regclass
  ) THEN
    ALTER TABLE public.lpa_scrape_configs
      ADD CONSTRAINT lpa_scrape_configs_mapper_path_relative
      CHECK (mapper_path NOT LIKE '/%' AND mapper_path NOT LIKE '%..%');
  END IF;
END $$;

COMMIT;
