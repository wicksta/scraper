-- Canonical case-study content for the Newmark planning page.
-- The source CSV is retained as an import snapshot, but the page reads this table.
CREATE TABLE IF NOT EXISTS public.newmark_planning_case_studies (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_row_number integer NOT NULL UNIQUE,
    site text,
    source_name text NOT NULL,
    client text,
    borough text,
    location text,
    latitude double precision,
    longitude double precision,
    geocode_source text,
    source_file_name text,
    source_folder text,
    notes text,
    title text,
    editorial_strapline text,
    service_categories text,
    challenges text,
    our_role text,
    outcome text,
    enrichment_source_sha256 text,
    enrichment_model text,
    enriched_at timestamptz,
    image_file text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS newmark_planning_case_studies_borough_idx
    ON public.newmark_planning_case_studies (borough);

CREATE INDEX IF NOT EXISTS newmark_planning_case_studies_categories_idx
    ON public.newmark_planning_case_studies (service_categories);

CREATE INDEX IF NOT EXISTS newmark_planning_case_studies_title_idx
    ON public.newmark_planning_case_studies (title);
