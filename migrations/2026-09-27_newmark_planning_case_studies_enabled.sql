-- Allow individual Newmark planning case studies to be hidden from the public page
-- without deleting their editorial record.
ALTER TABLE public.newmark_planning_case_studies
    ADD COLUMN IF NOT EXISTS enabled boolean NOT NULL DEFAULT true;

CREATE INDEX IF NOT EXISTS newmark_planning_case_studies_enabled_idx
    ON public.newmark_planning_case_studies (enabled, source_row_number);
