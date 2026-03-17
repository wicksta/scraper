CREATE TABLE IF NOT EXISTS public.policy_chapters (
    id bigserial PRIMARY KEY,
    doc_id uuid NOT NULL REFERENCES public.documents(id) ON DELETE CASCADE,
    chapter_key text NOT NULL,
    chapter_order integer NOT NULL,
    chapter_number text,
    chapter_title text NOT NULL,
    chapter_text text,
    page_start integer NOT NULL,
    page_end integer NOT NULL,
    heading_path_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    source_meta_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT policy_chapters_doc_key_uq UNIQUE (doc_id, chapter_key)
);

CREATE INDEX IF NOT EXISTS policy_chapters_doc_order_idx
    ON public.policy_chapters (doc_id, chapter_order);

CREATE INDEX IF NOT EXISTS policy_chapters_doc_page_idx
    ON public.policy_chapters (doc_id, page_start, page_end);

CREATE INDEX IF NOT EXISTS policy_chapters_heading_path_gin
    ON public.policy_chapters
    USING gin (heading_path_json);

CREATE INDEX IF NOT EXISTS policy_chapters_source_meta_gin
    ON public.policy_chapters
    USING gin (source_meta_json);
