-- Optional applicant extraction fields (LLM-assisted from cover letter text).

ALTER TABLE public.newmark_jobcode_candidates
  ADD COLUMN IF NOT EXISTS applicant_name_extracted text,
  ADD COLUMN IF NOT EXISTS applicant_evidence_quote text,
  ADD COLUMN IF NOT EXISTS applicant_confidence text,
  ADD COLUMN IF NOT EXISTS applicant_extraction_model text;

