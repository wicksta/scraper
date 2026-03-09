# Docling Page-By-Page Extraction

This repo includes a helper script:

- `scripts/docling_extract_pagewise.py`

It runs Docling one page at a time (safer for large PDFs), writes per-page text files, and can also build one merged text file.

## Prerequisites

- `docling` installed for the current user/environment.
- PDF file path available on disk.

## Basic Usage (per-page files only)

```bash
python3 /opt/scraper/scripts/docling_extract_pagewise.py \
  /opt/scraper/tmp/cityplan.pdf \
  --start-page 1 \
  --end-page 250 \
  --out-dir /opt/scraper/tmp/cityplan_pages
```

Output files:

- `/opt/scraper/tmp/cityplan_pages/page_0001.txt`
- `/opt/scraper/tmp/cityplan_pages/page_0002.txt`
- etc.

## Usage With One Merged File

```bash
python3 /opt/scraper/scripts/docling_extract_pagewise.py \
  /opt/scraper/tmp/cityplan.pdf \
  --start-page 1 \
  --end-page 250 \
  --out-dir /opt/scraper/tmp/cityplan_pages \
  --merged-output /opt/scraper/tmp/cityplan_merged.txt
```

This creates:

- Per-page files in `--out-dir`
- One combined file at `--merged-output`

The merged file includes separators like:

```text
===== PAGE 38 =====
```

## Useful Options

- `--sleep-ms 200`  
  Pause between pages to reduce load.

- `--stop-on-error`  
  Stop immediately on first failed page.

Without `--stop-on-error`, failures are logged and processing continues.

## Quick Single-Page Test

```bash
python3 /opt/scraper/scripts/docling_extract_pagewise.py \
  /opt/scraper/tmp/cityplan.pdf \
  --start-page 38 \
  --end-page 38 \
  --out-dir /opt/scraper/tmp/cityplan_page38 \
  --merged-output /opt/scraper/tmp/cityplan_page38_merged.txt
```

