# Planning data import tools

Manual importer for MHCLG Planning Quality Open Data (P152a major and P154 non-major decisions). Run on Otso with Node.js and Python 3. It uses the repository's existing `mysql2` package and Python standard library; no installation or scheduler is needed.

## Commands

Run from any directory; these absolute commands work from Otso:

```bash
# Discover the current GOV.UK workbook, inspect changes without writing anything.
node /opt/scraper/planning_data_import_tools/import_planning_quality.js --dry-run

# Discover, validate and publish to the existing dashboard data directory.
# The /mnt/ngist SSH mount must be available in this terminal session.
node /opt/scraper/planning_data_import_tools/import_planning_quality.js

# Import a specific release.
node /opt/scraper/planning_data_import_tools/import_planning_quality.js \
  --source 'https://assets.publishing.service.gov.uk/media/6ab3b2b0997a4b2950ccedb9/Planning_quality_-_open_data_-_202606.ods' \
  --dry-run

# Build locally when nGISt is not mounted; --source also accepts a local file.
node /opt/scraper/planning_data_import_tools/import_planning_quality.js \
  --source /path/to/planning_quality.ods --output-dir /tmp/planning-quality-output
```

The LPA lookup comes from a read-only query of MySQL `lpa_codes`. Credentials are loaded from `/opt/scraper/.env` (`MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE`/`MYSQL_DB`, `MYSQL_USER`, `MYSQL_PASSWORD`/`MYSQL_PASS`); exported environment variables take precedence. For an offline run, pass `--lpa-json /path/to/lpas.json`, containing an array of `{ "ons_code": "E09000033", "lpa_name": "Westminster" }` records. Use the actual lookup export, not the old rankings, for production imports.

## Outputs and calculations

The importer preserves the dashboard's existing filenames and JSON arrays:

- `250419_appeals_by_year.json`: annual totals across the complete source workbook, including historical authorities. The old date in the filename is retained for reader compatibility.
- `2020_appeal_rankings.json`: totals from January 2020 to the latest quarter. The population is the LPA lookup intersected with authorities present in that quarter in **both** sheets. This is a data-based definition of current authorities, not a separate legal-status register.
- `appeals_metadata.json`: source/hash, coverage, partial years, generation, lookup hash, output hashes and check/update times.

The source's quarter refers to the original application-decision quarter, or non-determination appeal receipt quarter. It is **not** the year in which PINS decided the appeal. Annual stored per-100 rates divide by application decisions; ranking rates divide by decisions **plus non-determined cases**, matching the previous builders. Overturn rates divide overturned cases by appeal decisions.

Rank 1 is the highest value. Ties receive successive ranks, ordered by ONS code. Undefined rates and their ranks are `null`. Annual rates retain one decimal; ranking overturn rates one decimal and per-100 rates two decimals.

Importing rebuilds historical years as well as adding new ones, so revisions are captured. Coverage regression, unexpected symbols/missing counts, duplicate authority/quarter rows, mismatched sheet periods and inconsistent component totals fail validation.

The dashboard's existing PHP/JavaScript readers use these files directly. Its Appeals headings and source footers read `appeals_metadata.json` on each page request, showing the full dataset period, ranking period, last data update and last successful check. Reload the dashboard after publication to see new dates; missing or malformed metadata shows unavailable coverage instead of an old hard-coded date.

## Publication, backups and recovery

The importer stages and reads back the JSON files on the destination before replacing them by rename, with metadata last as the completion marker. Replacements are atomic **per file**; existing readers do not support a transaction spanning several requests, so avoid loading the dashboard during publication. Output files retain their previous permission modes (new files use 0644).

Downloaded workbooks and backup bundles are kept under the git-ignored, private `planning_data_import_tools/.state/` directory, separated by output-directory hash. The CLI prints the backup path. A process lock prevents simultaneous imports to the same destination. If interrupted, the next publishing run recovers the previous bundle before importing; dry runs refuse a pending recovery. A live SSH disconnect can delay rollback until the mount is restored.

Each successful run also archives the read-only LPA lookup as a hash-named `.lpas.json` file alongside the workbook, enabling reproducible offline runs with `--lpa-json`.

An unchanged dataset leaves the two data files untouched and refreshes only check metadata. No weekly job is installed. Run manually whenever a refresh is needed.

For manual rollback, stop imports and copy the annual/ranking files from the printed backup directory back into the destination, along with metadata if the backup contains it. A pre-import backup may have no metadata; remove the new metadata when restoring that older snapshot. Do not run the old `combineAppeals.php` or `appeal_rankings.php` builders afterward: they can overwrite this tool's output.

## Tests

```bash
node /opt/scraper/planning_data_import_tools/appeals.test.js
python3 /opt/scraper/planning_data_import_tools/test_ods.py

# Verify the mounted dashboard's actual readers and rendering functions.
node /opt/scraper/planning_data_import_tools/verify_dashboard.js

# The same check against a locally staged bundle, without modifying the readers.
node /opt/scraper/planning_data_import_tools/verify_dashboard.js --data-dir /tmp/planning-quality-output
```

Tests cover ODS numeric cells/repeats, malformed data and discovery ambiguity, cohort aggregation, denominators, population selection, null rates/ties, rollback and interrupted-run recovery.

The dashboard verifier checks output hashes, both PHP readers, annual/ranking/chart card markup and the major/non-major chart configurations for Westminster and Camden. It also checks that later metadata changes coverage/update/check dates and that missing or malformed metadata has a truthful fallback. It uses mocked DOM/canvas objects, not a logged-in browser session.

## Initial import: 3 October 2026

The supplied `Planning_quality_-_open_data_-_202606.ods` release was published successfully with coverage **2016 Q1–2025 Q3**, 3,311 annual records and 283 ranked authorities. Its latest quarter contains 310 authorities; 27 do not match the existing LPA lookup and are excluded from rankings under the agreed rule. Their annual source records remain available.

Verification included independent aggregation of every annual count and ranking total, the existing live PHP readers and dashboard render functions, and an unchanged repeat import. Automatic GOV.UK discovery also produced the same dataset. Unit tests cover format handling and rollback/recovery.

Regression against the saved older workbook identified the existing converter's comma-number bug in six 2021 non-major authority/year records: Birmingham, Bradford, Leeds, Cornwall, Buckinghamshire and Wiltshire. The old JSON exactly matched the converter's literal CSV behaviour; the new importer correctly retains the omitted counts. Other old-workbook values matched.
