# Scraper / Dashboard Notes

## Oxford Street Development Corporation

OSDC (`E51000008`) uses the shared Detailed Stats, Officers, Agent Comparison,
and Performance pages. Its `PA/year/number` references do not identify the
application type; OSDC queries classify the stored `application_type` instead.
The matching PHP mapping lives in `wcc/osdc_authority.php` on nGISt.

The nine `queries/cache_osdc_*.sql` producers write ten distinct `osdc_` cache
keys and are automatically picked up by `scripts/refresh_query_cache.js` and
the existing daily timer. Officers are queried directly from `applications`.
Pending/consultation/new-application outcomes are excluded from approval
percentages. Timing requires valid validation and decision-issued dates.
Major status and committee routes are absent from the current OSDC records,
so those breakdowns and statutory target lines are not displayed.

## Adding A Third LPA To The Shared Detailed Stats / Comparison / Performance Tabs

The shared PHP pages now support more than Westminster-only logic:

- [stats.php](/opt/scraper/ngist/public_html/wcc/stats.php)
- [wcc_agent_compare.php](/opt/scraper/ngist/public_html/wcc/wcc_agent_compare.php)
- [wcc_speed_curve.php](/opt/scraper/ngist/public_html/wcc/wcc_speed_curve.php)
- [dashboard.php](/opt/scraper/ngist/public_html/dashboard/dashboard.php)
- [wcc_app_type_definitions.js](/opt/scraper/ngist/public_html/wcc/wcc_app_type_definitions.js)

For a new LPA, the main work is now authority-specific SQL and categorisation, not building a separate UI from scratch.

### 1. Confirm The Authority’s Reference Suffixes

Start by checking how the LPA encodes application types in `public.applications.reference`.

Example pattern:

```sql
SELECT
  split_part(reference, '/', 3) AS app_type_code,
  COUNT(*) AS apps
FROM public.applications
WHERE ons_code = '<ONS_CODE>'
  AND reference IS NOT NULL
  AND reference <> ''
  AND reference LIKE '%/%/%'
GROUP BY 1
ORDER BY COUNT(*) DESC, 1;
```

Do not assume Westminster conventions such as `FULL + major column`, `ADFULL`, `ADLBC`, or `ADV`.

### 2. Decide The Authority’s Type Mapping

For the new LPA, define:

- the application types used in detailed stats
- which suffixes count as “major”
- whether there is a meaningful committee bucket
- which types should appear in comparison / performance views

Examples:

- Westminster uses types like `FULL (Major)`, `FULL (Non-Major)`, `LBC`, `ADFULL`, `ADLBC`, `ADV`
- City of London uses `FULL`, `FULMAJ`, `FULEIA`, `LBC`, `ADVT`, `MDC`, `LDC`

Keep this logic authority-specific. Do not force another LPA into Westminster’s buckets if its own suffixes already encode the distinction directly.

### 3. Decide Agent Normalisation

The agent-facing caches use canonical names built from `agent_company_name`, `agent_name`, and `agent_address`.

For the new LPA, decide:

- which agents should be normalised into canonical names
- which agents should appear in the “top agents” and comparison views

This logic lives in the SQL cache producers and should reflect the authority’s actual market, not Westminster’s by default.

### 4. Add LPA-Specific Cache SQL Files

Add new SQL producers under:

- [/opt/scraper/queries](/opt/scraper/queries)

For the current shared tabs, this usually means:

- `cache_<lpa>_total_by_agents.sql`
- `cache_<lpa>_majors_by_agents.sql`
- `cache_<lpa>_committee_by_agents.sql`
- `cache_<lpa>_top_agents.sql`
- `cache_<lpa>_determination_periods.sql`
- `cache_<lpa>_pending_received_weeks.sql`
- `cache_<lpa>_publication_lag_by_validated_week.sql`
- `cache_<lpa>_agent_compare_baseline_approval.sql`
- `cache_<lpa>_agent_compare_baseline_timing.sql`
- `cache_<lpa>_agent_compare_baseline_timing_percentiles.sql`
- `cache_<lpa>_determination_speed_curves.sql`

Each file should:

- filter `public.applications` by the authority `ons_code`
- use authority-specific type mapping
- use authority-specific agent normalisation
- write to a unique `query_cache.cache_key`

### 5. Choose Distinct Cache Keys

`public.query_cache` is keyed by `cache_key`, so multiple LPAs can coexist as long as the keys differ.

Examples:

- Westminster:
  - `wcc_agent_compare_baseline_approval`
  - `wcc_determination_speed_curves`
- City:
  - `city_agent_compare_baseline_approval`
  - `city_determination_speed_curves`

Do not reuse Westminster keys for another authority.

### 6. Extend Shared Type Labels

If the new authority introduces new app-type codes, add readable labels in:

- [/opt/scraper/ngist/public_html/wcc/wcc_app_type_definitions.js](/opt/scraper/ngist/public_html/wcc/wcc_app_type_definitions.js)

Without this, the UI will fall back to raw type codes.

### 7. Make The Shared Pages Aware Of The New Authority

The shared pages already accept `ons_code`, but a new authority still needs config added in:

- [/opt/scraper/ngist/public_html/wcc/wcc_agent_compare.php](/opt/scraper/ngist/public_html/wcc/wcc_agent_compare.php)
- [/opt/scraper/ngist/public_html/wcc/wcc_speed_curve.php](/opt/scraper/ngist/public_html/wcc/wcc_speed_curve.php)

Add an authority config entry covering:

- `ons_code`
- page title / authority label
- the relevant cache keys
- the authority-specific app-type ordering
- the authority-specific type-case SQL
- the authority-specific agent-case SQL

### 8. Expose The Tabs In The Dashboard

Update:

- [/opt/scraper/ngist/public_html/dashboard/dashboard.php](/opt/scraper/ngist/public_html/dashboard/dashboard.php)

Add:

- tab buttons for the new authority
- tab panes / iframes for stats, officers, comparison, performance as needed
- `data-src` URLs that pass the authority `ons_code`
- authority-specific show/hide logic in `updateAuthoritySpecificTabs(...)`

### 9. Refresh The Cache

Run:

```bash
node /opt/scraper/scripts/refresh_query_cache.js
```

This picks up all `queries/cache_*.sql` files automatically.

### 10. Verify In The UI

Check:

- Detailed Stats tab loads
- Agent Comparison tab loads and offers a sensible agent list
- Performance tab loads and shows the expected application types
- labels are human-readable, not raw codes where avoidable
- committee / major logic matches the authority’s real conventions

### Practical Rule

If the shared pages already understand the authority, onboarding another LPA is mostly:

1. identify its type system
2. identify its agent market
3. add the right cache SQL
4. wire the dashboard tab visibility

The failure mode to avoid is assuming Westminster’s categories apply everywhere.
