# Advice Thailand live diagnostic result

## Authorization and scope

One diagnostic source fetch was performed after `robots.txt` allowed the same
public price-list URL and user-agent as the collector. No DAG was triggered,
no product snapshot was created, and no processed or warehouse data changed.

## Evidence

| Item | Observed value |
| --- | --- |
| HTTP body size | 339,727 bytes |
| Page title | `desktop-pc-server | Price List` |
| Server-rendered `/productdetail/A...` links | 0 |
| Server-rendered `<tr>` rows | 0 |
| Known historical product ID `A0183648` | absent |
| Known historical price `23990` | absent |
| Product-list page content in HTML | absent |
| Explicit access/bot markers | absent |
| Script blocks removed during safe capture | 5 |

The original `cloudflare` signal was a false positive from the normal public
Font Awesome CDN URL `cdnjs.cloudflare.com`; it is not classified as bot
protection. The diagnostic classifier was corrected accordingly.

The sanitized offline HTML has an expected price-list title but contains no
product markup, links, known IDs, or known price values. It is therefore
classified as **other_or_unrecognized_html**. The available evidence cannot
prove client-side JavaScript loading because scripts were intentionally removed
from the saved sample to avoid retaining possible session/form secrets.

## Decision

**BLOCKED.** The live response is not a server-rendered product listing that
the collector can safely parse. There is no evidence of an allowed public data
endpoint in the retained sanitized artefact, and no attempt was made to bypass
access controls or inspect protected runtime traffic.

`ingest_advice_th` was deliberately not run again: its prerequisite—verified
listing data with at least 150 valid products—was not met. `prepare_advice_th`,
Spark ETL and warehouse loading were not run.

## Preservation and test results

| Check | Result |
| --- | --- |
| Existing Advice raw snapshot | PASS — 196 validated THB products |
| Existing Advice warehouse dimension/facts | PASS — 196 / 196 (read-only) |
| Minimum threshold | PASS — unchanged at 150 |
| Offline diagnostic classifier | PASS — no false bot-protection classification |
| Parser fixtures | PASS (table, structured card, unknown page) |
| Diagnostic script syntax | PASS |
| Airflow DAG parse | PASS — no import errors |
| `ingest_advice_th` new runtime attempt | NOT RUN / BLOCKED |

## Files changed

- `scripts/diagnose_advice_response.py` (new; one-shot robots-respecting,
  sanitized diagnostic utility)
- `reports/ADVICE_LIVE_DIAGNOSTIC_RESULT_20261012.md` (this report)

The existing snapshot should be used for presentation only with its observed
timestamp and labelled historical/not a fresh current snapshot. A future live
ingestion change requires an explicit, documented public data source or Advice
approval; it must not lower validation standards or bypass protection.
