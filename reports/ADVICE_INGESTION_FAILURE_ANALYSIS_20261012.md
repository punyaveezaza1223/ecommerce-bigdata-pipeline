# Advice Thailand ingestion failure analysis

## Scope

Read-only investigation of the failed run
`manual__2026-10-11T23:26:02.581156+00:00`. No DAG was retriggered, no Advice
web request was made during this investigation, and no warehouse data changed.

## Evidence from the failing task log

`ingest_advice_th` began successfully and reached the source request path, then
logged:

```text
VALID_PRODUCTS: 0
REJECTED_ROWS: 0
RuntimeError: Too few valid products; stop without writing.
```

The original parser only counted a candidate after finding a `<tr>` that had
exactly one link matching `/productdetail/A...`. `REJECTED_ROWS: 0` shows the
failure happened *before* price/name validation: no table row reached that
candidate stage. This is not evidence of a low product count and the 150-item
threshold was not reduced.

The raw snapshot at
`data/raw/external/advice_th/advice_th_20261011_164306_300987.json` was
successfully collected earlier the same day. It contains 196 valid products;
the timestamps establish that the source/response contract differed between
the earlier collection and the failed run.

Because the task did not retain its response body, the available evidence does
not distinguish conclusively between a site layout change (table → cards), a
context-dependent response, or an access/interstitial response. External-source
runtime status is therefore **BLOCKED pending an approved retest**.

## Downstream and warehouse status

| Check | Result |
| --- | --- |
| `ingest_advice_th` | FAILED |
| `prepare_advice_th` | `upstream_failed` — correctly did not run |
| Existing `dim_product` Advice rows | 196 (read-only query) |
| Existing `fact_external_product` Advice rows | 196 (read-only query) |
| Warehouse writes during investigation | None |

## Corrective change

`scripts/collect_advice_th.py` now:

1. Retains the original strict table extraction path.
2. Uses a strict structured-card fallback **only when the table path returns
   zero products**. A card still needs exactly one Advice product ID, exactly
   one THB price, a real product name, and positive price.
3. Emits HTML size, extraction mode, row/link counts and reason counters.
   A future approved run can distinguish no table rows, missing product links,
   invalid card containers, or missing names without inventing data.
4. Keeps the 150-product quality gate, robots policy, duplicate-price check,
   disk check, and exclusive JSON output unchanged.

No automatic fallback to the old snapshot was added. Such a fallback would be
misleading because it is historical data, not a newly observed price snapshot.
The existing 196-row snapshot remains available for read-only reporting or an
explicitly labelled historical-data workflow.

## Regression tests

| Test | Result |
| --- | --- |
| Existing Advice raw snapshot validation | PASS — 196 canonical THB records |
| Table-layout parser fixture | PASS |
| Structured-card parser fixture | PASS |
| Unrecognized-page diagnostics fixture | PASS |
| Collector/DAG syntax validation | PASS |
| Airflow DAG parse | PASS — no import errors |
| Live Advice source retest | NOT TESTED (intentionally) |

## Safe retest after approval

1. Use the Airflow UI to trigger **only** `01_ingest_external_data` once, or
   run only task `ingest_advice_th`; do not start the master DAG.
2. Inspect its log for `HTML_BYTES`, `EXTRACTION_MODE`, and `DIAGNOSTIC_*`.
3. If at least 150 valid products pass, inspect the new exclusive raw JSON
   before manually approving `prepare_advice_th`, Spark ETL, and warehouse load.
4. If valid products remain zero, do not lower the threshold. Preserve the log
   evidence and classify the live source as blocked until Advice's response
   contract/access policy is clarified.
