# Advice Thailand runtime HTML diagnosis

## Scope

This investigation used only existing logs, code, raw snapshots, and read-only
warehouse queries. It did not trigger a DAG, request Advice Thailand again, or
write raw/processed/warehouse data.

## Live-run evidence

The latest temporary task log records:

```text
HTML_BYTES: 339727
VALID_PRODUCTS: 0
EXTRACTION_MODE: structured_card
DIAGNOSTIC_CARD_LINKS: 0
DIAGNOSTIC_TABLE_ROWS: 0
```

The collector requests the permitted public price-list URL with its declared
user-agent after a `robots.txt` check. Its server-rendered response therefore
contained neither a table row nor an anchor href matching
`/productdetail/A...`. It is not a product listing in either of the two
supported server-rendered contracts.

## Response artefact search

No HTML response from the failed run was saved in `data/`, `logs/`, reports,
or the available temporary locations. The only Advice raw artefact is the
previous validated JSON snapshot:

`data/raw/external/advice_th/advice_th_20261011_164306_300987.json`

It is data, not the source HTML, so it cannot establish whether the current
response is a JavaScript shell, access/interstitial page, or changed listing
markup.

## Root-cause status

**BLOCKED — live response classification cannot be proved offline.**

The evidence rules out the prior HTML table and the known card/link contract.
The 339,727-byte response is consistent with several possibilities, including
a JavaScript application shell, an access/interstitial page, or another page
layout. There is no retained body from which to select or implement a parser.
No access-control bypass was attempted.

## Data preservation checks

- Existing raw snapshot validation: **PASS**, 196 valid THB products.
- `dim_product` Advice rows (read-only): **196**.
- `fact_external_product` Advice rows (read-only): **196**.
- Minimum valid-product threshold: unchanged at **150**.
- `prepare_advice_th` in the failed run: **upstream_failed**, so it correctly
  did not execute.

## Test results

| Test | Result |
| --- | --- |
| Table parser fixture | PASS |
| Structured-card parser fixture | PASS |
| Unknown-page diagnostics fixture | PASS |
| Collector syntax | PASS |
| Airflow DAG parse | PASS — no import errors |
| Actual-layout regression test | NOT TESTED — no retained actual HTML exists |
| Live source retest | NOT TESTED — requires permission |

## Safe next step requiring approval

Authorize one bounded, read-only diagnostic HTTP request to the same permitted
price-list URL, after the normal `robots.txt` check. The diagnostic should
store an exclusive local HTML sample for offline inspection and redact/avoid
reporting any cookie, authorization, session-token, or form-token values.
It must not create a product snapshot, run Spark, or write the warehouse.

Only after classifying that saved HTML should a parser for any new layout be
implemented and tested. If it is a JavaScript-only page, access-denied page, or
bot-protection response, the collector remains blocked and the existing
validated snapshot must be presented as historical—not current—data.
