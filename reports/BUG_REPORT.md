# Bug Report

## BUG-002 — Recommendation false rank for incomplete StockX data

- Severity: Major (fixed)
- Reproduce: select StockX fact `79660` (price/currency NULL) with Etsy fact `339228`, request Original or THB recommendation.
- Expected: no recommendation ranking; explain incomplete price/currency.
- Root cause: the previous scoring flow allowed an incomplete item into the response list when another item supplied a price.
- Fix: `web/backend/app/main.py` now returns `{ "ranked": [], "limitation": ... }` before scoring whenever any selected record lacks price or currency. `web/frontend/src.tsx` clears stale recommendation state and ignores late API responses after selection/mode changes.
- Retest: PASS for Original and THB; both returned empty `ranked`.
- Current status: CLOSED.

## DATA-001 — StockX catalog records have no price/currency

- Severity: Limitation, not a fabricated-data bug.
- Root cause: StockX Catalog Search source intentionally has no market price/currency. Market Data API requires authorization; one read-only request returned 401.
- Resolution: catalog-only handling; no USD/rate/price fabricated. Price enrichment: BLOCKED pending authorized Market Data access.
