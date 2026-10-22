# Final QA Test Results — 2026-10-12

| ID | Objective | Expected / Actual | Status | Evidence | Bug |
|---|---|---|---|---|---|
| TC-026-R | StockX + Etsy recommendation, Original | `ranked: []` and limitation / returned exactly that | PASS | POST `/api/recommendations`, facts 79660+339228 | BUG-002 |
| TC-026-T | StockX + Etsy recommendation, THB | no false rank / returned `ranked: []` and limitation | PASS | same request with `display_currency=thb` | BUG-002 |
| TC-REC-USD | valid USD pair | ranked products returned | PASS | facts 339674+339693 returned 2 ranked rows | — |
| TC-SEARCH-01 | search and filters | backend filtering works | PASS | shoe/all, Etsy, eBay, Etsy+eBay all HTTP 200 | — |
| TC-SEARCH-02 | StockX/Advice/Rakuten | records queryable | PASS | each `/api/products?platforms=` returned HTTP 200 | — |
| TC-AMZ-01 | Amazon ASIN | B00E5U3YEK returned catalog row | PASS | `/api/amazon/products` HTTP 200 | BUG-001 regression |
| TC-AMZ-02 | Amazon name/page | LG page 2 returned | PASS | HTTP 200 | BUG-001 regression |
| TC-FX | USD/GBP to THB | provider endpoint responds | PASS | both exchange endpoints HTTP 200 | — |
| TC-BUILD | TypeScript/Vite production build | build succeeds | PASS | `tsc -b && vite build` | — |
| TC-DOCKER | web containers and URLs | Up / HTTP 200 | PASS | compose ps, frontend/health/docs | — |
| TC-BROWSER | interactive UI, chart, mobile | browser automation unavailable | NOT TESTED | no browser binding available | — |
| TC-DAG | pipeline execution | no destructive/full trigger performed | NOT VERIFIED | only parse/service checks | — |

No mock data was used in production endpoint tests.
