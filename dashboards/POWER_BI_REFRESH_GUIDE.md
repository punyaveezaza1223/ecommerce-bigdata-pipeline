# Power BI refresh guide

Use `vw_powerbi_marketplace_snapshot` for detail visuals and keep `currency`
in every price visual's grouping or legend. Do not average currencies together.

Create active, single-direction relationships from `dim_product`, `dim_platform`,
and `dim_date` (one) to `fact_external_product` (many). Clear any page or
visual filter that restricts the Marketplace slicer to the legacy three sources.

Suggested measures:

```DAX
Snapshot Rows = COUNTROWS(public_fact_external_product)
Products = DISTINCTCOUNT(public_fact_external_product[product_key])
Average Price (per currency) = AVERAGE(public_fact_external_product[price])
```

Amazon 2023 is historical product metadata with no verified price-observation
timestamp, so it is intentionally absent from price/snapshot visuals. It can be
used through `dim_product` for product, brand, and category analysis only.
