# Power BI Final Fix Guide

PBIX was not modified or opened; use these verified source fields in Power BI Desktop.

1. Use `vw_powerbi_marketplace_snapshot` for price visuals. Put `currency` in legend/filter and never average across currency.
2. For price charts/filter, apply `price is not blank` and `currency is not blank`. This excludes StockX catalog-only rows from priced metrics while retaining them for catalog counts.
3. Use `marketplace`, `observation_date`, `product_name`, `price`, `currency`, and `product_url` from that view.
4. Amazon `amazon_2023` is only from `dim_product`; title it **Amazon Historical Product Catalog**, not current price/snapshot.
5. Validate measures before presentation:

```DAX
Priced Snapshot Rows = COUNTROWS(FILTER(public_fact_external_product, NOT ISBLANK(public_fact_external_product[price]) && NOT ISBLANK(public_fact_external_product[currency])))
Products = DISTINCTCOUNT(public_fact_external_product[product_key])
Average Price (per currency) = AVERAGE(public_fact_external_product[price])
```

6. Verify the Marketplace card’s label: it must distinguish all catalog/fact platforms from priced platforms. Do not claim StockX has a comparable current price.
