# Presentation Readiness

## Verdict: READY WITH LIMITATIONS

Demonstrate: Docker services, Airflow DAG list, Spark UI, Warehouse facts, marketplace search/filter, original/THB comparisons, valid rule-based recommendation, Amazon ASIN catalog search.

Say explicitly:

- Pipeline is batch/snapshot, not real-time.
- Amazon 2023 is historical metadata, not a current price fact.
- StockX is catalog-only in this dataset because its source lacks verified price/currency; it is excluded from price recommendation and THB conversion.
- Recommendation is rule-based, not ML.
- THB uses Frankfurter v2 via Backend; rate source/date are displayed.

Do not claim browser E2E, Power BI Desktop verification, or full DAG execution as passed. Use URLs: Frontend `http://localhost:5174`, API docs `http://localhost:8088/docs`, Airflow `http://localhost:8190`, Spark `http://localhost:8181`.
