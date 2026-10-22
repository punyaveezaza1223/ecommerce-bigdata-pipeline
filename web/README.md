# Smart Commerce Web App

Run from the repository root:

```powershell
docker compose up -d --build web-backend web-frontend
```

- UI: `http://localhost:5174`
- API docs: `http://localhost:8088/docs`

The API is read-only and uses the Docker hostname `warehouse-postgres`. It never
accepts raw SQL. Product search is server-paginated (maximum 50 rows). Amazon is
a historical product catalog and is deliberately excluded from price snapshots.

The recommendation endpoint is explainable rule-based ranking. It refuses
cross-currency price ranking because no verified FX conversion exists.
