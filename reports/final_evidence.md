# Final Project Evidence

This document records the verified evidence for the final-project additions. It does not replace the midterm results report.

## 1. Queries and Indexes

### Final indexes

The final index setup contains four indexes on `orders_validated`:

- `uniq_order_id` — unique index on `order_id`
- `idx_customer_id` — index on `customer_id`
- `idx_city` — index on `city`
- `idx_status_order_date` — compound index on `status` and `order_date`

Verified by running `python -m src.final_indexes`.

### Query 1 — Customer ID

Query: `customer_id = "عميل-9901424"`

| Metric | Before index | After index |
|---|---:|---:|
| Documents returned | 1 | 1 |
| Documents examined | 28,041,037 | 1 |
| Keys examined | 0 | 1 |
| Execution time (ms) | 127,469 | 16 |
| Winning stage | COLLSCAN | FETCH / index-backed |

The query changed from a full collection scan to an index-backed lookup.

### Query 2 — City

Query: `city = "المكلا"`

| Metric | Before index | After index |
|---|---:|---:|
| Documents returned | 2,803,862 | 2,803,862 |
| Documents examined | 28,041,037 | 2,803,862 |
| Keys examined | 0 | 2,803,862 |
| Execution time (ms) | 125,766 | 94,446 |
| Winning stage | COLLSCAN | FETCH / index-backed |

The city index reduced the number of documents examined to the matching set. Because the predicate returns a large portion of the collection, the measured time improvement is smaller than for the selective customer query.

### Query 3 — Status + date range

Query:
- `status = "مؤكد"`
- `order_date >= 2025-03-20`
- `order_date < 2025-03-21`

The query matched 38,851 records. The evidence query returned the first 20 records for the execution-statistics comparison.

| Metric | Before index | After compound index |
|---|---:|---:|
| Documents returned | 20 | 20 |
| Documents examined | 10,356 | 20 |
| Keys examined | 0 | 20 |
| Execution time (ms) | 35 | 1 |
| Winning plan | LIMIT → COLLSCAN | LIMIT → FETCH → IXSCAN |
| Index | — | `idx_status_order_date` |

The compound index provides both the status equality bound and the date-range bound.

## 2. Aggregations

Five independently runnable aggregation reports were implemented and verified with actual MongoDB data:

1. `aggregation_01_city_sales.py` — sales summary by city.
2. `aggregation_02_status_sales.py` — sales summary by order status.
3. `aggregation_03_daily_sales.py` — daily sales summary.
4. `aggregation_04_top_customers.py` — top customers for a supplied date range.
5. `aggregation_05_city_status.py` — grouped summary by city and status.

## 3. Materialized Views

Two materialized views were implemented:

- `mv_city_sales` — materialized city-level sales summary.
- `mv_daily_sales` — materialized daily sales summary.

Both provide full and incremental refresh mechanisms.

### MV1

Full refresh completed successfully and processed 10 city groups. Incremental refresh was also executed successfully; the tested run had no new source groups to process.

### MV2

Full refresh completed successfully and produced 121 daily source groups plus refresh-state metadata. An incremental refresh mechanism was also executed successfully.

The daily-sales scheduled job is configured to call the incremental refresh function rather than the full-refresh function.

## 4. Scheduled Jobs

Two project functions are exposed as scheduled jobs:

- `refresh_city_sales`
- `refresh_daily_sales`

Each job records start time, finish time, status, and result/error information in MongoDB `job_logs`.

Manual executions were verified for both job functions.

APScheduler was also verified as running with two registered hourly jobs:

- `refresh_city_sales` — every 1 hour
- `refresh_daily_sales` — every 1 hour

The scheduler registration was verified without waiting for an hourly trigger.

## 5. FastAPI

The unified FastAPI application exposes the required endpoints:

- `GET /health`
- `POST /ingest`
- `POST /indexes`
- `GET /queries`
- `GET /queries/{name}`
- `GET /aggregations`
- `GET /aggregations/{name}`
- `POST /refresh-mv`
- `GET /jobs`
- `POST /jobs/{name}/run`

Swagger documentation is available through `/docs`.

Verified:

- `/health` returned HTTP 200.
- `/queries` returned the five query names.
- `/aggregations` returned the five aggregation names.
- `/jobs` returned the two configured jobs.
- `/indexes` returned the four final indexes.
- All required routes were confirmed in the FastAPI route table.

The `/ingest` route was not executed against the 30-million-record dataset during final verification to avoid unnecessary reprocessing. Its implementation calls the existing project ingestion/router pipeline.

## 6. Project Safety Notes

The final verification did not delete or rebuild the 30-million-record source collections. Existing midterm data and evidence remain intact.

Heavy full-refresh operations were not unnecessarily repeated. This evidence records observed test results rather than inventing additional benchmark numbers.
