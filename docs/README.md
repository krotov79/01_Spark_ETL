# NYC Taxi — Spark ETL Pipeline
(Documentation stub; we’ll fill after Steps 2–4.)

## Data Quality & Validation
We include a small Spark-based checker to validate outputs:

```bash
python scripts/dq_checks.py

It reports:
- Row counts per dataset (cleaned `yellow`, enriched `yellow_zones`, and `aggregates/daily`)
- Null counts for critical columns (timestamps, locations, distance, amounts)
- Date range and year–month coverage for `pickup_ts`
- A preview of rows and day-level aggregate coverage

