# NYC Taxi — Spark-based Big Data Pipeline

End-to-end ETL workflow built with **PySpark** to process and analyze New York City Yellow Taxi trip data.  
The pipeline reads public **Parquet** data, performs cleaning and feature extraction, joins external lookup tables (pickup/drop-off zones), computes daily revenue and tip aggregations, runs basic data-quality checks, and produces a visualization of daily earnings.

---

### 🧰 Stack
**PySpark · Pandas · Matplotlib · Parquet**

### ⚙️ Features
- Modular ETL scripts for ingestion, transformation, and enrichment  
- Partitioned Parquet outputs for efficient querying  
- Automated workflow via `Makefile` (`ETL → Aggregation → Zone Join → DQ → Plot`)  
- Lightweight data-quality validator (`scripts/dq_checks.py`)  
- Reproducible single-command run: `make all`

### 🎯 Outcome
A clean, reproducible Spark pipeline demonstrating practical big-data handling,  
data-quality validation, and simple analytics — easily extendable to cloud-scale environments (e.g., Delta Lake or multi-month batches).

---

## 🧾 Data Quality & Validation

A small Spark-based checker validates pipeline outputs:

```bash
python scripts/dq_checks.py

