# Indian Startup Funding Data Pipeline (Medallion Architecture)

A local, fully offline **Data Engineering pipeline** built with **PySpark** and
**Spark SQL** that ingests raw Indian startup funding data and processes it
through a **Bronze → Silver → Gold** Medallion Architecture, producing
business-ready analytical tables — all without any cloud services.

---

## 1. Project Overview

This project simulates a real-world Data Engineering internship task: take a
messy, real-world-style CSV export of Indian startup funding records
(2020–2025) and turn it into clean, trustworthy, analytics-ready datasets.

It intentionally avoids any cloud vendor lock-in (no Azure, AWS, GCP, or
Databricks) so it can be run entirely on a local Windows/Mac/Linux machine
using open-source PySpark — making it easy to demo, grade, and reproduce.

**What it does:**
1. **Bronze** — Ingests the raw CSV as-is (no cleaning) and stores it as Parquet.
2. **Silver** — Cleans, deduplicates, standardizes, and type-casts the data.
3. **Gold** — Builds 10 business-facing analytical tables using Spark SQL.
4. **SQL** — Ships 24 additional standalone Spark SQL business/analysis queries.

---

## 2. Architecture

```
┌──────────────┐      ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│   Raw CSV    │ ───▶ │    BRONZE    │ ───▶ │    SILVER    │ ───▶ │     GOLD     │
│ (data/raw)   │      │ (data/bronze)│      │ (data/silver)│      │ (data/gold)  │
└──────────────┘      └──────────────┘      └──────────────┘      └──────────────┘
      │                     │                      │                     │
      │                     ▼                      ▼                     ▼
  Untouched            Raw Parquet          Clean, deduped,        10 analytical
  source file          (exact copy          standardized,          Parquet tables
                        of raw data,         typed Parquet          built with
                        no cleaning)         (partitioned           Spark SQL
                                              by funding_year)
```

**Layer responsibilities**

| Layer  | Responsibility                                                                 |
|--------|---------------------------------------------------------------------------------|
| Bronze | Read raw CSV, write to Parquet unmodified. Source of truth / replay-ability.    |
| Silver | Deduplicate, handle nulls, standardize names, clean amounts, parse dates, type-cast, drop invalid records. |
| Gold   | Aggregate into business-ready tables (top startups, top investors, funding by year, etc.) using Spark SQL. |

---

## 3. Folder Structure

```
Indian-Startup-Pipeline/
├── config/
│   ├── __init__.py
│   └── config.py                # All paths & settings (pathlib-based)
│
├── data/
│   ├── raw/                     # Source CSV lives here
│   ├── bronze/                  # Raw data as Parquet (generated)
│   ├── silver/                  # Cleaned data as Parquet (generated)
│   └── gold/                    # Analytical tables as Parquet (generated)
│
├── logs/
│   └── pipeline.log             # Rotating pipeline execution log (generated)
│
├── sql/
│   ├── analysis.sql             # Data-quality / exploratory Spark SQL
│   └── business_queries.sql     # 24 business-facing Spark SQL queries
│
├── src/
│   ├── __init__.py
│   ├── spark_session.py         # Local SparkSession factory
│   ├── utils.py                 # Logging + reusable cleaning/standardization functions
│   ├── bronze.py                # Bronze layer logic
│   ├── silver.py                # Silver layer logic
│   ├── gold.py                  # Gold layer logic (Spark SQL aggregations)
│   └── pipeline.py              # Orchestrates Bronze → Silver → Gold
│
├── screenshots/                 # Place execution screenshots here for submission
├── requirements.txt
├── README.md
└── .gitignore
```

---

## 4. Requirements

- Python 3.11
- Java 8/11/17 (required by PySpark — install a JDK and set `JAVA_HOME`)
- pip packages listed in `requirements.txt`:
  - `pyspark`, `pandas`, `numpy`, `pyarrow`, `python-dateutil`

> **Note (Windows):** PySpark on Windows also needs `winutils.exe` /
> Hadoop binaries on `PATH` for certain filesystem operations. If you hit
> `HADOOP_HOME` warnings, they are typically harmless for local Parquet
> read/write, but you can install `winutils` for a fully clean run.

---

## 5. Installation

```bash
git clone https://github.com/rishika2006/celebal-project.git
cd celebal-project

python -m venv .venv

# Windows
.venv\Scripts\activate

pip install -r requirements.txt
```

## Run the Pipeline

```bash
python src/pipeline.py
```



## 6. Execution Steps

### Run the full pipeline (Bronze → Silver → Gold) in one command:

```bash
python src/pipeline.py
```

### Or run each layer independently (useful for debugging/demoing):

```bash
python src/bronze.py     # Step 1: raw CSV -> Bronze Parquet
python src/silver.py     # Step 2: Bronze -> cleaned Silver Parquet
python src/gold.py       # Step 3: Silver -> Gold analytical tables
```

### Run the ad-hoc SQL business queries:

```python
from src.spark_session import get_spark_session
from src.silver import read_silver_parquet
from src.gold import register_silver_view

spark = get_spark_session()
silver_df = read_silver_parquet(spark)
register_silver_view(spark, silver_df)

with open("sql/business_queries.sql") as f:
    # Strip comment lines first, then split on ";" to get each statement
    lines = [line for line in f.read().splitlines() if not line.strip().startswith("--")]
    clean_sql = "\n".join(lines)
    queries = [q.strip() for q in clean_sql.split(";") if q.strip()]

for query in queries:
    spark.sql(query).show(truncate=False)
```

Logs for every run are written to `logs/pipeline.log` as well as the console.

---

## 7. Screenshots Section

> Add screenshots of your local run here for internship submission, e.g.:

- `screenshots/01_bronze_layer_console.png` — Bronze layer console output
- `screenshots/02_silver_layer_console.png` — Silver layer dedup/cleaning logs
- `screenshots/03_gold_tables_output.png` — Gold layer table generation
- `screenshots/04_spark_sql_query_results.png` — Sample business query results
- `screenshots/05_project_folder_structure.png` — VS Code project explorer

---

## 8. Expected Output

After a successful run:

- `data/bronze/bronze_startup_funding/` — raw data as Parquet (untouched)
- `data/silver/silver_startup_funding/` — cleaned data as Parquet, partitioned by `funding_year`
- `data/gold/` — 10 sub-folders, one per analytical table:
  - `top_funded_startups/`
  - `top_investors/`
  - `top_sectors/`
  - `top_cities/`
  - `funding_by_year/`
  - `funding_by_investment_type/`
  - `investor_activity/`
  - `sector_funding/`
  - `monthly_funding/`
  - `average_investment/`
- `logs/pipeline.log` — full execution log with row counts, timings, and dedup/invalid-record stats for every stage.

---

## 9. Data Quality Handled by the Silver Layer

The sample raw dataset intentionally contains realistic messy data so the
Silver layer has real problems to solve:

- **Duplicates** — exact duplicate rows are dropped.
- **Inconsistent city names** — `Bangalore`, `Bengaluru`, `BLR`, `bangalore`
  all standardize to a single canonical `Bangalore`.
- **Inconsistent investor names** — `Sequoia`, `sequoia capital india`,
  `Sequoia Capital` all standardize to `Sequoia Capital`. Multi-investor
  cells (comma-separated) are split, standardized individually, deduplicated,
  and rejoined.
- **Messy funding amounts** — values like `"$30,000,000"`, `"30,000,000"`,
  `"₹2,49,00,00,000"`, `"N/A"`, `"Undisclosed"`, and blanks are all parsed
  into a single clean `amount_usd` double column (or `null` when truly
  undisclosed).
- **Mixed date formats** — `yyyy-MM-dd`, `dd/MM/yyyy`, `MM-dd-yyyy`,
  `dd-MMM-yyyy`, and `yyyy/MM/dd` are all parsed into one canonical
  `DateType` column.
- **Invalid records** — rows missing a startup name, city, investor, valid
  amount, or valid date are removed from Silver (and their counts logged)
  rather than silently corrupting downstream analytics.

---

## 10. Future Improvements

- Add automated data-quality tests (e.g. `pytest` + `chispa`) for every
  transformation function in `src/utils.py`.
- Add a `dq_report.py` step that writes a data-quality summary (null %,
  duplicate %, invalid % ) to `data/gold/dq_report/` on every run.
- Parameterize the pipeline via a CLI (`argparse`) to support incremental /
  date-ranged loads instead of always-overwrite full loads.
- Add a lightweight BI layer (e.g. Streamlit or Apache Superset pointed at
  the Gold Parquet tables) for interactive dashboards.
- Add CI (GitHub Actions) to run the pipeline against the sample dataset on
  every push, catching regressions automatically.
- Swap the local Parquet Gold tables for a proper local warehouse (e.g.
  DuckDB or SQLite) to enable faster ad-hoc SQL without re-reading Parquet.

---

## 11. Tech Stack

| Tool        | Purpose                                   |
|-------------|--------------------------------------------|
| Python 3.11 | Core language                              |
| PySpark     | Distributed data processing (local mode)   |
| Spark SQL   | Business analytics & aggregation           |
| Parquet     | Columnar storage format for all 3 layers   |
| pathlib     | Cross-platform path handling               |
| logging     | Structured, file + console pipeline logs   |
| Git         | Version control                            |

---

## License

This project was created for educational / internship submission purposes.
