# Databricks Medallion Lakehouse Project (with PySpark/SQL Practice)

This repository combines:

1. A Databricks medallion-architecture lakehouse project (Bronze → Silver → Gold), and  
2. My PySpark/SQL practice solutions used to strengthen transformation and analytics engineering skills.

It started from learning materials by Data with Baraa, then expanded with my own implementation and practice library.

---

## Learning Sources / References

- Data with Baraa:
  - Video 1: https://www.youtube.com/watch?v=sxYcjDrMyNc
  - Video 2: https://www.youtube.com/watch?v=ldBLOasG23w
- Notion notes:
  - https://candle-gosling-511.notion.site/Databricks-Bootcamp-2e734b251f1280208697c641df833373
- Related resources I used for benchmarking:
  - DataExpert.io Intermediate Bootcamp
  - Databricks videos by Ansh Lamba

---

## Project Purpose

- Rebuild and internalize practical OLAP/lakehouse data engineering patterns.
- Practice production-style layered modeling with Databricks + Spark SQL.
- Build confidence before extending the same concepts into larger batch/streaming pipeline projects.

---

## What’s in this repo

### 1) Databricks Lakehouse Project (`bike_lakehouse_2026/`)
Implements a medallion architecture:

- **Bronze:** raw ingestion layer  
- **Silver:** cleaned/standardized domain transformations  
  - CRM: customers, products, sales
  - ERP: customers, customer location, product category
- **Gold:** analytics-ready dimensional model
  - `gold_dim_customers`
  - `gold_dim_products`
  - `gold_fact_sales`

Includes orchestration notebooks for Silver and Gold execution.

### 2) PySpark Practice (`spark_questions/`)
Curated solutions from:
- [Spark Playground](https://www.sparkplayground.com/)
- [Zillacode](https://zillacode.com/home)
- [Strata Scratch](https://www.stratascratch.com/)

### 3) SQL Practice
SQL practice solutions ([DataExpert.io SQL Questions](https://www.dataexpert.io/questions)).

---

## Architecture (High-level)

Sources / raw files  
→ Bronze (raw tables)  
→ Silver (domain cleaning + standardization)  
→ Gold (fact/dim models for BI & analytics)

---

## How to Run (Databricks)

1. Open and run `bike_lakehouse_2026/init_lakehouse.ipynb`  
   - Sets catalog, schemas (`bronze`, `silver`, `gold`), and volume.
2. Run Bronze ingestion notebook(s).
3. Run `bike_lakehouse_2026/silver/silver_orchestration.ipynb`.
4. Run `bike_lakehouse_2026/gold/gold_orchestration.ipynb`.
5. Validate output tables in Gold.

---

## Data Quality / Validation (Suggested checks)

- Null checks on key columns
- Duplicate checks on dimension keys
- Row-count reconciliation between layers
- Referential integrity between fact and dimensions

---

## Why this project matters

- Demonstrates practical Databricks medallion implementation.
- Shows hands-on Spark SQL transformation patterns.
- Reflects disciplined practice through high-volume PySpark/SQL exercises.

---

## Next Improvements

- Add automated data quality tests.
- Add CI checks for notebook/script quality.
- Add architecture diagram + sample output screenshots.
- Add performance notes (partitioning, optimization, runtime observations).