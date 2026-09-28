# Bike Sales Data Lakehouse on Databricks

A learning / portfolio project: an end-to-end **medallion lakehouse** (bronze → silver → gold) on Databricks, built with PySpark, Spark SQL, Delta Lake and Unity Catalog. It takes raw CSV exports from two source systems (a CRM and an ERP), cleans and aligns them, and publishes a small star schema for sales analysis.

> **Credit & how this was built:** I learned the medallion architecture and the overall project design from
> **[Data With Baraa](https://www.youtube.com/@DataWithBaraa)**'s Databricks Data Lakehouse Project
> ([Databricks Bootcamp playlist](https://www.youtube.com/playlist?list=PLNcg_FV9n7qZoxVkw-KPhcmgLWjHWVUc9) ·
> [project roadmap](https://candle-gosling-511.notion.site/Project-Building-the-Bike-Data-Lakehouse-2e734b251f1280ab8dadc269e033cc38)).
> The datasets, project structure and base notebooks come from his public repo
> ([DataWithBaraa/databricks_bootcamp_2026](https://github.com/DataWithBaraa/databricks_bootcamp_2026)).
> My work on top of it: documenting every notebook, finding and fixing three data bugs that the original
> pipeline let through into gold, adding data quality checks that fail the run, a single end-to-end entry
> point, and a local runner to test it without Databricks. See [My changes](#my-changes). The commit history
> separates the imported baseline from my changes.
> The original project is MIT-licensed by Baraa Khatib Salkini ([license](https://github.com/DataWithBaraa/databricks_bootcamp_2026/blob/main/LICENSE)).

---

## Architecture

```
CSV files (Unity Catalog volume: workspace.bronze.raw_sources)
        │
        ▼
🥉 Bronze  – raw CSVs loaded as-is into Delta tables (6 tables)
        │
        ▼
🥈 Silver  – trimmed, standardized, type-cast, renamed (6 tables)
        │
        ▼
🥇 Gold    – star schema: dim_customers, dim_products, fact_sales
        │
        ▼
✅ Quality checks – fail the run if keys, row counts or amounts are wrong
```

All tables live in the `workspace` catalog under the `bronze`, `silver` and `gold` schemas.

| Source file | Bronze table | Silver table | Used in gold |
|---|---|---|---|
| `source_crm/cust_info.csv` | `crm_cust_info` | `crm_customers` | `dim_customers` |
| `source_crm/prd_info.csv` | `crm_prd_info` | `crm_products` | `dim_products` |
| `source_crm/sales_details.csv` | `crm_sales_details` | `crm_sales` | `fact_sales` |
| `source_erp/CUST_AZ12.csv` | `erp_cust_az12` | `erp_customers` | `dim_customers` |
| `source_erp/LOC_A101.csv` | `erp_loc_a101` | `erp_customer_location` | `dim_customers` |
| `source_erp/PX_CAT_G1V2.csv` | `erp_px_cat_g1v2` | `erp_product_category` | `dim_products` |

---

## What's here

```
datasets/
  engineering/            # raw inputs for the pipeline
    source_crm/           # cust_info, prd_info, sales_details  (~18.5k customers, ~400 product rows, ~60k sales lines)
    source_erp/           # CUST_AZ12 (birthdate/gender), LOC_A101 (country), PX_CAT_G1V2 (categories)
  analysts/               # not used by the pipeline (see note below)
    sales_dataset/        # prebuilt dim_customers / dim_products / fact_sales CSVs
    hr_dataset/           # semicolon-delimited HR dataset
script/
  init_lakehouse          # creates bronze/silver/gold schemas and the raw_sources volume
  run_pipeline            # end-to-end entry point: bronze → silver → gold (+ quality checks)
  bronze/
    bronze_layer(basic)       # one cell per file
    bronze_layer_(improved)   # config-driven loop over INGESTION_CONFIG, explicit Delta format
  silver/
    crm/  silver_crm_cust_info, silver_crm_prd_info, silver_crm_sales_details
    erp/  silver_erp_cust_az12, silver_erp_loc_a101, silver_erp_px_cat_g1v2
    silver_orchestration      # runs all six silver notebooks via dbutils.notebook.run
  gold/
    gold_dim_customers, gold_dim_products, gold_fact_sales
    gold_quality_checks       # data quality checks; raises if any fail
    gold_orchestration        # runs the gold notebooks in dependency order, then the checks
tests/
  run_local.py            # runs the notebooks locally with PySpark (no Databricks needed)
```

**About SQL:** there are no standalone `.sql` files. Spark SQL is embedded in the notebooks: DDL in `init_lakehouse`, the join and surrogate-key queries in the three gold notebooks, the check queries in `gold_quality_checks`, and `SELECT … LIMIT 10` previews after each write.

**About `datasets/analysts/`:** these files come with the original project and aren't read by any notebook here.

---

## How to run

1. In a Databricks workspace with a `workspace` catalog, run `script/init_lakehouse`.
2. Upload the CSVs from `datasets/engineering/` to
   `/Volumes/workspace/bronze/raw_sources/source_crm/` and `.../source_erp/`.
3. Run `script/run_pipeline`. It runs bronze, silver and gold in order and fails if a quality check fails.
   (Each layer can also be run on its own through its notebook or orchestration notebook.)

Every step is a full `overwrite`, with no incremental loading.

**Locally, without Databricks** (Java 17+):

```bash
pip install "pyspark>=4.0,<4.1"
python tests/run_local.py
```

This runs `init_lakehouse` and `run_pipeline` against the CSVs in `datasets/engineering/`, with the storage swapped (parquet instead of Delta, a temporary local warehouse instead of Unity Catalog). The notebook code itself runs unchanged.

---

## What the pipeline does

- **Config-driven bronze ingestion:** one loop over an `INGESTION_CONFIG` list, so adding a source is a config change (`bronze_layer(basic)` is the earlier one-cell-per-file version).
- **Generic silver cleanup:** every string column is trimmed by inspecting the schema, then renamed to business-friendly names through a `RENAME_MAP` dict.
- **Code → label normalization:** marital status, gender, product line, country codes, and the maintenance flag (converted to a real boolean).
- **Date repair:** CRM sales dates arrive as `yyyyMMdd` integers; zeros and malformed values become `NULL`, the rest are parsed to dates. ERP birthdates in the future are nulled.
- **Sales and price repair:** negative prices are sign-corrected, missing prices are derived from `sales / quantity`, and invalid or inconsistent sales amounts are recomputed as `quantity × price`.
- **Product versioning:** product end dates are derived from the version history, and `dim_products` holds the current version of each product.
- **Cross-system key alignment:** ERP customer IDs are stripped of the `NAS` prefix and of dashes so they join to the CRM `customer_number`; the CRM product key is split into a category ID and product number to join to the ERP category table.
- **Gold star schema:** surrogate keys via `ROW_NUMBER()`, gender taken from CRM with ERP as fallback, one row per customer, and `fact_sales` keyed on the dimension surrogate keys.
- **Notebook orchestration** with `dbutils.notebook.run`: per layer, plus `run_pipeline` for the whole thing.

## My changes

Running the original pipeline end to end showed that gold didn't match the source data. I traced each issue back to silver, fixed it there, and added checks so it can't return silently. One commit per fix:

| Issue in the original pipeline | Effect in gold | Fix |
|---|---|---|
| 5 customer IDs appear 2–3 times in the CRM (an early, mostly empty record, then a complete one) | `dim_customers` had 18,490 rows for 18,484 customers | Keep the latest record per ID with a `row_number()` window |
| Every product end date is earlier than its start date, and the current-version filter was commented out | `dim_products` held all 397 versions of 295 products, so a sale matched every version: **`fact_sales` had 89,833 rows for 60,398 sales lines (+49%)** | Derive `end_date` as the next version's start − 1 day with `lead()`, and keep only current versions in gold |
| Only missing prices were repaired; negative prices and bad sales amounts passed through | 24 fact rows with `sales_amount ≠ quantity × price`, including zero and negative sales | Sign-correct prices, then recompute sales when it's missing, non-positive or inconsistent |
| No checks beyond `LIMIT 10` previews, and bronze wasn't in any orchestration | The issues above went unnoticed | `gold_quality_checks` (7 checks, raises on failure) and `run_pipeline` as the single entry point |

After the fixes, gold has 18,484 customers, 295 products and 60,398 sales rows. Every sale matches a product and a customer, and all seven checks pass. Run against the original notebooks, four of the checks fail, catching all three bugs.

I also added a header and comments to every notebook explaining what each step does and why, and `tests/run_local.py` so all of this can be reproduced on a laptop.

**Known gaps / next steps:**
- Every run is a full overwrite; there's no incremental loading (e.g. `MERGE` into Delta).
- Surrogate keys are regenerated on each rebuild, so they're not stable across runs.
- `dim_products` keeps only current versions, so older sales show today's product attributes. A full SCD Type 2 join on the order date would keep history.
- Money columns are `DOUBLE`; `DECIMAL` would be the better type.
- No Databricks Job / Asset Bundle definition is checked in. `run_pipeline` is written to be that Job's single task.

---

## Tech

Databricks · Apache Spark (PySpark, Spark SQL) · Delta Lake · Unity Catalog
