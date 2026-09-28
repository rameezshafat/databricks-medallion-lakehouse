# Bike Sales Data Lakehouse on Databricks

A learning / portfolio project: an end-to-end **medallion lakehouse** (bronze → silver → gold) on Databricks, built with PySpark, Spark SQL, Delta Lake and Unity Catalog. It takes raw CSV exports from two source systems (a CRM and an ERP), cleans and aligns them, and publishes a small star schema for sales analysis.

> **Credit & how this was built:** I learned the medallion architecture and the overall project design from
> **[Data With Baraa](https://www.youtube.com/@DataWithBaraa)**'s Databricks Data Lakehouse Project
> ([Databricks Bootcamp playlist](https://www.youtube.com/playlist?list=PLNcg_FV9n7qZoxVkw-KPhcmgLWjHWVUc9) ·
> [project roadmap](https://candle-gosling-511.notion.site/Project-Building-the-Bike-Data-Lakehouse-2e734b251f1280ab8dadc269e033cc38)).
> The datasets, project structure and base notebooks come from his public repo
> ([DataWithBaraa/databricks_bootcamp_2026](https://github.com/DataWithBaraa/databricks_bootcamp_2026)).
> My work on top of it: documenting every notebook, finding and fixing data bugs in the pipeline,
> and adding data-quality checks. The commit history separates the imported baseline from my changes.
> The original project is MIT-licensed by Baraa Khatib Salkini. See [LICENSE](LICENSE).

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
  bronze/
    bronze_layer(basic)       # one cell per file
    bronze_layer_(improved)   # config-driven loop over INGESTION_CONFIG, explicit Delta format
  silver/
    crm/  silver_crm_cust_info, silver_crm_prd_info, silver_crm_sales_details
    erp/  silver_erp_cust_az12, silver_erp_loc_a101, silver_erp_px_cat_g1v2
    silver_orchestration      # runs all six silver notebooks via dbutils.notebook.run
  gold/
    gold_dim_customers, gold_dim_products, gold_fact_sales
    gold_orchestration        # runs the three gold notebooks in dependency order
```

**About SQL:** there are no standalone `.sql` files. Spark SQL is embedded in the notebooks: DDL in `init_lakehouse`, the join and surrogate-key queries in the three gold notebooks, and `SELECT … LIMIT 10` sanity checks after each write.

**About `datasets/analysts/`:** these files come with the original project and aren't read by any notebook here.

---

## How to run

1. In a Databricks workspace with a `workspace` catalog, run `script/init_lakehouse`.
2. Upload the CSVs from `datasets/engineering/` to
   `/Volumes/workspace/bronze/raw_sources/source_crm/` and `.../source_erp/`.
3. Run `script/bronze/bronze_layer_(improved)`.
4. Run `script/silver/silver_orchestration`.
5. Run `script/gold/gold_orchestration`.

There is no Databricks Job definition checked in; the two orchestration notebooks are meant to be the entry points for one. Every step is a full `overwrite`, with no incremental loading.

---

## What the pipeline does

- **Config-driven bronze ingestion:** one loop over an `INGESTION_CONFIG` list, so adding a source is a config change (`bronze_layer(basic)` is the earlier one-cell-per-file version).
- **Generic silver cleanup:** every string column is trimmed by inspecting the schema, then renamed to business-friendly names through a `RENAME_MAP` dict.
- **Code → label normalization:** marital status, gender, product line, country codes, and the maintenance flag (converted to a real boolean).
- **Date repair:** CRM sales dates arrive as `yyyyMMdd` integers; zeros and malformed values become `NULL`, the rest are parsed to dates. ERP birthdates in the future are nulled.
- **Price repair:** missing or non-positive prices are derived from `sales / quantity`.
- **Cross-system key alignment:** ERP customer IDs are stripped of the `NAS` prefix and of dashes so they join to the CRM `customer_number`; the CRM product key is split into a category ID and product number to join to the ERP category table.
- **Gold star schema:** surrogate keys via `ROW_NUMBER()`, gender taken from CRM with ERP as fallback, and `fact_sales` keyed on the dimension surrogate keys.
- **Notebook orchestration** for silver and gold with `dbutils.notebook.run`.

**Known gaps / next steps:**
- Customer IDs aren't de-duplicated in silver (the source has a handful of duplicates), so `dim_customers` can contain duplicate rows.
- `dim_products` keeps historical product versions (the current-only filter is commented out) and `end_date` isn't derived. Because `fact_sales` joins on `product_number`, sales for products with multiple versions can match more than one product row.
- `sales_amount` isn't recalculated when it's inconsistent with `quantity × price`; only `price` is repaired.
- No automated tests or data-quality checks beyond `LIMIT 10` previews; bronze isn't wired into an orchestration notebook.

---

## Tech

Databricks · Apache Spark (PySpark, Spark SQL) · Delta Lake · Unity Catalog

## License

MIT. See [LICENSE](LICENSE). Original copyright © 2026 Baraa Khatib Salkini.
