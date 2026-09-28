"""Run the Databricks notebooks locally with PySpark, no workspace needed.

Only the environment is swapped; the notebook code runs as written:
  - /Volumes/workspace/bronze/raw_sources  -> datasets/engineering/
  - workspace.<schema>.<table>             -> <schema>.<table> in a temporary local warehouse
  - Delta                                  -> parquet
  - Unity Catalog DDL (catalog/schema/volume) is skipped; schemas are created up front
  - display() -> show(), dbutils.notebook.run() -> runs the child notebook in-process

Requires Java 17+ and PySpark 4.0 (matching Databricks serverless):
    pip install "pyspark>=4.0,<4.1"

Usage (from the repo root):
    python tests/run_local.py                  # init_lakehouse + run_pipeline, incl. quality checks
    python tests/run_local.py gold/gold_fact_sales ...   # specific notebooks, paths relative to script/
"""
import json
import os
import re
import sys
import tempfile

from pyspark.sql import DataFrame, SparkSession

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_DIR = os.path.join(REPO, "script")

spark = (
    SparkSession.builder.master("local[2]")
    .config("spark.sql.warehouse.dir", tempfile.mkdtemp(prefix="lakehouse_wh_"))
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
for schema in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")

DataFrame.display = lambda self, *args, **kwargs: self.show(5, truncate=40)

UC_DDL = re.compile(r"(?is)^(USE CATALOG|CREATE (CATALOG|VOLUME|SCHEMA))")


def localize(code):
    code = code.replace("/Volumes/workspace/bronze/raw_sources", os.path.join(REPO, "datasets/engineering"))
    code = re.sub(r"\bworkspace\.(bronze|silver|gold)\.", r"\1.", code)
    return code.replace('.format("delta")', '.format("parquet")')


class _Notebook:
    def __init__(self, base_dir):
        self.base_dir = base_dir

    def run(self, path, timeout_seconds=0, arguments=None):
        run_notebook(os.path.normpath(os.path.join(self.base_dir, path)))
        return ""


class _DBUtils:
    def __init__(self, base_dir):
        self.notebook = _Notebook(base_dir)


def run_sql_cell(code):
    body = "\n".join(line for line in code.splitlines() if not line.strip().startswith("--"))
    for statement in (s.strip() for s in localize(body).split(";")):
        if statement and not UC_DDL.match(statement):
            spark.sql(statement).show(3, truncate=30)


def run_notebook(path):
    print(f"\n===== {os.path.relpath(path, SCRIPT_DIR)}")
    with open(path + ".ipynb") as f:
        cells = json.load(f)["cells"]
    namespace = {"spark": spark, "dbutils": _DBUtils(os.path.dirname(path))}
    for cell in cells:
        if cell["cell_type"] != "code":
            continue
        code = "".join(cell["source"]).lstrip()
        if code.startswith("%sql"):
            run_sql_cell(code[len("%sql"):])
            continue
        code = re.sub(r"^%python\s*\n", "", code)
        exec(compile(localize(code), path, "exec"), namespace)


if __name__ == "__main__":
    notebooks = sys.argv[1:] or ["init_lakehouse", "run_pipeline"]
    for nb in notebooks:
        run_notebook(os.path.join(SCRIPT_DIR, nb))
    print("\nAll notebooks completed. Gold row counts:")
    for table in ("dim_customers", "dim_products", "fact_sales"):
        if spark.catalog.tableExists(f"gold.{table}"):
            print(f"  {table}: {spark.table(f'gold.{table}').count():,}")
