#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Iceberg snapshot operations demo for Spark 3.5.2 + Iceberg 1.8.1

Run it either way:
  1) python iceberg_snapshots_demo.py
  2) pyspark -f iceberg_snapshots_demo.py

It configures all needed settings programmatically (no --packages/--conf flags).
If you *do* prefer CLI flags, you can omit the builder's config for spark.jars.packages, etc.

Environment variables you can tweak:
  ICEBERG_VERSION   (default: 1.8.1)
  ICEBERG_WAREHOUSE (default: /tmp/iceberg_warehouse)
  ICEBERG_CATALOG   (default: local)
  ICEBERG_NAMESPACE (default: db)
  ICEBERG_TABLE     (default: events)
"""

import os
from pyspark.sql import SparkSession

ICEBERG_VERSION   = os.environ.get("ICEBERG_VERSION", "1.8.1")
WAREHOUSE         = os.environ.get("ICEBERG_WAREHOUSE", "/tmp/iceberg_warehouse")
CATALOG           = os.environ.get("ICEBERG_CATALOG", "local")
NAMESPACE         = os.environ.get("ICEBERG_NAMESPACE", "db")
TABLE             = os.environ.get("ICEBERG_TABLE", "events")
FULL_TABLE        = f"{CATALOG}.{NAMESPACE}.{TABLE}"

def build_spark():
    # Important: all these configs must be set BEFORE getOrCreate().
    # Using spark.jars.packages here is fine for a single-process script.
    # If you spin up multiple SparkSessions in one process (pytest, notebooks),
    # prefer PYSPARK_SUBMIT_ARGS or CLI flags to ensure JARs load in the first JVM.
    builder = (
        SparkSession.builder
        .appName("IcebergOpsDemo")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        .config("spark.jars.packages", f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}")
        # Keep logs readable
        .config("spark.ui.showConsoleProgress", "true")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    spark = build_spark()
    print(f"Spark version: {spark.version}")
    print(f"Using Iceberg runtime: {ICEBERG_VERSION}")
    print(f"Catalog: {CATALOG}, Namespace: {NAMESPACE}, Table: {TABLE}")
    print(f"Warehouse: {WAREHOUSE}")
    print("-" * 80)

    # Prep namespace/table
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE}")
    spark.sql(f"""
        CREATE TABLE {FULL_TABLE} (
          id BIGINT,
          company_id INT,
          created_at TIMESTAMP,
          write_operation INT,
          payload STRING
        )
        USING iceberg
        PARTITIONED BY (company_id)
    """)

    # 1) INSERT (append)
    spark.sql(f"""
        INSERT INTO {FULL_TABLE} VALUES
          (1, 10, TIMESTAMP '2025-08-01 00:00:00', 2, '{{"k":"a"}}'),
          (2, 10, TIMESTAMP '2025-08-02 00:00:00', 2, '{{"k":"b"}}'),
          (3, 20, TIMESTAMP '2025-08-03 00:00:00', 2, '{{"k":"c"}}')
    """)

    # 2) INSERT OVERWRITE (partition overwrite)
    spark.sql(f"""
        INSERT OVERWRITE {FULL_TABLE}
        SELECT id, company_id, created_at, write_operation, payload
        FROM {FULL_TABLE}
        WHERE company_id = 10
        UNION ALL
        SELECT 4, 10, TIMESTAMP '2025-08-04 00:00:00', 2, '{{"k":"new"}}'
    """)

    # 3) DELETE (row-level)
    spark.sql(f"DELETE FROM {FULL_TABLE} WHERE id = 3 AND company_id = 20")

    # 4) UPDATE (row-level)
    spark.sql(f"""
        UPDATE {FULL_TABLE}
        SET write_operation = 1
        WHERE company_id = 10 AND id = 4
    """)

    # 5) MERGE (row-level mix)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW src AS
        SELECT 2 AS id, 10 AS company_id, TIMESTAMP '2025-08-05 00:00:00' AS created_at, 1 AS write_operation, '{{"k":"merge-upd"}}' AS payload
        UNION ALL
        SELECT 5 AS id, 20 AS company_id, TIMESTAMP '2025-08-06 00:00:00' AS created_at, 1 AS write_operation, '{{"k":"merge-ins"}}' AS payload
    """)

    spark.sql(f"""
        MERGE INTO {FULL_TABLE} t
        USING src s
        ON t.id = s.id AND t.company_id = s.company_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print("\n=== Current table content ===")
    spark.sql(f"SELECT * FROM {FULL_TABLE} ORDER BY company_id, id").show(truncate=False)

    print("\n=== Snapshots (operation + helpful summary keys) ===")
    snap_df = spark.sql(f"""
        SELECT
          snapshot_id,
          parent_id,
          committed_at,
          operation,
          summary['deleted-data-files']    AS deleted_data_files,
          summary['added-data-files']      AS added_data_files,
          summary['added-delete-files']    AS added_delete_files,
          summary['removed-delete-files']  AS removed_delete_files
        FROM {FULL_TABLE}.snapshots
        ORDER BY committed_at
    """)
    snap_df.show(truncate=False)

    print("\n=== Entries (what changed per snapshot) ===")
    # entries has one row per file change with a status (ADDED/DELETED/EXISTING)
    entries_df = spark.sql(f"""
        SELECT snapshot_id, status, count(*) AS cnt
        FROM {FULL_TABLE}.entries
        GROUP BY snapshot_id, status
        ORDER BY snapshot_id, status
    """)
    entries_df.show(truncate=False)

    print("\nTip: In Spark + Iceberg, row-level DELETE/UPDATE/MERGE often commit as overwrite-style snapshots.")
    print("     Inspect 'operation' plus 'summary' and 'entries' to infer the exact row-level behavior.\n")

    spark.stop()

if __name__ == "__main__":
    main()
