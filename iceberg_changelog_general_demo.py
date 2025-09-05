#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Demo: Iceberg changelog view with ONLY end-snapshot-id (no start)
Tested with Spark 3.5.2 + Iceberg 1.8.1

How to run:
  python iceberg_changelog_end_only.py
or
  pyspark -f iceberg_changelog_end_only.py

What it shows:
  * Creates a tiny table and makes two commits.
  * Grabs snapshot IDs after the 1st and 2nd commits.
  * Creates a changelog view using ONLY end-snapshot-id (no start supplied).
    Omitting start means the view covers [first_snapshot .. end_snapshot] (both inclusive).
  * Queries the changelog view to show CDC rows grouped by _commit_snapshot_id and _change_type.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ICEBERG_VERSION   = os.environ.get("ICEBERG_VERSION", "1.8.1")
WAREHOUSE         = os.environ.get("ICEBERG_WAREHOUSE", "/tmp/iceberg_warehouse")
CATALOG           = os.environ.get("ICEBERG_CATALOG", "local")
NAMESPACE         = os.environ.get("ICEBERG_NAMESPACE", "db")
TABLE             = os.environ.get("ICEBERG_TABLE", "events")
FULL_TABLE        = f"{CATALOG}.{NAMESPACE}.{TABLE}"
VIEW_NAME         = f"{TABLE}_clv"

def build_spark():
    builder = (
        SparkSession.builder
        .appName("IcebergChangelogEndOnlyDemo")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        .config("spark.jars.packages", f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}")
        .config("spark.ui.showConsoleProgress", "true")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def first_value(df, col):
    row = df.select(col).limit(1).collect()
    return row[0][0] if row else None

def last_value(df, col):
    row = df.select(col).orderBy(col).collect()
    return row[-1][0] if row else None

def main():
    spark = build_spark()
    print(f"Spark version: {spark.version}, Iceberg runtime: {ICEBERG_VERSION}")
    print(f"Catalog={CATALOG}  Namespace={NAMESPACE}  Table={TABLE}")
    print(f"Warehouse: {WAREHOUSE}")
    print("-" * 80)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    spark.sql(f"DROP VIEW IF EXISTS {VIEW_NAME}")
    spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE}")

    # Create table
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

    # Commit #1: initial insert (3 rows)
    spark.sql(f"""
        INSERT INTO {FULL_TABLE} VALUES
          (1, 10, TIMESTAMP '2025-08-01 00:00:00', 2, '{{"k":"a"}}'),
          (2, 10, TIMESTAMP '2025-08-02 00:00:00', 2, '{{"k":"b"}}'),
          (3, 20, TIMESTAMP '2025-08-03 00:00:00', 2, '{{"k":"c"}}')
    """)
    snaps_after_1 = spark.sql(f"SELECT snapshot_id, committed_at FROM {FULL_TABLE}.snapshots ORDER BY committed_at")
    snaps_after_1.show(truncate=False)
    first_snapshot = first_value(snaps_after_1.orderBy(F.col("committed_at").asc()), "snapshot_id")
    end_snapshot_1 = first_value(snaps_after_1.orderBy(F.col("committed_at").desc()), "snapshot_id")  # same as first_snapshot here

    print(f"\nAfter first commit: first_snapshot={first_snapshot}, end_snapshot_1={end_snapshot_1}")

    # Commit #2: update id=1 twice
    spark.sql(f"update {FULL_TABLE} set payload='1st update' where id = 1")
    spark.sql(f"update {FULL_TABLE} set payload='2nd update' where id = 1")
    snaps_after_2 = spark.sql(f"SELECT snapshot_id, committed_at FROM {FULL_TABLE}.snapshots ORDER BY committed_at")
    snaps_after_2.show(truncate=False)
    end_snapshot_2 = first_value(snaps_after_2.orderBy(F.col("committed_at").desc()), "snapshot_id")

    print(f"\nAfter second commit: end_snapshot_2={end_snapshot_2}")

    # --- Create changelog view using ONLY end-snapshot-id = end_snapshot_1 ---
    print(f"\nCreating changelog view with ONLY end-snapshot-id = {end_snapshot_2} ...")
    spark.sql(f"DROP VIEW IF EXISTS {VIEW_NAME}")
    spark.sql(f"""
        CALL {CATALOG}.system.create_changelog_view(
          table => '{NAMESPACE}.{TABLE}',
          options => map('end-snapshot-id', '{end_snapshot_2}'),
          changelog_view => '{VIEW_NAME}',
          net_changes => true
        )
    """)

    print("\n=== Changelog (end = first snapshot only) ===")
    spark.sql(f"""
      SELECT * FROM {VIEW_NAME}
      order by _change_ordinal desc, _change_type desc
    """).show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
