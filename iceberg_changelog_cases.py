#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Comprehensive demo: Iceberg changelog view (with and without net_changes)
Target: Spark 3.5.2 + Iceberg 1.8.1

What this covers:
- Creates a table with identifier fields (id, company_id) so net_changes can collapse by key.
- Executes a sequence of commits to exercise cases:
  S1: INSERT (id=1, id=2)
  S2: UPDATE id=1
  S3: UPDATE id=1 again
  S4: DELETE id=1
  S5: RE-INSERT id=1
  S6: MERGE: UPDATE id=2, INSERT id=3
- Builds changelog views over various windows to demonstrate:
  1) Insert then delete -> net_changes yields nothing for that key
  2) Multiple updates -> net_changes yields exactly 1 DELETE + 1 INSERT for the key
  3) Insert then update -> net_changes yields a single INSERT (final state present)
  4) Re-insert after delete within window -> net_changes yields a single INSERT
  5) Cross-check raw (non-net) changes vs net_changes

Run:
  python iceberg_changelog_cases.py
or
  pyspark -f iceberg_changelog_cases.py
"""

import os
from pyspark.sql import SparkSession, functions as F

ICEBERG_VERSION   = os.environ.get("ICEBERG_VERSION", "1.8.1")
WAREHOUSE         = os.environ.get("ICEBERG_WAREHOUSE", "/tmp/iceberg_warehouse")
CATALOG           = os.environ.get("ICEBERG_CATALOG", "local")
NAMESPACE         = os.environ.get("ICEBERG_NAMESPACE", "db")
TABLE             = os.environ.get("ICEBERG_TABLE", "events_cdc_demo")
FULL_TABLE        = f"{CATALOG}.{NAMESPACE}.{TABLE}"
CLV_NAME          = "clv_demo"   # temp view must be single-part in Spark

def build_spark():
    builder = (
        SparkSession.builder
        .appName("IcebergChangelogCasesDemo")
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

def snapshot_ids(spark):
    return (
        spark.sql(f"SELECT snapshot_id, committed_at FROM {FULL_TABLE}.snapshots")
             .orderBy(F.col("committed_at").asc())
             .select("snapshot_id")
             .rdd.flatMap(lambda r: r)
             .collect()
    )


def current_snapshot_id(spark):
    return snapshot_ids(spark)[-1]


def snapshots(spark):
    print(f"\n=== current snapshots ===")
    spark.sql(f"""
        SELECT snapshot_id, parent_id, operation, committed_at FROM {FULL_TABLE}.snapshots
        ORDER BY committed_at
    """).show(truncate=False)


def current_rows(spark, msg):
    print(f"\n=== {msg} ===")
    spark.sql(f"""
      SELECT id, company_id, write_operation, payload, created_at
      FROM {FULL_TABLE}
      ORDER BY company_id, id
    """).show(truncate=False)

def create_changelog_view(spark, start_id, end_id, net_changes: bool):
    spark.sql(f"DROP VIEW IF EXISTS {CLV_NAME}")

    opts = {"end-snapshot-id": str(end_id)}
    if start_id is not None and str(start_id) != str(end_id):
        opts["start-snapshot-id"] = str(start_id)  # start is EXCLUSIVE
    opts_kv = ", ".join([f"'{k}', '{v}'" for k, v in opts.items()])

    spark.sql(f"""
      CALL {CATALOG}.system.create_changelog_view(
        table => '{NAMESPACE}.{TABLE}',
        changelog_view => '{CLV_NAME}',
        options => map({opts_kv}),
        net_changes => {"true" if net_changes else "false"}
      )
    """)


def show_changes(spark, title, where_clause="1=1"):
    """
      SELECT _commit_snapshot_id, _change_type, _change_ordinal,
         id, company_id, write_operation, payload
    """
    print(f"\n--- {title} ---")
    spark.sql(f"""
      SELECT *
      FROM {CLV_NAME}
      WHERE {where_clause}
      ORDER BY _commit_snapshot_id, _change_ordinal, company_id, id
    """).show(truncate=False)

def show_net_counts_by_key(spark, title):
    print(f"\n[Net counts by key] {title}")
    spark.sql(f"""
      SELECT id, company_id, _change_type, COUNT(*) AS n
      FROM {CLV_NAME}
      GROUP BY id, company_id, _change_type
      ORDER BY company_id, id, _change_type
    """).show(truncate=False)

def main():
    spark = build_spark()
    print(f"Spark: {spark.version} | Iceberg runtime: {ICEBERG_VERSION}")
    print(f"Catalog={CATALOG}  Namespace={NAMESPACE}  Table={TABLE}")
    print(f"Warehouse: {WAREHOUSE}")
    print("-" * 80)

    # Fresh table
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
      TBLPROPERTIES ('format-version'='2')
    """)

    # Set identifier fields: key for CDC net_changes collapsing
    # spark.sql(f"ALTER TABLE {FULL_TABLE} SET IDENTIFIER FIELDS id, company_id")

    # S1: INSERT 2 rows
    spark.sql(f"""
      INSERT INTO {FULL_TABLE} VALUES
        (1, 10, TIMESTAMP '2025-08-01 00:00:00', 2, '{{"k":"A"}}'),
        (2, 10, TIMESTAMP '2025-08-01 00:00:00', 2, '{{"k":"B"}}')
    """)
    s1 = current_snapshot_id(spark)
    current_rows(spark, f"After S1 INSERT (id=1,2 in company 10), snapshot_id {s1}")

    # S2: UPDATE id=1 (payload)
    spark.sql(f"""
      UPDATE {FULL_TABLE}
      SET payload = '{{"k":"A2"}}', write_operation = 1
      WHERE id = 1 AND company_id = 10
    """)
    s2 = current_snapshot_id(spark)
    current_rows(spark, f"After S2 UPDATE id=1 (SET payload = A2, write_operation = 1), snapshot_id {s2}")

    # S3: UPDATE id=1 again
    spark.sql(f"""
      UPDATE {FULL_TABLE}
      SET payload = '{{"k":"A3"}}'
      WHERE id = 1 AND company_id = 10
    """)
    s3 = current_snapshot_id(spark)
    current_rows(spark, f"After S3 UPDATE id=1 again (SET payload = A3), snapshot_id {s3}")

    # S4: DELETE id=1
    spark.sql(f"DELETE FROM {FULL_TABLE} WHERE id = 1 AND company_id = 10")
    s4 = current_snapshot_id(spark)
    current_rows(spark, f"After S4 DELETE id=1, snapshot_id {s4}")

    # S5: RE-INSERT id=1 (later value)
    spark.sql(f"""
      INSERT INTO {FULL_TABLE} VALUES
        (1, 10, TIMESTAMP '2025-08-10 00:00:00', 1, '{{"k":"A4"}}')
    """)
    s5 = current_snapshot_id(spark)
    current_rows(spark, f"After S5 RE-INSERT id=1, snapshot_id {s5}")


    # S6: MERGE to UPDATE id=2 and INSERT id=3
    spark.sql(f"""
      CREATE OR REPLACE TEMP VIEW src AS
      SELECT 2 AS id, 10 AS company_id, TIMESTAMP '2025-08-11 00:00:00' AS created_at, 1 AS write_operation, '{{"k":"B2"}}' AS payload
      UNION ALL
      SELECT 3 AS id, 10 AS company_id, TIMESTAMP '2025-08-12 00:00:00' AS created_at, 1 AS write_operation, '{{"k":"C1"}}' AS payload
    """)
    spark.sql(f"""
      MERGE INTO {FULL_TABLE} t
      USING src s
      ON t.id = s.id AND t.company_id = s.company_id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)
    s6 = current_snapshot_id(spark)
    current_rows(spark, "After S6 MERGE (id=2 updated to B2, id=3 inserted)")

    # List snapshots
    snapshots(spark)

    # Helper: windows = [(label, start, end)]
    windows = [
        ("W1 S1..S1 (just initial inserts)", s1, s1),
        ("W1.1 0..S1 (just initial inserts)", None, s1),
        ("W2 S1..S2 (insert then one update of id=1)", s1, s2),
        ("W2.1 0..S2 (insert then one update of id=1)", None, s2),
        ("W3 S1..S3 (insert then two updates of id=1)", s1, s3),
        ("W3.1 0..S3 (insert then two updates of id=1)", None, s3),
        ("W4 S1..S4 (insert then delete id=1)", s1, s4),
        ("W4.1 0..S4 (insert then delete id=1)", None, s4),

        ("W4 S1..S5 (insert, update, delete, re-insert id=1)", s1, s5),
        ("W4.1 0..S5 (insert, update, delete, re-insert id=1)", None, s5),

        ("W4 S1..S6 (insert, update, delete, re-insert id=1, update id=2, insert id=3)", s1, s6),
        ("W4.1 0..S6 (insert, update, delete, re-insert id=1, update id=2, insert id=3)", None, s6),



        ("W5 S2..S3 (two updates only)", s2, s3),
        ("W5.1 S2..S4 (two updates and delete)", s2, s4),
        ("W6 S3..S4 (update then delete)", s3, s4),
        ("W7 S4..S5 (delete then re-insert)", s4, s5),
        ("W8 S5..S6 (reinsert, then merge: update id=2 + insert id=3)", s5, s6),
        ("W9 S1..S6 (entire history)", s1, s6),



    ]

    # For each window, show non-net (raw) and net_changes views
    for label, st, en in windows:
        # RAW changes
        create_changelog_view(spark, st, en, net_changes=False)
        # show_changes(spark, f"{label} RAW", where_clause="company_id=10")
        show_changes(spark, f"{label} RAW")
        # NET changes
        create_changelog_view(spark, st, en, net_changes=True)
        # show_changes(spark, f"{label} NET (collapsed by identifier fields)", where_clause="company_id=10")
        show_changes(spark, f"{label} NET (collapsed by identifier fields)")
        show_net_counts_by_key(spark, f"{label} NET")

    print("\nLegend / Expectations:\n"
          "• Case 1 (insert then delete within window): NET should emit nothing for that key.\n"
          "• Case 2 (multiple updates within window): NET should emit a DELETE of the earliest version in-window and an INSERT of the final version (i.e., 1 DEL + 1 INS) for that key.\n"
          "• Case 3 (insert then update): NET should emit only 1 INSERT for that key (final version present).\n"
          "• Re-insert after delete within window: NET should emit a single INSERT for that key.\n"
          "• Compare RAW vs NET to see compression behavior driven by identifier fields.\n")

    spark.stop()

if __name__ == "__main__":
    main()
