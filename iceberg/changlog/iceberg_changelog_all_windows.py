#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Comprehensive Iceberg changelog_view demo (S0..S6) with all window combinations + pivot

Spark: 3.5.2
Iceberg: 1.8.1
Table: v2, identifier fields = (id, company_id)

Commit sequence (each produces snapshot S0..S6 in order):
  S0: INSERT id=98, id=99                -- seed rows (earliest)
  S1: INSERT id=1, id=2                  -- initial main rows
  S2: UPDATE id=1 (payload A -> A2)      -- row-level update
  S3: UPDATE id=1 again (A2 -> A3)       -- another update
  S4: DELETE id=1                        -- delete
  S5: INSERT id=1 (A4)                   -- re-insert id=1
  S6: MERGE: UPDATE id=2 (B -> B2), INSERT id=3 (C1)

Windows to evaluate (start is EXCLUSIVE, end is INCLUSIVE):
  • (beginning, sK]  for K in {S0..S6}    -- start omitted -> first..end
  • (sI, sJ]          for all 0 <= I < J <= 6  -- all valid pairs

For each window we print:
  1) RAW changes by _change_type
  2) NET changes by _change_type (identifier-fields collapse)
  3) NET counts per key (id, company_id)
At the end we also print a PIVOT (matrix) of counts: rows = windows, columns = change types.

Env toggles:
  PRINT_ROWS=1       → also print full rows for each window (verbose)
  WHERE_COMPANY=10   → restrict output to a specific company_id (default 10)
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
PRINT_ROWS        = os.environ.get("PRINT_ROWS", "1") == "1"
WHERE_COMPANY     = int(os.environ.get("WHERE_COMPANY", "10"))  # filter to keep output small

def build_spark():
    builder = (
        SparkSession.builder
        .appName("IcebergChangelogAllWindows")
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

def snapshot_pairs(spark):
    snaps = (
        spark.sql(f"SELECT snapshot_id, committed_at FROM {FULL_TABLE}.snapshots")
             .orderBy(F.col("committed_at").asc())
             .select("snapshot_id")
             .rdd.flatMap(lambda r: r)
             .map(int)
             .collect()
    )
    labels = [f"S{i}" for i in range(len(snaps))]
    mapping = dict(zip(labels, snaps))
    return mapping  # e.g., {'S0': 123, 'S1': 456, ...}

def current_rows(spark, msg):
    print()
    print(f"=== {msg} ===")
    spark.sql(f"""
      SELECT id, company_id, write_operation, payload, created_at
      FROM {FULL_TABLE}
      WHERE company_id = {WHERE_COMPANY}
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

def summarize_changes_collect_df(spark, window_title, mode_label):
    # Return a DF with columns: window, mode, _change_type, n
    df = spark.sql(f"""
      SELECT '{window_title}' AS window,
             '{mode_label}'  AS mode,
             _change_type, COUNT(*) AS n
      FROM {CLV_NAME}
      WHERE company_id = {WHERE_COMPANY}
      GROUP BY _change_type
    """)
    return df

def summarize_changes_print(spark, title):
    print()
    print(f"--- {title} (company_id={WHERE_COMPANY}) ---")
    # spark.sql(f"""
    #   SELECT _change_type, COUNT(*) AS n
    #   FROM {CLV_NAME}
    #   WHERE company_id = {WHERE_COMPANY}
    #   GROUP BY _change_type
    #   ORDER BY _change_type
    # """).show(truncate=False)
    #
    # print("[NET by key] id,company_id → counts")
    # spark.sql(f"""
    #   SELECT id, company_id, _change_type, COUNT(*) AS n
    #   FROM {CLV_NAME}
    #   WHERE company_id = {WHERE_COMPANY}
    #   GROUP BY id, company_id, _change_type
    #   ORDER BY company_id, id, _change_type
    # """).show(truncate=False)

    if PRINT_ROWS:
        print("[Rows]")
        spark.sql(f"""
          SELECT *
          FROM {CLV_NAME}
          WHERE company_id = {WHERE_COMPANY}
          ORDER BY _commit_snapshot_id, _change_ordinal, company_id, id
        """).show(truncate=False)

def build_table_and_commits(spark):
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
    # Identifier fields must be NOT NULL
    # spark.sql(f"ALTER TABLE {FULL_TABLE} ALTER COLUMN id SET NOT NULL")
    # spark.sql(f"ALTER TABLE {FULL_TABLE} ALTER COLUMN company_id SET NOT NULL")
    # spark.sql(f"ALTER TABLE {FULL_TABLE} SET IDENTIFIER FIELDS id, company_id")

    # ---- S0: seed rows (id=98,99) ----
    spark.sql(f"""
      INSERT INTO {FULL_TABLE} VALUES
        (98, {WHERE_COMPANY}, TIMESTAMP '2025-07-31 00:00:00', 2, '{{"k":"Z98"}}'),
        (99, {WHERE_COMPANY}, TIMESTAMP '2025-07-31 00:00:01', 2, '{{"k":"Z99"}}')
    """)
    current_rows(spark, "After S0 INSERT (id=98,99)")

    # ---- S1: initial rows ----
    spark.sql(f"""
      INSERT INTO {FULL_TABLE} VALUES
        (1, {WHERE_COMPANY}, TIMESTAMP '2025-08-01 00:00:00', 2, '{{"k":"A"}}'),
        (2, {WHERE_COMPANY}, TIMESTAMP '2025-08-01 00:00:00', 2, '{{"k":"B"}}')
    """)
    current_rows(spark, "After S1 INSERT (id=1,2)")

    # ---- S2: update id=1 ----
    spark.sql(f"""
      UPDATE {FULL_TABLE}
      SET payload = '{{"k":"A2"}}', write_operation = 1
      WHERE id = 1 AND company_id = {WHERE_COMPANY}
    """)

    # ---- S3: update id=1 again ----
    spark.sql(f"""
      UPDATE {FULL_TABLE}
      SET payload = '{{"k":"A3"}}'
      WHERE id = 1 AND company_id = {WHERE_COMPANY}
    """)

    # ---- S4: delete id=1 ----
    spark.sql(f"DELETE FROM {FULL_TABLE} WHERE id = 1 AND company_id = {WHERE_COMPANY}")

    # ---- S5: re-insert id=1 ----
    spark.sql(f"""
      INSERT INTO {FULL_TABLE} VALUES
        (1, {WHERE_COMPANY}, TIMESTAMP '2025-08-10 00:00:00', 1, '{{"k":"A4"}}')
    """)

    # ---- S6: MERGE update id=2 and insert id=3 ----
    spark.sql(f"""
      CREATE OR REPLACE TEMP VIEW src AS
      SELECT 2 AS id, {WHERE_COMPANY} AS company_id, TIMESTAMP '2025-08-11 00:00:00' AS created_at, 1 AS write_operation, '{{"k":"B2"}}' AS payload
      UNION ALL
      SELECT 3 AS id, {WHERE_COMPANY} AS company_id, TIMESTAMP '2025-08-12 00:00:00' AS created_at, 1 AS write_operation, '{{"k":"C1"}}' AS payload
    """)
    spark.sql(f"""
      MERGE INTO {FULL_TABLE} t
      USING src s
      ON t.id = s.id AND t.company_id = s.company_id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)
    current_rows(spark, "After S6 MERGE (id=2->B2, id=3 inserted)")

def main():
    spark = build_spark()
    print(f"Spark: {spark.version} | Iceberg runtime: {ICEBERG_VERSION}")
    print(f"Catalog={CATALOG}  Namespace={NAMESPACE}  Table={TABLE}")
    print(f"Warehouse: {WAREHOUSE}")
    print("-" * 80)

    build_table_and_commits(spark)

    # Map S0..S6 to ids
    mapping = snapshot_pairs(spark)
    if len(mapping) != 7:
        print("ERROR: expected 7 snapshots (S0..S6), got:", mapping)
        spark.stop()
        return

    print("Snapshot order (by committed_at):")
    for lbl in sorted(mapping.keys(), key=lambda x: int(x[1:])):
        print(f"  {lbl} → {mapping[lbl]}")

    # Build window list:
    windows = []
    labels = [f"S{i}" for i in range(7)]  # S0..S6
    # (beginning, sK]
    for K in labels:
        windows.append((f"(beginning, {K}]", None, mapping[K]))
    # (sI, sJ]
    for i in range(len(labels)):
        for j in range(i+1, len(labels)):
            Si, Sj = labels[i], labels[j]
            windows.append((f"({Si}, {Sj}]", mapping[Si], mapping[Sj]))

    # Collect summaries for pivot
    summary_rows = []

    # Evaluate all windows
    for title, start_id, end_id in windows:
        print("=================================================================================")
        print(f"Window: {title}  → start={start_id} (exclusive), end={end_id} (inclusive)")

        # RAW
        create_changelog_view(spark, start_id, end_id, net_changes=False)
        summarize_changes_print(spark, f"{title} RAW")
        summary_rows.append(summarize_changes_collect_df(spark, title, "RAW"))

        # NET
        create_changelog_view(spark, start_id, end_id, net_changes=True)
        summarize_changes_print(spark, f"{title} NET (identifier-fields collapse)")
        summary_rows.append(summarize_changes_collect_df(spark, title, "NET"))

    # Build and show pivot (matrix of counts by window x change type), for RAW and NET
    if summary_rows:
        summary_df = summary_rows[0]
        for df in summary_rows[1:]:
            summary_df = summary_df.unionByName(df)

        print()
        print("=== PIVOT SUMMARY (rows = window + mode, cols = change types) ===")
        pivot = (
            summary_df
            .groupBy("window", "mode")
            .pivot("_change_type", ["INSERT", "DELETE", "UPDATE_BEFORE", "UPDATE_AFTER"])
            .sum("n")
            .na.fill(0)
            .orderBy("window", "mode")
        )
        pivot.show(500, truncate=False)

    print()
    print("Notes on expectations for NET (using identifier fields [id, company_id]):")
    print("• Insert → Delete within the window ⇒ no output for that key.")
    print("• Multiple Updates within the window ⇒ collapses to 1 DELETE (earliest image) + 1 INSERT (final image).")
    print("• Insert → Update(s) (still present) ⇒ 1 INSERT (final image).")
    print("• Delete → Re-insert within the window ⇒ 1 INSERT (final image).")
    print("• MERGE that updates some keys and inserts new ones ⇒ affected keys show as INSERT (new/updated) and/or DELETE where applicable.")

    spark.stop()

if __name__ == "__main__":
    main()
