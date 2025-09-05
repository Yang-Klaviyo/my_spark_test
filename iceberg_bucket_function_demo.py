from datetime import datetime

from pyspark.sql import SparkSession, functions as f, types as T

bucket_size = 512

spark = (
    SparkSession.builder.master("local[*]")
    .appName("IcebergBucketTest")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1")
    # .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.4.3")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "hadoop")
    .config("spark.sql.catalog.iceberg.warehouse", "/tmp/iceberg_warehouse")
    .getOrCreate()
)


# spark.sql("SELECT iceberg.months(TIMESTAMP '2025-08-01 12:00:00') AS m").show()




schema = T.StructType([
    T.StructField("id", T.LongType(), True),
    T.StructField("company_id", T.StringType(), True),
    T.StructField("timestamp", T.TimestampType(), True),
])

rows = [
    (1, "acme", datetime(2025, 8, 1, 12, 0, 0)),
    (2, "acme", datetime(2025, 8, 15, 9, 10, 11)),
    (3, "globex", datetime(2025, 7, 31, 23, 59, 59)),
]

fqtn = "iceberg.demo.my_events_by_company_v1"

df = spark.createDataFrame(rows, schema=schema)
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.demo")
spark.sql(f"DROP TABLE IF EXISTS {fqtn}")
df.writeTo(fqtn).using("iceberg").create()




# f.expr("iceberg.bucket(512, `company_id`)")
# f.expr("iceberg.months(`timestamp`)")

# spark.sql("show functions like 'iceberg.%'").show(200, False)
# spark.sql("show functions like 'months'").show(10, False)




source_df = spark.table(fqtn)
out = source_df.withColumn(
    "bucket_company_id", f.expr(f"iceberg.bucket({bucket_size}, `company_id`)")
).withColumn("month_timestamp", f.expr("iceberg.months(`timestamp`)"))

out.show(truncate=False)

# spark.sql(f""""
# select * from {fqtn}
# """).show()


spark.stop()
