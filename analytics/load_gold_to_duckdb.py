import duckdb
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.appName("GoldPipeline")
    .master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = builder.getOrCreate()

con = duckdb.connect("analytics.db")

# Events Per Minute
epm_df = (
    spark.read
    .format("delta")
    .load("delta/gold_events_per_minute")
)

epm_pd = epm_df.toPandas()

con.execute("""
DROP TABLE IF EXISTS events_per_minute
""")

con.register("epm_temp", epm_pd)

con.execute("""
CREATE TABLE events_per_minute AS
SELECT *
FROM epm_temp
""")

print("Loaded events_per_minute")

# Category Metrics
category_df = (
    spark.read
    .format("delta")
    .load("delta/gold_category_metrics")
)

category_pd = category_df.toPandas()

con.execute("""
DROP TABLE IF EXISTS category_metrics
""")

con.register("category_temp", category_pd)

con.execute("""
CREATE TABLE category_metrics AS
SELECT *
FROM category_temp
""")

print("Loaded category_metrics")

con.close()