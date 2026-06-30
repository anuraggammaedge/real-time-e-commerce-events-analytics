from pyspark.sql import SparkSession
from pyspark.sql.functions import window


builder = (
    SparkSession.builder.appName("ProductViewStreaming")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        ",".join(
            [
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "org.apache.spark:spark-avro_2.12:3.5.1",
                "io.delta:delta-spark_2.12:3.2.0",
            ]
        ),
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = builder.getOrCreate()

bronze_df = (
    spark.read
    .format("delta")
    .load("delta/raw_events")
)

bronze_df.groupBy("partition").count().orderBy("partition").show()

bronze_count = (
    spark.read
    .format("delta")
    .load("delta/raw_events")
    .count()
)

silver_count = (
    spark.read
    .format("delta")
    .load("delta/silver_events")
    .count()
)
EPM_count = (
    spark.read
    .format("delta")
    .load("delta/gold_events_per_minute")
    .count()
)

print(f"Bronze: {bronze_count}")
print(f"Silver: {silver_count}")
print(f"EPM_count: {EPM_count}")

epm_df = (
    spark.read
    .format("delta")
    .load("delta/gold_events_per_minute")
)

epm_df.show(truncate=False)

epm_df.agg({"count":"sum"}).show()

gold_df = (
    spark.read
    .format("delta")
    .load("delta/gold_events_per_minute")
)

gold_df.orderBy("window").show(100, False)


df = (
    spark.read
    .format("delta")
    .load("delta/gold_category_metrics")
)

print("Rows:", df.count())

df.orderBy("window").show(
    100,
    truncate=False
)