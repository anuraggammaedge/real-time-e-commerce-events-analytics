from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    window,
)

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

spark.conf.set("spark.sql.shuffle.partitions", "4")


silver_df = spark.readStream.format("delta").load("delta/silver_events")

silver_df = silver_df.withWatermark(
    "event_time",
    "1 minutes"
)

category_df = (
    silver_df
    .groupBy(
        window("event_time", "1 minute"),
        "category"
    )
    .count()
)

category_query = (
    category_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "checkpoints/gold_category_metrics"
    )
    .trigger(processingTime="30 seconds")
    .start("delta/gold_category_metrics")
)

spark.streams.awaitAnyTermination()
