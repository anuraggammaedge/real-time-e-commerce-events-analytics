from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    window,
    current_timestamp,
    unix_timestamp,
)

builder = (
    SparkSession.builder
    .appName("GoldPipeline")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.12:3.2.0"
    )
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = builder.getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "4")

# ==================================================
# Read Silver Layer
# ==================================================

silver_df = (
    spark.readStream
    .format("delta")
    .load("delta/silver_events")
)

# ==================================================
# Watermark
# ==================================================

silver_df = silver_df.withWatermark(
    "event_time",
    "5 minutes"
)

# ==================================================
# Events Per Minute
# ==================================================

events_per_minute = (
    silver_df
    .groupBy(
        window("event_time", "1 minute")
    )
    .count()
)

# ==================================================
# Events Per Second
# ==================================================

# eps_df = (
#     silver_df
#     .groupBy(
#         window("event_time", "1 second")
#     )
#     .count()
# )

# ==================================================
# Category Metrics
# ==================================================

# category_df = (
#     silver_df
#     .groupBy(
#         window("event_time", "1 minute"),
#         "category"
#     )
#     .count()
# )

# ==================================================
# Top Products
# ==================================================

# top_products_df = (
#     silver_df
#     .groupBy(
#         window("event_time", "1 minute"),
#         "product_id"
#     )
#     .count()
# )

# ==================================================
# Latency Metrics
# ==================================================

# latency_df = (
#     silver_df
#     .withColumn(
#         "processing_time",
#         current_timestamp()
#     )
#     .withColumn(
#         "latency_seconds",
#         unix_timestamp("processing_time")
#         -
#         unix_timestamp("event_time")
#     )
# )

# ==================================================
# Events Per Second Table
# ==================================================

# eps_query = (
#     eps_df.writeStream
#     .format("delta")
#     .outputMode("append")
#     .option(
#         "checkpointLocation",
#         "checkpoints/gold_events_per_second"
#     )
#     .trigger(processingTime="30 seconds")
#     .start("delta/gold_events_per_second")
# )

# ==================================================
# Events Per Minute Table
# ==================================================

epm_query = (
    events_per_minute.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "checkpoints/gold_events_per_minute"
    )
    .trigger(processingTime="30 seconds")
    .start("delta/gold_events_per_minute")
)

# ==================================================
# Category Metrics Table
# ==================================================

# category_query = (
#     category_df.writeStream
#     .format("delta")
#     .outputMode("append")
#     .option(
#         "checkpointLocation",
#         "checkpoints/gold_category_metrics"
#     )
#     .trigger(processingTime="30 seconds")
#     .start("delta/gold_category_metrics")
# )

# ==================================================
# Product Metrics Table
# ==================================================

# top_products_query = (
#     top_products_df.writeStream
#     .format("delta")
#     .outputMode("append")
#     .option(
#         "checkpointLocation",
#         "checkpoints/gold_product_metrics"
#     )
#     .trigger(processingTime="30 seconds")
#     .start("delta/gold_product_metrics")
# )

# ==================================================
# Latency Table
# ==================================================

# latency_query = (
#     latency_df.writeStream
#     .format("delta")
#     .outputMode("append")
#     .option(
#         "checkpointLocation",
#         "checkpoints/gold_latency"
#     )
#     .trigger(processingTime="30 seconds")
#     .start("delta/gold_latency")
# )

spark.streams.awaitAnyTermination()