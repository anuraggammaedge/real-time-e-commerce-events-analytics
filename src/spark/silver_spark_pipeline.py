from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
)
spark = builder.getOrCreate()

spark.conf.set(
    "spark.sql.shuffle.partitions",
    "4"
)

bronze_df = (
    spark.readStream
    .format("delta")
    .load("delta/raw_events")
)

silver_df = bronze_df.filter(
    col("product_id").isNotNull()
)


query = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "checkpoints/silver_events"
    )
    .trigger(
        processingTime="10 seconds"
    )
    .start(
        "delta/silver_events"
    )
)

query.awaitTermination()