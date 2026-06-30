from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
)
from pyspark.sql.avro.functions import from_avro
from pathlib import Path

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

# spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark = builder.getOrCreate()

print(spark.sparkContext.getConf().get("spark.jars.packages"))

spark.conf.set("spark.sql.shuffle.partitions", "32")

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "product-views")
    .option("startingOffsets", "latest")
    .load()
)

df.printSchema()

raw_df = df.select(col("value"))

schema_path = Path("src/model/product_viewed.avsc")

with open(schema_path) as f:
    avro_schema = f.read()

payload_df = df.selectExpr(
    "topic",
    "partition",
    "offset",
    "timestamp as kafka_timestamp",
    "substring(value, 6) as avro_value",
)

decoded_df = payload_df.select(
    "topic",
    "partition",
    "offset",
    "kafka_timestamp",
    from_avro("avro_value", avro_schema).alias("event"),
)

final_df = decoded_df.select(
    "topic", "partition", "offset", "kafka_timestamp", "event.*"
)

final_df = final_df.withColumn("event_time", to_timestamp("event_timestamp"))

raw_query = (
    final_df.writeStream.format("delta")
    .queryName("raw_events")
    .outputMode("append")
    .option("checkpointLocation", "checkpoints/raw_events")
    .trigger(processingTime="10 seconds")
    .start("delta/raw_events")
)
spark.streams.awaitAnyTermination()
