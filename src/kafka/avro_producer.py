# from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from pathlib import Path

from producer.generators.products import generate_product_viewed
from confluent_kafka import Producer

def delivery_report(err, msg):

    if err is not None:
        print(f"Delivery failed: {err}")
        return

    print(
        f"Delivered to "
        f"topic={msg.topic()} "
        f"partition={msg.partition()} "
        f"offset={msg.offset()}"
    )

SCHEMA_REGISTRY_URL = "http://localhost:8081"

schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

schema_path = Path("src/model/product_viewed.avsc")

with open(schema_path, "r") as f:
    schema_str = f.read()

avro_serializer = AvroSerializer(
    schema_registry_client=schema_registry_client, schema_str=schema_str
)

producer_config = {"bootstrap.servers": "localhost:9092"}

producer = Producer(producer_config)

try:
    while True:
        event = generate_product_viewed()

        serialized_data = avro_serializer(
            event, SerializationContext("product-views", MessageField.VALUE)
        )

        producer.produce(
            topic="product-views",
            value=serialized_data,
            key=str(event["user_id"]),
            callback=delivery_report,
        )

        producer.poll(0)

except KeyboardInterrupt:
    print("Stopping producer...")

finally:
    producer.flush()

# print("Event sent Successfully")
