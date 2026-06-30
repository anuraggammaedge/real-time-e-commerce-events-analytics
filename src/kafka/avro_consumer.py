from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "product-view-consumer-group",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(consumer_config)

consumer.subscribe(["product-views"])

schema_registry_client = SchemaRegistryClient(
    {"url": "http://localhost:8081"}
)

avro_deserializer = AvroDeserializer(
    schema_registry_client
)

print("Consumer connected and subscribed successfully")

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print(msg.error())
        continue

    event = avro_deserializer(
        msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
    )
    print(event)
