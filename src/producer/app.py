from producer.generators.products import generate_product_viewed

event = generate_product_viewed()

required_fields = {
    "event_id",
    "user_id",
    "product_id",
    "category",
    "event_timestamp",
}

assert set(event.keys()) == required_fields

print("Schema validation passed")
print(event)