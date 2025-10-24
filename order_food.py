from confluent_kafka import Producer
import json

producer = Producer(
    {'bootstrap.servers': 'localhost:9092'}
)
def message_delivery(error, msg):
    if error:
        print("❌ Failed to add event")
    else:
        print(f"👍 Delivered {msg.value().decode('utf-8')}")
        print(msg.partition())

order = {
    "order_id": "1234",
    "user": 98,
    "order": "Chapati + beans",
    "quantity":[4,6]
}

# 1. Covert dictionary to a json string => to bytes kafka can understand(encode)
value = json.dumps(order).encode('utf-8')
value = producer.produce(
    topic='food-bought',
      value=value,
      callback=message_delivery
      )
producer.flush()