from confluent_kafka import Consumer
import json

consumer = Consumer(
    {
        'bootstrap.servers': 'localhost:9092',
        "group.id":"order-tracker",
        "auto.offset.reset":"earliest",
    }
)

consumer.subscribe(["food-bought"])

print(f"Consumers is subscribed to food-topic")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error:{msg.error()}")
        # Reversing the process from json strings => dictionary.
        value = msg.value().decode("utf-8")
        order =  json.loads(value)
        print(f" 📦 Received package: {order}")
except KeyboardInterrupt:
    print("\nStopping consumer gracefully")
finally:
    # CLOSES CONSUMER INCASE FOR A FAILURE
    consumer.close()