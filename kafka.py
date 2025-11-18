from confluent_kafka import Consumer
import json
import sys 

bootstrap_server = input("Hello👋, please provide bootstrap servers:   ")

if bootstrap_server:
    b = bootstrap_server
else:
    b='localhost:9092'

print(f"\n🔗 connected to bootstrap server: {b}")

consumer = Consumer(
    {
        'bootstrap.servers': b,
        "group.id":"send-message-handler",
        "auto.offset.reset":"earliest",
    }
)
try:
    kafka_topic = sys.argv[1] 

    print(f"✅ Received topic {kafka_topic}")

    consumer.subscribe([kafka_topic])
    print(f"🛠️ Configuration of consumer done topic: {kafka_topic}")
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
    print("\n ❌ Stopped consumer gracefully")
except Exception as e:
    print(f"❌ ran into a problem \nError: {str(e)}")

finally:
    # CLOSES CONSUMER INCASE FOR A FAILURE
    consumer.close()