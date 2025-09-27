import csv, json, time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all", linger_ms=50
)

with open("data/stocks.csv", newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        producer.send("stocks", row)   # send each row to Kafka
        time.sleep(0.01)               # simulate streaming (10 ms delay)

producer.flush()
print(" Finished sending messages")
