import csv, json, time, os
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "stocks")
CSV_PATH = os.getenv("CSV_PATH", "data/stocks.csv")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
    acks="all", linger_ms=50
)

sent = 0
with open(CSV_PATH, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        # try to coerce common numeric fields
        for k, v in list(row.items()):
            if v is None:
                continue
            s = v.strip()
            if s == "":
                row[k] = None
                continue
            try:
                if "." in s:
                    row[k] = float(s)
                else:
                    row[k] = int(s)
            except ValueError:
                row[k] = s
        producer.send(TOPIC, row)
        sent += 1
        # small delay to simulate streaming; adjust or remove
        time.sleep(0.005)

producer.flush()
print(f" Sent {sent} records from {CSV_PATH} to topic '{TOPIC}' via {BOOTSTRAP}")
