from kafka import KafkaConsumer
import json

def smart_decode(v: bytes):
    s = v.decode("utf-8", errors="replace").strip()
    if not s:
        return None
    try:
        return json.loads(s)   # JSON message
    except Exception:
        return s               # plain text message

consumer = KafkaConsumer(
    "stocks",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",   # read old messages too
    enable_auto_commit=True,
    group_id="stocks-demo-v2",      # new group so offsets reset
    value_deserializer=smart_decode
)

print(" Listening for messages...")
for msg in consumer:
    val = msg.value
    if val is not None:
        print(val)
