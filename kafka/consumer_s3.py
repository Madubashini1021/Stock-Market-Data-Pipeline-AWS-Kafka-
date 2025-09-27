# consumer_s3.py
import os, io, gzip, json, datetime
from dateutil import tz
import boto3
from kafka import KafkaConsumer

# ---- Settings (override via environment variables if you like) ----
BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")
TOPIC     = os.getenv("TOPIC", "stocks")
GROUP_ID  = os.getenv("GROUP_ID", "stocks-to-s3-v1")

BUCKET    = os.getenv("BUCKET", "kafka-stock-market-nishadi")  # <-- your bucket
PREFIX    = os.getenv("PREFIX", "raw/stocks")                  # s3://BUCKET/raw/stocks/...

BATCH_SIZE      = int(os.getenv("BATCH_SIZE", "50"))  # small for quick testing
IDLE_FLUSH_SEC  = int(os.getenv("IDLE_FLUSH_SEC", "5"))
# -------------------------------------------------------------------

s3 = boto3.client("s3")

def today_iso():
    return datetime.datetime.now(tz=tz.tzlocal()).date().isoformat()

def now_ts():
    return int(datetime.datetime.utcnow().timestamp())

def choose_partition_date(record: dict):
    """
    Use message's date if present (e.g., '2025-09-26' or '2025/09/26'),
    else fall back to today's date. If message has 'event_time' like
    '2025-09-26T12:34:56Z', use its date part.
    """
    if not isinstance(record, dict):
        return today_iso()
    for key in ("date", "event_time", "timestamp"):
        val = record.get(key)
        if not val:
            continue
        s = str(val)
        # try YYYY-MM-DD first
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return s[:10]
        # try YYYY/MM/DD
        if len(s) >= 10 and s[4] == "/" and s[7] == "/":
            return s[:10].replace("/", "-")
    return today_iso()

def write_batch(records):
    if not records:
        return None
    dt = choose_partition_date(records[-1])
    key = f"{PREFIX}/dt={dt}/part-{now_ts()}.json.gz"

    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="w") as gz:
        for r in records:
            gz.write((json.dumps(r, separators=(',', ':')) + "\n").encode("utf-8"))
    buf.seek(0)

    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    print(f"⬆️  Uploaded {len(records)} → s3://{BUCKET}/{key}")
    return key

def smart_deserialize(b: bytes):
    s = b.decode("utf-8", errors="replace").strip()
    if not s:
        return None
    try:
        return json.loads(s)  # JSON message
    except Exception:
        return {"text": s}    # plain text fallback

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="earliest",     # read from beginning on first run for this group
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=smart_deserialize,
        consumer_timeout_ms=IDLE_FLUSH_SEC * 1000  # lets us flush on idle
    )

    print(f" Consuming '{TOPIC}' from {BOOTSTRAP} → S3 s3://{BUCKET}/{PREFIX}/dt=...")
    batch, uploaded = [], 0

    try:
        while True:
            try:
                msg = next(consumer)  # blocks until message or consumer_timeout_ms
                if msg.value is not None:
                    batch.append(msg.value)
                    if len(batch) >= BATCH_SIZE:
                        write_batch(batch); uploaded += len(batch); batch.clear()
            except StopIteration:
                # idle period → flush partial batch
                if batch:
                    write_batch(batch); uploaded += len(batch); batch.clear()
    except KeyboardInterrupt:
        print("\n Ctrl+C → flushing…")
        if batch:
            write_batch(batch); uploaded += len(batch); batch.clear()
    finally:
        print(f" Done. Total uploaded: {uploaded}")

if __name__ == "__main__":
    main()
