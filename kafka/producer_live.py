# producer_live.py
import csv, json, time, os, random, itertools, datetime
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "stocks")
CSV_PATH = os.getenv("CSV_PATH", "data/stocks.csv")
RATE_PER_SEC = float(os.getenv("RATE_PER_SEC", "5"))    # msgs/sec
LOOP_FOREVER = os.getenv("LOOP_FOREVER", "true").lower() in ("1","true","yes","y")
RANDOM_WALK = os.getenv("RANDOM_WALK", "true").lower() in ("1","true","yes","y")
WIGGLE_PCT = float(os.getenv("WIGGLE_PCT", "0.1"))      # +/- 0.1%
ADD_EVENT_TIME = os.getenv("ADD_EVENT_TIME", "true").lower() in ("1","true","yes","y")

def coerce(row):
    out = {}
    for k, v in row.items():
        if v is None: out[k] = None; continue
        s = str(v).strip()
        if not s: out[k] = None; continue
        try:
            out[k] = int(s) if s.isdigit() else float(s)
        except ValueError:
            out[k] = s
    return out

def wiggle(row):
    if not RANDOM_WALK: return row
    for key in ("price","close","last","open","high","low"):
        if key in row and isinstance(row[key], (int,float)):
            pct = (random.random()*2-1) * (WIGGLE_PCT/100.0)
            row[key] = round(row[key]*(1+pct), 4)
    return row

def add_ts(row):
    if ADD_EVENT_TIME:
        row["event_time"] = datetime.datetime.utcnow().isoformat(timespec="seconds")+"Z"
    return row

def load_rows(path):
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            yield coerce(r)

def main():
    if RATE_PER_SEC <= 0: raise ValueError("RATE_PER_SEC must be > 0")
    pause = 1.0 / RATE_PER_SEC
    p = KafkaProducer(bootstrap_servers=BOOTSTRAP,
                      value_serializer=lambda v: json.dumps(v, separators=(",",":")).encode("utf-8"),
                      acks="all", linger_ms=20)

    data = list(load_rows(CSV_PATH))
    it = itertools.cycle(data) if LOOP_FOREVER else iter(data)

    print(f" Streaming {CSV_PATH} → '{TOPIC}' @~{RATE_PER_SEC}/s "
          f"(loop={'on' if LOOP_FOREVER else 'off'}, wiggle={'on' if RANDOM_WALK else 'off'})")
    sent = 0
    try:
        for r in it:
            r = add_ts(wiggle(r))
            p.send(TOPIC, r)
            sent += 1
            time.sleep(pause)
    except KeyboardInterrupt:
        print("\n Ctrl+C → flushing…")
    finally:
        p.flush()
        print(f" Sent {sent} messages total.")
if __name__ == "__main__":
    main()
