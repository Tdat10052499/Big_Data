#!/usr/bin/env python3
"""
trip_producer.py  --  replay historical taxi trips into Kafka as a live stream.

Course 72ITDS40303 Big Data Analytics, Van Lang University.
Lab 3: Windows, State, Latency, Replay and Failure in Structured Streaming.

Why a replay producer and not a random generator.
The stream you will analyse in Lab 3 must be the SAME data you profiled in Lab 1
and transformed in Lab 2. Only then can you tell whether a difference in your
results came from the streaming engine or from the data. This script reads the
curated trips and emits them as JSON events with a controllable arrival rate and
a controllable fraction of LATE events, so watermark behaviour is observable
instead of theoretical.

Event shape sent to Kafka:

    key:   the pickup zone id, as bytes, so all trips from one zone land in one partition
    value: {"trip_id", "event_time", "pickup_zone", "dropoff_zone",
            "passenger_count", "trip_distance", "fare_amount", "total_amount",
            "sent_at", "is_late"}

    event_time  is the time the trip is claimed to have happened.
    sent_at     is the time this script handed the event to Kafka.
    The gap between them is what a watermark reasons about.

Usage
    python trip_producer.py --rate 200 --limit 50000
    python trip_producer.py --rate 200 --late-fraction 0.10 --late-seconds 180
    python trip_producer.py --rate 50 --late-fraction 0.25 --late-seconds 600 --topic taxi-trips-late

Stop it with Ctrl+C. It prints a summary on exit, and that summary is the
denominator you need when you check how many late events the watermark dropped.
"""

import argparse
import json
import random
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

try:
    import pandas as pd
except ImportError:
    sys.exit("pandas is missing. Activate the lab environment first: source ~/bda/venv/bin/activate")

try:
    from kafka import KafkaProducer
except ImportError:
    sys.exit("kafka-python is missing. Install it with: pip install kafka-python-ng")


DEFAULT_SOURCE = "/home/tdat1/bda/data/raw/yellow_tripdata_2024-01.parquet"

COLUMNS = [
    "tpep_pickup_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
]

running = True


def stop(signum, frame):
    global running
    running = False


def load_trips(path, limit):
    """Read the trips once into memory. A month of yellow taxi data is about
    3 million rows and roughly 300 MB in memory with these seven columns, which
    fits comfortably in the 8 GB the WSL machine has by default."""
    print(f"reading {path}")
    df = pd.read_parquet(path, columns=COLUMNS)
    print(f"  {len(df):,} rows read")

    # Keep only rows that can form a sensible event. This mirrors the curated
    # rules from Lab 1. If you changed those rules, change them here too and say
    # so in your run record.
    before = len(df)
    df = df[
        (df["trip_distance"] > 0)
        & (df["fare_amount"] >= 0)
        & (df["passenger_count"].notna())
    ]
    print(f"  {before - len(df):,} rows dropped as not event shaped, {len(df):,} remain")

    if limit and limit < len(df):
        df = df.head(limit)
        print(f"  limited to the first {len(df):,} rows")
    return df


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bootstrap", default="localhost:9092", help="Kafka broker. Default localhost:9092.")
    p.add_argument("--topic", default="taxi-trips", help="Target topic. Default taxi-trips.")
    p.add_argument("--source", default=DEFAULT_SOURCE, help="Parquet file to replay.")
    p.add_argument("--rate", type=float, default=100.0, help="Events per second. Default 100.")
    p.add_argument("--limit", type=int, default=0, help="Stop after this many events. 0 means the whole file.")
    p.add_argument("--late-fraction", type=float, default=0.0,
                   help="Fraction of events stamped with an OLD event_time, between 0 and 1. Default 0.")
    p.add_argument("--late-seconds", type=int, default=300,
                   help="How far back a late event's event_time is pushed. Default 300.")
    p.add_argument("--seed", type=int, default=42, help="Random seed, so a run is reproducible. Default 42.")
    p.add_argument("--report-every", type=int, default=2000, help="Print a progress line every N events.")
    args = p.parse_args()

    if not 0.0 <= args.late_fraction <= 1.0:
        sys.exit("--late-fraction must be between 0 and 1")

    random.seed(args.seed)
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    df = load_trips(args.source, args.limit)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",           # wait for the broker to acknowledge, so a kill does not silently lose events
        linger_ms=20,         # small batching window, keeps throughput sane without hiding latency
        retries=3,
    )

    print()
    print(f"producing to {args.bootstrap} topic {args.topic}")
    print(f"  rate           {args.rate} events per second")
    print(f"  late fraction  {args.late_fraction:.0%} of events, pushed back {args.late_seconds} s")
    print(f"  seed           {args.seed}")
    print("  press Ctrl+C to stop")
    print()

    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    sent = 0
    late_sent = 0
    started = time.time()
    next_due = started

    for row in df.itertuples(index=False):
        if not running:
            break

        now = datetime.now(timezone.utc)
        is_late = random.random() < args.late_fraction
        if is_late:
            event_time = now - timedelta(seconds=args.late_seconds + random.randint(0, 60))
            late_sent += 1
        else:
            # A small natural jitter, so not every event lands exactly on "now".
            event_time = now - timedelta(seconds=random.uniform(0, 5))

        event = {
            "trip_id": f"t{sent:09d}",
            "event_time": event_time.isoformat(),
            "pickup_zone": int(row.PULocationID),
            "dropoff_zone": int(row.DOLocationID),
            "passenger_count": int(row.passenger_count),
            "trip_distance": float(row.trip_distance),
            "fare_amount": float(row.fare_amount),
            "total_amount": float(row.total_amount),
            "sent_at": now.isoformat(),
            "is_late": is_late,
        }

        producer.send(args.topic, key=int(row.PULocationID), value=event)
        sent += 1

        if sent % args.report_every == 0:
            elapsed = time.time() - started
            print(f"  sent {sent:>9,}  late {late_sent:>7,}  "
                  f"{sent / elapsed:>7.1f} events/s  elapsed {elapsed:>6.1f} s")

        if interval:
            next_due += interval
            delay = next_due - time.time()
            if delay > 0:
                time.sleep(delay)
            else:
                # We are behind the requested rate. Do not try to catch up in a
                # burst, because that would make the latency measurement in
                # section 9 meaningless. Reset the clock instead.
                next_due = time.time()

    print("\nflushing")
    producer.flush(timeout=30)
    producer.close(timeout=30)

    elapsed = time.time() - started
    print()
    print("  ---- run summary, copy this into your run record ----")
    print(f"  topic              {args.topic}")
    print(f"  events sent        {sent:,}")
    print(f"  of which late      {late_sent:,}  ({late_sent / sent:.2%} of the {sent:,} sent)" if sent else "  events sent 0")
    print(f"  wall clock         {elapsed:.1f} s")
    print(f"  achieved rate      {sent / elapsed:.1f} events per second" if elapsed > 0 else "")
    print(f"  requested rate     {args.rate} events per second")
    print(f"  late offset        {args.late_seconds} s")
    print(f"  seed               {args.seed}")
    print(f"  source             {args.source}")


if __name__ == "__main__":
    main()
