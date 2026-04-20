"""Replay enriched 311 complaints onto a Kafka topic to simulate a live feed.

The producer reads `data/enriched/complaints311/` (so each message already has
its nta_code), rewrites `created_date` to "now", and sends paced events to
topic `complaints311` on localhost:9092. It loops forever so you can leave it
running.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import time
from datetime import datetime

import pandas as pd
from kafka import KafkaProducer

from pipeline.paths import ENR_COMPLAINTS311

TOPIC = "complaints311"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")


def _load_records() -> list[dict]:
    df = pd.read_parquet(ENR_COMPLAINTS311)
    df = df.dropna(subset=["nta_code"])
    keep = ["COMPLAINT_ID", "COMPLAINT_TYPE", "DESCRIPTOR", "ZIPCODE",
            "BORO", "Latitude", "Longitude", "nta_code", "nta_name"]
    keep = [c for c in keep if c in df.columns]
    return df[keep].to_dict(orient="records")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rps", type=float, default=50.0, help="events per second")
    ap.add_argument("--shuffle", action="store_true", help="shuffle order each loop")
    args = ap.parse_args()

    print(f"[producer] loading source records from {ENR_COMPLAINTS311}")
    records = _load_records()
    if not records:
        raise SystemExit("no source records found — run the batch pipeline first")
    print(f"[producer] {len(records)} records loaded; emitting to {TOPIC} @ {args.rps} rps")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: (k or "").encode("utf-8"),
        linger_ms=10,
    )

    interval = 1.0 / max(args.rps, 0.1)
    sent = 0
    try:
        while True:
            pool = list(records)
            if args.shuffle:
                random.shuffle(pool)
            for r in pool:
                payload = dict(r)
                payload["created_date"] = datetime.utcnow().isoformat() + "Z"
                producer.send(TOPIC, key=str(payload.get("nta_code", "")), value=payload)
                sent += 1
                if sent % 500 == 0:
                    print(f"[producer] {sent} events sent")
                time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\n[producer] stopped after {sent} events")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
