"""Consume Kafka events with explicit offsets and an idempotent SQLite inbox."""
import argparse
import json
from pathlib import Path
from inbox import apply_event, connect, summary


def main() -> int:
    from kafka import KafkaConsumer

    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap-server", default="127.0.0.1:9092")
    p.add_argument("--topic", default="academy-orders")
    p.add_argument("--group-id", default="academy-notifications")
    p.add_argument("--database", type=Path, default=Path("output/inbox.db"))
    p.add_argument("--max-records", type=int, default=100)
    p.add_argument("--crash-after-first", action="store_true")
    args = p.parse_args()
    consumer = KafkaConsumer(
        args.topic, bootstrap_servers=[args.bootstrap_server],
        group_id=args.group_id, enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda data: json.loads(data.decode("utf-8")),
        consumer_timeout_ms=4000,
    )
    processed = 0
    try:
        with connect(args.database) as db:
            for record in consumer:
                is_new = apply_event(db, record.value)
                print("received", record.offset, record.value["event_id"],
                      "new_effect", is_new, flush=True)
                processed += 1
                if args.crash_after_first and processed == 1:
                    print("SIMULATED CRASH after SQLite commit, before Kafka offset commit",
                          flush=True)
                    return 23
                consumer.commit()
                if processed >= args.max_records:
                    break
            print(json.dumps(summary(db), indent=2))
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
