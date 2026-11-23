"""Publish fixture events to a local single-node Kafka topic."""
import argparse
import json
from pathlib import Path


def main():
    from kafka import KafkaProducer

    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-server", default="127.0.0.1:9092")
    parser.add_argument("--topic", default="academy-orders")
    parser.add_argument("--source", type=Path, default=Path("data/events.jsonl"))
    args = parser.parse_args()
    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap_server],
        value_serializer=lambda item: json.dumps(item).encode("utf-8"),
        acks="all",
    )
    count = 0
    try:
        for line in args.source.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            event = json.loads(line)
            metadata = producer.send(
                args.topic, key=event["order_id"].encode("utf-8"), value=event
            ).get(timeout=15)
            print("published", event["event_id"], "partition", metadata.partition,
                  "offset", metadata.offset)
            count += 1
    finally:
        producer.flush(timeout=15)
        producer.close()
    print("published_count", count)


if __name__ == "__main__":
    main()
