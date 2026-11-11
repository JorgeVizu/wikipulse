"""Read Wikimedia SSE and publish a minimal edit record to Kafka."""
import argparse
import json
import logging
import os
from http.client import HTTPException
from pathlib import Path
import time
from urllib.request import Request, urlopen

STREAM = "https://stream.wikimedia.org/v2/stream/recentchange"
TOPIC = "wiki-edits"
LOCAL = {"bootstrap_servers": "kafka:9092"}
CURSOR = Path("/state/wikimedia.cursor")


def milliseconds():
    return time.time_ns() // 1_000_000


def sse(response):
    data = []
    cursor = None
    kind = "message"
    for raw_line in response:
        line = raw_line.decode("utf-8").rstrip("\r\n")
        if not line:
            if kind == "message" and data and cursor:
                yield cursor, "\n".join(data)
            data = []
            cursor = None
            kind = "message"
            continue
        field, _, value = line.partition(":")
        value = value.removeprefix(" ")
        if field == "data":
            data.append(value)
        elif field == "id":
            cursor = value
        elif field == "event":
            kind = value


def normalize(event, received_ms):
    if (
        event.get("wiki") != "eswiki" or event.get("namespace") != 0
        or event.get("type") not in {"edit", "new"}
        or event.get("meta", {}).get("domain") == "canary"
    ):
        return None
    event_ms = event["timestamp"] * 1000
    return {
        "edit_id": str(event["id"]),
        "bot": bool(event.get("bot", False)),
        "title": event["title"],
        "event_ms": event_ms,
        "ingested_ms": received_ms,
    }


def live_events(cursor=None):
    headers = {
        "Accept": "text/event-stream",
        "User-Agent": os.getenv(
            "WIKIMEDIA_USER_AGENT", "WikiPulse-demo/0.1 (local educational streaming demo)"
        ),
    }
    delay = 2
    while True:
        if cursor:
            headers["Last-Event-ID"] = cursor
        try:
            with urlopen(Request(STREAM, headers=headers), timeout=30) as response:
                for cursor, raw in sse(response):
                    delay = 2
                    try:
                        edit = normalize(json.loads(raw), milliseconds())
                    except (ValueError, TypeError, KeyError):
                        logging.warning("Discarded invalid Wikimedia event")
                        continue
                    if edit:
                        yield cursor, edit
        except (OSError, HTTPException) as error:
            logging.warning("Wikimedia disconnected (%s)", type(error).__name__)
        time.sleep(delay)
        delay = min(60, delay * 2)


def cloud_config():
    bootstrap = os.getenv("AZURE_BOOTSTRAP")
    secret = os.getenv("AZURE_CONNECTION_STRING")
    if bool(bootstrap) != bool(secret):
        raise SystemExit("Configure both Azure variables or leave both empty.")
    if not bootstrap:
        return None
    return {
        "bootstrap_servers": bootstrap,
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "PLAIN",
        "sasl_plain_username": "$ConnectionString",
        "sasl_plain_password": secret,
    }


def publish(producer, topic, edit):
    producer.send(
        topic, key=edit["title"].encode(), value=json.dumps(edit).encode()
    ).get(timeout=30)


def capture(count):
    for number, (_, edit) in enumerate(live_events(), start=1):
        print(json.dumps(edit, ensure_ascii=True), flush=True)
        if number == count:
            break


def replay(path, speed):
    with path.open(encoding="utf-8-sig") as file:
        start = time.monotonic()
        first_ms = None
        for line in file:
            edit = json.loads(line)
            arrival = edit["ingested_ms"]
            if first_ms is None:
                first_ms = arrival
            target = start + (arrival - first_ms) / 1000 / speed
            time.sleep(max(0, target - time.monotonic()))
            yield edit


def relay(cloud, topic):
    from kafka import KafkaConsumer, KafkaProducer

    if cloud is None:
        raise SystemExit("Azure configuration is required for the relay.")
    producer = KafkaProducer(acks="all", **LOCAL)
    consumer = KafkaConsumer(
        topic, group_id="wikipulse-relay", auto_offset_reset="earliest",
        enable_auto_commit=False, **cloud,
    )
    for record in consumer:
        producer.send(TOPIC, key=record.key, value=record.value).get(timeout=30)
        consumer.commit()


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="mode", required=True)
    commands.add_parser("live")
    commands.add_parser("relay")
    capture_parser = commands.add_parser("capture")
    capture_parser.add_argument("--count", type=int, default=300)
    replay_parser = commands.add_parser("replay")
    replay_parser.add_argument("path", type=Path)
    replay_parser.add_argument("--speed", type=float, default=10)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    if args.mode == "capture":
        if args.count < 1:
            parser.error("--count must be positive")
        capture(args.count)
        return

    from kafka import KafkaProducer

    cloud = cloud_config()
    topic = os.getenv("AZURE_TOPIC", TOPIC) if cloud else TOPIC
    if args.mode == "relay":
        relay(cloud, topic)
        return

    producer = KafkaProducer(acks="all", **(cloud or LOCAL))
    try:
        if args.mode == "replay":
            if args.speed <= 0:
                parser.error("--speed must be positive")
            for edit in replay(args.path, args.speed):
                publish(producer, topic, edit)
            return
        cursor = CURSOR.read_text() if CURSOR.exists() else None
        for cursor, edit in live_events(cursor):
            publish(producer, topic, edit)
            pending = CURSOR.with_suffix(".tmp")
            pending.write_text(cursor)
            pending.replace(CURSOR)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
