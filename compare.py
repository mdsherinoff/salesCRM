"""
compare.py — Publish the same CRM event to both RabbitMQ and Kafka
and compare delivery, latency and behaviour side by side.

Usage:
    python compare.py
"""

import json
import os
import time
import threading
from datetime import datetime
from dotenv import load_dotenv

import pika
from kafka import KafkaConsumer, KafkaProducer

load_dotenv()

# ── Shared test event ─────────────────────────────────────────────────────────
TEST_EVENT = {
    "event":        "lead.converted",
    "lead_id":      99,
    "lead_name":    "Compare Test Lead",
    "company_name": "Side By Side Co",
    "email":        "compare@test.com",
    "opp_id":       99,
    "opp_name":     "Compare Test Lead — New Opportunity"
}

results = {
    "rabbitmq": {"published_at": None, "received_at": None, "latency_ms": None},
    "kafka":    {"published_at": None, "received_at": None, "latency_ms": None},
}


# ── RabbitMQ ──────────────────────────────────────────────────────────────────

def rabbitmq_consumer_thread():
    """Listen for the test message on RabbitMQ and record receive time."""
    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER"),
        os.getenv("RABBITMQ_PASS")
    )
    params = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST"),
        port=int(os.getenv("RABBITMQ_PORT")),
        credentials=credentials
    )
    conn    = pika.BlockingConnection(params)
    channel = conn.channel()

    channel.exchange_declare(exchange="crm_exchange", exchange_type="topic", durable=True)

    # Exclusive temporary queue just for this test
    result = channel.queue_declare(queue="", exclusive=True)
    temp_queue = result.method.queue
    channel.queue_bind(exchange="crm_exchange", queue=temp_queue, routing_key="lead.converted")

    def on_message(ch, method, properties, body):
        received_at = time.time()
        results["rabbitmq"]["received_at"] = received_at
        latency = (received_at - results["rabbitmq"]["published_at"]) * 1000
        results["rabbitmq"]["latency_ms"] = latency
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()

    channel.basic_consume(queue=temp_queue, on_message_callback=on_message)
    channel.start_consuming()
    conn.close()


def publish_to_rabbitmq():
    """Publish test event to RabbitMQ and record publish time."""
    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER"),
        os.getenv("RABBITMQ_PASS")
    )
    params = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST"),
        port=int(os.getenv("RABBITMQ_PORT")),
        credentials=credentials
    )
    conn    = pika.BlockingConnection(params)
    channel = conn.channel()
    channel.exchange_declare(exchange="crm_exchange", exchange_type="topic", durable=True)

    published_at = time.time()
    results["rabbitmq"]["published_at"] = published_at

    channel.basic_publish(
        exchange="crm_exchange",
        routing_key="lead.converted",
        body=json.dumps(TEST_EVENT),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json"
        )
    )
    conn.close()
    return published_at


# ── Kafka ─────────────────────────────────────────────────────────────────────

def kafka_consumer_thread():
    """Listen for the test message on Kafka and record receive time."""
    consumer = KafkaConsumer(
        "crm.leads",
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id=f"compare-test-{int(time.time())}",
        auto_offset_reset="latest",   # only new messages
        enable_auto_commit=True,
        consumer_timeout_ms=10000,
    )

    try:
        for message in consumer:
            raw  = message.value
            data = json.loads(raw.decode("utf-8")) if isinstance(raw, bytes) else raw

            # Only capture our specific test event
            if data.get("lead_id") == 99:
                received_at = time.time()
                results["kafka"]["received_at"] = received_at
                latency = (received_at - results["kafka"]["published_at"]) * 1000
                results["kafka"]["latency_ms"] = latency
                break
    finally:
        consumer.close()


def publish_to_kafka():
    """Publish test event to Kafka and record publish time."""
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        acks="all",
        retries=3,
    )

    published_at = time.time()
    results["kafka"]["published_at"] = published_at

    future = producer.send(
        topic="crm.leads",
        key=b"99",
        value=json.dumps(TEST_EVENT).encode("utf-8")
    )
    future.get(timeout=10)
    producer.close()
    return published_at


# ── Main comparison ───────────────────────────────────────────────────────────

def run_comparison():
    print(f"\n{'='*60}")
    print(f"  CRM BROKER COMPARISON — RabbitMQ vs Kafka")
    print(f"  Event: lead.converted (lead_id=99)")
    print(f"  Time:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    # Start both consumers in background threads BEFORE publishing
    print("Starting consumers...")
    rmq_thread   = threading.Thread(target=rabbitmq_consumer_thread, daemon=True)
    kafka_thread = threading.Thread(target=kafka_consumer_thread,    daemon=True)
    rmq_thread.start()
    kafka_thread.start()

    # Give consumers time to connect and start listening
    time.sleep(2)
    print("Consumers ready.\n")

    # Publish to RabbitMQ
    print("Publishing to RabbitMQ...")
    publish_to_rabbitmq()
    print("Publishing to Kafka...")
    publish_to_kafka()
    print()

    # Wait for both consumers to receive
    print("Waiting for consumers to receive messages...")
    rmq_thread.join(timeout=15)
    kafka_thread.join(timeout=15)

    # ── Results ───────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  RESULTS")
    print(f"{'='*60}\n")

    rmq   = results["rabbitmq"]
    kafka = results["kafka"]

    print(f"  RabbitMQ")
    print(f"  {'─'*40}")
    if rmq["latency_ms"] is not None:
        print(f"  Latency:          {rmq['latency_ms']:.2f} ms")
        print(f"  Message delivery: Push (broker delivers to consumer)")
        print(f"  After consumed:   Message DELETED from queue")
        print(f"  Replay possible:  NO")
    else:
        print(f"  Did not receive message within timeout")

    print()
    print(f"  Kafka")
    print(f"  {'─'*40}")
    if kafka["latency_ms"] is not None:
        print(f"  Latency:          {kafka['latency_ms']:.2f} ms")
        print(f"  Message delivery: Pull (consumer reads from log)")
        print(f"  After consumed:   Message RETAINED in log")
        print(f"  Replay possible:  YES — offset {results['kafka'].get('offset', 'see replay.py')}")
    else:
        print(f"  Did not receive message within timeout")

    print()

    # Winner
    if rmq["latency_ms"] and kafka["latency_ms"]:
        diff = abs(rmq["latency_ms"] - kafka["latency_ms"])
        faster = "RabbitMQ" if rmq["latency_ms"] < kafka["latency_ms"] else "Kafka"
        print(f"  Latency winner:   {faster} (by {diff:.2f} ms)")
        print(f"  Note: Both are fast enough for CRM workloads.")
        print(f"        Choose based on use case, not latency alone.")

    print(f"\n{'='*60}")
    print(f"  WHEN TO USE EACH IN YOUR CRM")
    print(f"{'='*60}")
    print(f"""
  RabbitMQ                        Kafka
  ──────────────────────────────  ──────────────────────────────
  Send stale lead email           Audit trail of all lead changes
  Trigger one-time notifications  Replay events after bug fix
  Route tasks to specific workers Build read models from events
  Simple job queues               Event sourcing / CQRS
  Short-lived task processing     Long-term event retention
""")


if __name__ == "__main__":
    run_comparison()