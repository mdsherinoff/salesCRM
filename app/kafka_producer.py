from kafka import KafkaProducer
import json
import os
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# Module-level producer — shared across app lifetime
_producer = None


def get_producer() -> KafkaProducer:
    """Get or create the Kafka producer."""
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",           # wait for all replicas to acknowledge
            retries=3,            # retry up to 3 times on failure
            linger_ms=5,          # batch messages within 5ms window
        )
        logger.info("✓ Kafka producer created")
        print("✓ Kafka producer created")
    return _producer


def publish_kafka(topic: str, key: str, message: dict):
    """
    Publish a message to a Kafka topic.

    topic:   Kafka topic name e.g. 'crm.leads', 'crm.deals'
    key:     Partition key e.g. str(lead_id) — same key always goes to same partition
    message: dict that will be JSON serialised
    """
    producer = get_producer()

    future = producer.send(
        topic=topic,
        key=key,
        value=message
    )

    # Block until sent (with timeout) — confirms broker received it
    record_metadata = future.get(timeout=10)

    print(
        f"✓ Kafka [{topic}] partition={record_metadata.partition} "
        f"offset={record_metadata.offset} key={key}"
    )
    logger.info(
        f"Published to [{topic}] partition={record_metadata.partition} "
        f"offset={record_metadata.offset}"
    )


def close_producer():
    """Flush and close the producer on shutdown."""
    global _producer
    if _producer:
        _producer.flush()
        _producer.close()
        print("✓ Kafka producer closed")