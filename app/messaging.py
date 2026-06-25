import pika
import json
import os
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# Module-level connection — shared across the app lifetime
_connection = None
_channel    = None


def connect_rabbitmq():
    """Open connection and declare exchange + queues."""
    global _connection, _channel

    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER"),
        os.getenv("RABBITMQ_PASS")
    )
    params = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST"),
        port=int(os.getenv("RABBITMQ_PORT")),
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )
    _connection = pika.BlockingConnection(params)
    _channel    = _connection.channel()

    # Declare exchange
    _channel.exchange_declare(
        exchange='crm_exchange',
        exchange_type='topic',
        durable=True
    )

    # Declare queues and bind them
    queues = {
        'lead_events': 'lead.*',
        'deal_events': 'deal.*',
    }
    for queue, routing_key in queues.items():
        _channel.queue_declare(queue=queue, durable=True)
        _channel.queue_bind(
            exchange='crm_exchange',
            queue=queue,
            routing_key=routing_key
        )

    logger.info("✓ RabbitMQ connected and queues ready")
    print("✓ RabbitMQ connected and queues ready")


def disconnect_rabbitmq():
    """Close connection cleanly on shutdown."""
    global _connection
    if _connection and not _connection.is_closed:
        _connection.close()
        print("✓ RabbitMQ connection closed")


def publish(routing_key: str, message: dict):
    """
    Publish a message to crm_exchange.
    Reconnects automatically if connection was lost.
    """
    global _connection, _channel

    # Reconnect if connection dropped
    if _connection is None or _connection.is_closed:
        print("RabbitMQ reconnecting...")
        connect_rabbitmq()

    try:
        _channel.basic_publish(
            exchange='crm_exchange',
            routing_key=routing_key,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type='application/json'
            )
        )
        print(f"✓ Published [{routing_key}]")
    except Exception as e:
        logger.error(f"Failed to publish [{routing_key}]: {e}")
        raise