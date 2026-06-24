import pika
import json
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    """Create a connection to RabbitMQ."""
    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER"),
        os.getenv("RABBITMQ_PASS")
    )
    params = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST"),
        port=int(os.getenv("RABBITMQ_PORT")),
        credentials=credentials
    )
    return pika.BlockingConnection(params)


def publish_event(routing_key: str, message: dict):
    """
    Publish a message to the CRM exchange.
    routing_key: e.g. 'lead.converted', 'deal.won', 'lead.stale'
    message: dict that will be JSON serialised
    """
    connection = get_connection()
    channel = connection.channel()

    # Declare a topic exchange — routes by routing key pattern
    channel.exchange_declare(
        exchange='crm_exchange',
        exchange_type='topic',
        durable=True  # survives RabbitMQ restart
    )

    # Declare the queue
    channel.queue_declare(
        queue='lead_events',
        durable=True  # survives RabbitMQ restart
    )

    # Bind queue to exchange with pattern "lead.*"
    channel.queue_bind(
        exchange='crm_exchange',
        queue='lead_events',
        routing_key='lead.*'
    )

    # Publish the message
    channel.basic_publish(
        exchange='crm_exchange',
        routing_key=routing_key,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
            content_type='application/json'
        )
    )

    print(f"✓ Published [{routing_key}]: {message}")
    connection.close()


# Test it directly when run as a script
if __name__ == "__main__":
    publish_event(
        routing_key="lead.converted",
        message={
            "lead_id":      1,
            "lead_name":    "Sarah Mitchell",
            "company_name": "Nexaflow Inc",
            "event":        "lead.converted"
        }
    )
    publish_event(
        routing_key="lead.stale",
        message={
            "lead_id":       4,
            "lead_name":     "Carlos Mendez",
            "days_inactive": 10,
            "event":         "lead.stale"
        }
    )