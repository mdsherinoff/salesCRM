import pika
import json
import os
from dotenv import load_dotenv

load_dotenv()


def handle_lead_converted(data: dict):
    """Handle a lead.converted event."""
    print(f"  → Lead converted: {data['lead_name']} from {data['company_name']}")
    print(f"  → Action: would send welcome email to sales team")
    # Day 5: replace this with real email sending


def handle_lead_stale(data: dict):
    """Handle a lead.stale event."""
    print(f"  → Stale lead: {data['lead_name']} ({data['days_inactive']} days inactive)")
    print(f"  → Action: would send reminder to lead owner")
    # Day 5: replace this with real email sending


def on_message(channel, method, properties, body):
    """Called once per message received from the queue."""
    data         = json.loads(body)
    routing_key  = method.routing_key

    print(f"\n[✓] Received [{routing_key}]")

    # Route to the right handler based on event type
    if routing_key == "lead.converted":
        handle_lead_converted(data)
    elif routing_key == "lead.stale":
        handle_lead_stale(data)
    else:
        print(f"  → Unknown event type: {routing_key}")

    # Acknowledge the message — tells RabbitMQ it was processed
    # Without this, RabbitMQ keeps the message and redelivers it
    channel.basic_ack(delivery_tag=method.delivery_tag)


def start_consumer():
    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER"),
        os.getenv("RABBITMQ_PASS")
    )
    params = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST"),
        port=int(os.getenv("RABBITMQ_PORT")),
        credentials=credentials
    )
    connection = pika.BlockingConnection(params)
    channel    = connection.channel()

    # Declare same exchange and queue (idempotent — safe to re-declare)
    channel.exchange_declare(
        exchange='crm_exchange',
        exchange_type='topic',
        durable=True
    )
    channel.queue_declare(queue='lead_events', durable=True)
    channel.queue_bind(
        exchange='crm_exchange',
        queue='lead_events',
        routing_key='lead.*'
    )

    # Only process 1 message at a time — don't overwhelm the worker
    channel.basic_qos(prefetch_count=1)

    # Start listening
    channel.basic_consume(
        queue='lead_events',
        on_message_callback=on_message
    )

    print("[*] Consumer running — waiting for messages. CTRL+C to stop.")
    channel.start_consuming()


if __name__ == "__main__":
    start_consumer()