import pika
import json
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from app.database import SessionLocal
from app.models import EventLog

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S"
)


class BaseWorker:
    """
    Base class for all CRM event workers.
    Handles RabbitMQ connection, message dispatch,
    DB logging and error handling.
    Subclasses only need to define handlers dict.
    """

    exchange    = "crm_exchange"
    queue       = None   # override in subclass
    routing_key = None   # override in subclass
    handlers    = {}     # override in subclass: {"routing.key": method}

    def __init__(self):
        self.logger     = logging.getLogger(self.__class__.__name__)
        self.connection = None
        self.channel    = None

    def connect(self):
        """Connect to RabbitMQ and set up queue."""
        credentials = pika.PlainCredentials(
            os.getenv("RABBITMQ_USER"),
            os.getenv("RABBITMQ_PASS")
        )
        params = pika.ConnectionParameters(
            host=os.getenv("RABBITMQ_HOST"),
            port=int(os.getenv("RABBITMQ_PORT")),
            credentials=credentials,
            heartbeat=600
        )
        self.connection = pika.BlockingConnection(params)
        self.channel    = self.connection.channel()

        self.channel.exchange_declare(
            exchange=self.exchange,
            exchange_type="topic",
            durable=True
        )
        self.channel.queue_declare(queue=self.queue, durable=True)
        self.channel.queue_bind(
            exchange=self.exchange,
            queue=self.queue,
            routing_key=self.routing_key
        )
        self.channel.basic_qos(prefetch_count=1)
        self.logger.info(f"Connected — listening on [{self.queue}]")

    def on_message(self, channel, method, properties, body):
        """Called for each message — dispatches to correct handler."""
        routing_key = method.routing_key
        db          = SessionLocal()

        try:
            data    = json.loads(body)
            handler = self.handlers.get(routing_key)

            self.logger.info(f"[{routing_key}] received")

            if handler:
                handler(data)
                status = "processed"
                error  = None
            else:
                self.logger.warning(f"No handler for [{routing_key}]")
                status = "skipped"
                error  = f"No handler registered for {routing_key}"

            # Write to audit log
            db.add(EventLog(
                event_type   = routing_key,
                routing_key  = routing_key,
                payload      = data,
                processed_at = datetime.utcnow(),
                status       = status,
                error        = error
            ))
            db.commit()

            # Acknowledge — remove from queue
            channel.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            self.logger.error(f"Error processing [{routing_key}]: {e}")
            try:
                db.add(EventLog(
                    event_type   = routing_key,
                    routing_key  = routing_key,
                    payload      = {},
                    processed_at = datetime.utcnow(),
                    status       = "failed",
                    error        = str(e)
                ))
                db.commit()
            except:
                db.rollback()
            # Nack — requeue the message for retry
            channel.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=True
            )
        finally:
            db.close()

    def run(self):
        """Start consuming messages."""
        self.connect()
        self.channel.basic_consume(
            queue=self.queue,
            on_message_callback=self.on_message
        )
        print(f"[*] {self.__class__.__name__} running. CTRL+C to stop.")
        self.channel.start_consuming()