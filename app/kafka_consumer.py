from kafka import KafkaConsumer
import json
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("KafkaConsumer")

# Topics to subscribe to
TOPICS = [
    "crm.leads",
    "crm.deals",
]


def handle_lead_event(key: str, data: dict, partition: int, offset: int):
    """Process events from crm.leads topic."""
    event = data.get("event", "unknown")

    print(f"\n{'='*50}")
    print(f"  Topic:     crm.leads")
    print(f"  Event:     {event}")
    print(f"  Key:       {key}")
    print(f"  Partition: {partition}  Offset: {offset}")
    print(f"  Time:      {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*50}")

    if event == "lead.created":
        print(f"  Lead:    {data.get('lead_name')}")
        print(f"  Company: {data.get('company_name')}")
        print(f"  Source:  {data.get('source')}")
        print(f"  Action:  Notifying sales team...")

    elif event == "lead.converted":
        print(f"  Lead:    {data.get('lead_name')}")
        print(f"  Opp:     {data.get('opp_name')} (#{data.get('opp_id')})")
        print(f"  Action:  Sending welcome email to {data.get('email')}...")

    elif event == "lead.stale":
        print(f"  Lead:    {data.get('lead_name')}")
        print(f"  Inactive: {data.get('days_inactive')} days")
        print(f"  Action:  Sending reminder to lead owner...")

    else:
        print(f"  Unknown event type: {event}")
        print(f"  Payload: {data}")


def handle_deal_event(key: str, data: dict, partition: int, offset: int):
    """Process events from crm.deals topic."""
    event = data.get("event", "unknown")

    print(f"\n{'='*50}")
    print(f"  Topic:     crm.deals")
    print(f"  Event:     {event}")
    print(f"  Key:       {key}")
    print(f"  Partition: {partition}  Offset: {offset}")
    print(f"  Time:      {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*50}")

    if event == "deal.won":
        value = data.get("deal_value", 0)
        print(f"  Deal:    {data.get('opp_name')}")
        print(f"  Value:   ${value:,.2f}")
        print(f"  Action:  Sending win notification to manager...")

    elif event == "deal.lost":
        print(f"  Deal:    {data.get('opp_name')}")
        print(f"  Action:  Sending loss report to manager...")

    else:
        print(f"  Unknown event type: {event}")
        print(f"  Payload: {data}")


def start_consumer():
    """Start consuming from all CRM Kafka topics."""
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id="crm-consumer-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="earliest",   # read from beginning if no offset saved
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
    )

    print(f"\n[*] Kafka consumer running — subscribed to: {TOPICS}")
    print("[*] Waiting for messages. CTRL+C to stop.\n")

    try:
        for message in consumer:
            topic     = message.topic
            key       = message.key
            data      = message.value
            partition = message.partition
            offset    = message.offset

            if topic == "crm.leads":
                handle_lead_event(key, data, partition, offset)
            elif topic == "crm.deals":
                handle_deal_event(key, data, partition, offset)

    except KeyboardInterrupt:
        print("\n[*] Consumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    start_consumer()