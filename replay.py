"""
replay.py — Replay all CRM events from Kafka from the beginning.

Usage:
    python replay.py                    # replay all events
    python replay.py --topic crm.leads  # replay one topic only
    python replay.py --lead-id 1        # replay events for a specific lead
"""

import argparse
import json
import os
import time
from datetime import datetime
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

TOPICS = ["crm.leads", "crm.deals"]


def replay_all(topic_filter=None, lead_id_filter=None):
    topics = [topic_filter] if topic_filter else TOPICS

    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id=f"crm-replay-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=8000,
        session_timeout_ms=30000,
        heartbeat_interval_ms=3000,
    )

    print(f"\n{'='*60}")
    print(f"  CRM EVENT REPLAY")
    print(f"  Topics:    {topics}")
    print(f"  Lead ID:   {lead_id_filter or 'all'}")
    print(f"  Started:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    # Wait until partitions are actually assigned
    print("Waiting for partition assignment...")
    assigned = set()
    attempts = 0
    while not assigned and attempts < 10:
        consumer.poll(timeout_ms=1000)
        assigned = consumer.assignment()
        attempts += 1
        if not assigned:
            print(f"  attempt {attempts} — not yet assigned, retrying...")
            time.sleep(1)

    if not assigned:
        print("ERROR: Could not get partition assignment after 10 attempts.")
        consumer.close()
        return []

    print(f"Assigned partitions: {assigned}")
    consumer.seek_to_beginning(*assigned)
    print("Seeked to beginning — reading messages...\n")

    event_count = 0
    skipped     = 0
    timeline    = []

    try:
        for message in consumer:
            raw_value = message.value
            raw_key   = message.key

            data = json.loads(raw_value.decode("utf-8")) if isinstance(raw_value, bytes) else raw_value
            key  = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else str(raw_key)

            topic     = message.topic
            partition = message.partition
            offset    = message.offset

            # Filter by lead_id if requested
            if lead_id_filter:
                msg_lead_id = str(data.get("lead_id", ""))
                if key != str(lead_id_filter) and msg_lead_id != str(lead_id_filter):
                    skipped += 1
                    continue

            event_count += 1
            timeline.append({
                "offset":    offset,
                "partition": partition,
                "topic":     topic,
                "event":     data.get("event", "unknown"),
                "key":       key,
                "data":      data
            })

            print(f"[{event_count:03d}] offset={offset}  partition={partition}")
            print(f"       topic={topic}")
            print(f"       event={data.get('event', 'unknown')}")
            print(f"       key={key}")

            event = data.get("event", "")
            if event == "lead.created":
                print(f"       lead={data.get('lead_name')}  company={data.get('company_name')}")
            elif event == "lead.converted":
                print(f"       lead={data.get('lead_name')}  opp={data.get('opp_name')}")
            elif event == "lead.stale":
                print(f"       lead={data.get('lead_name')}  days_inactive={data.get('days_inactive')}")
            elif event == "deal.won":
                print(f"       deal={data.get('opp_name')}  value=${data.get('deal_value', 0):,.2f}")
            elif event == "deal.lost":
                print(f"       deal={data.get('opp_name')}")
            print()

    except Exception as e:
        print(f"Replay error: {e}")
    finally:
        consumer.close()

    print(f"\n{'='*60}")
    print(f"  REPLAY COMPLETE")
    print(f"  Events replayed: {event_count}")
    print(f"  Events skipped:  {skipped}")
    print(f"{'='*60}\n")

    if lead_id_filter and timeline:
        print(f"  LEAD #{lead_id_filter} TIMELINE")
        print(f"  {'─'*40}")
        for i, entry in enumerate(timeline, 1):
            print(f"  {i}. {entry['event']} (offset {entry['offset']})")
        print()

    return timeline


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replay CRM Kafka events")
    parser.add_argument("--topic",   help="Filter by topic e.g. crm.leads")
    parser.add_argument("--lead-id", help="Filter by lead ID", type=int)
    args = parser.parse_args()

    replay_all(
        topic_filter=args.topic,
        lead_id_filter=args.lead_id
    )