import threading
import sys
from workers.lead_worker import LeadWorker
from workers.deal_worker import DealWorker


def start_worker(worker):
    """Run a worker in its own thread."""
    try:
        worker.run()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Worker {worker.__class__.__name__} crashed: {e}")


def main():
    workers = [
        LeadWorker(),
        DealWorker(),
    ]

    threads = []
    for worker in workers:
        t = threading.Thread(
            target=start_worker,
            args=(worker,),
            daemon=True,
            name=worker.__class__.__name__
        )
        t.start()
        threads.append(t)
        print(f"✓ Started {worker.__class__.__name__}")

    print("\n[*] All workers running. CTRL+C to stop.\n")

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print("\n[*] Shutting down workers...")
        sys.exit(0)


if __name__ == "__main__":
    main()