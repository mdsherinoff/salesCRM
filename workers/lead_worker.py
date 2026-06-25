from workers.base import BaseWorker


class LeadWorker(BaseWorker):
    queue       = "lead_events"
    routing_key = "lead.*"

    def __init__(self):
        super().__init__()
        # Register handlers for each event type
        self.handlers = {
            "lead.created":   self.on_lead_created,
            "lead.converted": self.on_lead_converted,
            "lead.stale":     self.on_lead_stale,
        }

    def on_lead_created(self, data: dict):
        """New lead added to CRM."""
        self.logger.info(
            f"New lead: {data['lead_name']} "
            f"from {data.get('company_name', 'Unknown')} "
            f"via {data.get('source', 'Unknown')}"
        )
        self._simulate_notification(
            subject=f"New lead: {data['lead_name']}",
            body=f"A new lead has been added from {data.get('source')}."
        )

    def on_lead_converted(self, data: dict):
        """Lead converted to opportunity."""
        self.logger.info(
            f"Lead converted: {data['lead_name']} → "
            f"Opp #{data.get('opp_id')} ({data.get('opp_name')})"
        )
        self._simulate_email(
            to=data.get("email", "unknown"),
            subject=f"Welcome {data['lead_name']} — your account is being set up",
            body=f"Hi {data['lead_name']}, great news — you have been qualified!"
        )

    def on_lead_stale(self, data: dict):
        """Lead inactive for too long."""
        days = data.get("days_inactive", "?")
        self.logger.info(
            f"Stale lead: {data['lead_name']} "
            f"({days} days inactive)"
        )
        self._simulate_email(
            to="sales@yourcompany.com",
            subject=f"Follow up needed: {data['lead_name']}",
            body=f"{data['lead_name']} has been inactive for {days} days."
        )

    def _simulate_email(self, to: str, subject: str, body: str):
        """Placeholder — replace with real SMTP later."""
        self.logger.info(f"  [EMAIL] To: {to}")
        self.logger.info(f"  [EMAIL] Subject: {subject}")
        self.logger.info(f"  [EMAIL] Body: {body[:80]}")

    def _simulate_notification(self, subject: str, body: str):
        """Placeholder — replace with Slack/Teams webhook later."""
        self.logger.info(f"  [NOTIFY] {subject}")
        self.logger.info(f"  [NOTIFY] {body}")