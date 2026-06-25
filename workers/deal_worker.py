from workers.base import BaseWorker


class DealWorker(BaseWorker):
    queue       = "deal_events"
    routing_key = "deal.*"

    def __init__(self):
        super().__init__()
        self.handlers = {
            "deal.won":  self.on_deal_won,
            "deal.lost": self.on_deal_lost,
        }

    def on_deal_won(self, data: dict):
        """Deal marked as Won."""
        value = data.get("deal_value", 0)
        self.logger.info(
            f"Deal WON: {data['opp_name']} — "
            f"${value:,.2f}"
        )
        self._simulate_email(
            to="manager@yourcompany.com",
            subject=f"Deal closed! {data['opp_name']}",
            body=(
                f"Great news! {data['opp_name']} has been marked as Won.\n"
                f"Deal value: ${value:,.2f}"
            )
        )

    def on_deal_lost(self, data: dict):
        """Deal marked as Lost."""
        self.logger.info(
            f"Deal LOST: {data['opp_name']} "
            f"(Lead #{data.get('lead_id')})"
        )
        self._simulate_email(
            to="manager@yourcompany.com",
            subject=f"Deal lost: {data['opp_name']}",
            body=(
                f"{data['opp_name']} has been marked as Lost.\n"
                f"Consider scheduling a post-mortem review."
            )
        )

    def _simulate_email(self, to: str, subject: str, body: str):
        self.logger.info(f"  [EMAIL] To: {to}")
        self.logger.info(f"  [EMAIL] Subject: {subject}")
        self.logger.info(f"  [EMAIL] Body: {body[:80]}")