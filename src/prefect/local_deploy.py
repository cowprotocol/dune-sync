import os
from dotenv import load_dotenv
from src.prefect.deployment import order_rewards

load_dotenv()

if __name__ == "__main__":
    # Not ideal, but this script is for local testing
    os.environ["PREFECT_SERVER_API_HOST"] = "0.0.0.0"
    os.environ["PREFECT_SERVER__TELEMETRY__ENABLED"] = "false"
    os.environ["PREFECT_API_URL"] = "http://localhost:4200/api"
    os.environ["PREFECT_LOGGING_LEVEL"] = "INFO"

    order_rewards.serve(
        name="dune-sync-prod-order-rewards",
        cron="*/30 * * * *", # Every 30 minutes
        tags=["solver", "dune-sync"],
        description="Run the dune sync order_rewards query",
        version="0.0.1",
        )
