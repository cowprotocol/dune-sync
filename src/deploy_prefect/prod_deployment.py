"""Prefect Deployment for Order Rewards Data"""
# pylint: disable=import-error
from prefect import flow  # type: ignore
from prefect.runner.storage import GitRepository  # type: ignore


# pylint: disable=duplicate-code
if __name__ == "__main__":
    git_source = GitRepository(
        url="https://github.com/cowprotocol/dune-sync.git",
    )
    flow.from_source(
        source=git_source,
        entrypoint="src/deploy_prefect/flows.py:prod_order_rewards",
    ).deploy(
        name="dune-sync-prod-order-rewards",
        work_pool_name="cowbarn",
        cron="*/30 * * * *",  # Every 30 minutes
        tags=["prod", "solver", "dune-sync"],
        description="Run the dune sync order_rewards query",
        version="0.0.2",
    )
