"""Prefect Deployment for Order Rewards Data"""
import os

# pylint: disable=import-error
from prefect import flow  # type: ignore
from prefect.runner.storage import GitRepository  # type: ignore


# pylint: disable=duplicate-code
if __name__ == "__main__":
    branch_name = os.getenv("BRANCH_NAME")
    git_source = GitRepository(
        url="https://github.com/cowprotocol/dune-sync.git",
        branch=branch_name,
    )
    flow.from_source(
        source=git_source,
        entrypoint="src/deploy_prefect/flows.py:dev_order_rewards",
    ).deploy(
        name="dune-sync-pr-order-rewards",
        work_pool_name="cowbarn",
        tags=["dev", "solver", "dune-sync"],
        description="Run the dune sync order_rewards query",
        version="0.0.2",
    )
