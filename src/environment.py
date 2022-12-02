"""Project Global Constants."""
import argparse
import logging
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
QUERY_PATH = PROJECT_ROOT / Path("src/sql")


def parse_log_level() -> str:
    """Parses Global Log Level from runtime arguments (default = INFO)"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-l", "--log-level", type=str, default="INFO", help="set log level"
    )
    args, _ = parser.parse_known_args()
    level: str = args.log_level.upper()
    logging.info(f"Global Log Level configured as {level}")
    return level


LOG_LEVEL = parse_log_level()
