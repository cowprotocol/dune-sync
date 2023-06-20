"""Project Global Constants."""
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
QUERY_PATH = PROJECT_ROOT / Path("src/sql")
LOG_CONFIG_FILE = PROJECT_ROOT / Path("logging.conf")
