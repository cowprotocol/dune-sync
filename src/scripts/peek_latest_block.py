from dotenv import load_dotenv

from src.main import ScriptArgs
from src.post.aws import AWSClient
from src.sync.common import last_sync_block

if __name__ == "__main__":
    load_dotenv()
    block = last_sync_block(
        aws=AWSClient.new_from_environment(),
        table=ScriptArgs().sync_table,
    )

    print("Last sync block", block)
