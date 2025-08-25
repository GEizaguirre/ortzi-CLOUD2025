
import hashlib
import random
from typing import List

from donfig import Config
import os

random.seed(42)  # Set a fixed seed value for deterministic results


PKG_NAME = "ortzi"
ORTZI_TMP_DIR = "/tmp/ortzi"
HOME_DIR = os.path.expanduser("~")
ORTZI_DIR = os.path.join(HOME_DIR, ".ortzi")
ORTZI_LOGS_DIR = os.path.join(ORTZI_DIR, "logs")

KB = 1024
MB = KB * KB
GB = MB * KB

# IO
MAX_RETRIES_LOW: int = 3
MAX_RETRIES_POLL: int = 10
MAX_READ_TIME: int = 60
RETRY_WAIT_TIME: float = 0.5
CONNECTION_RETRY_WAIT_TIME: float = 1.0
META_SUFFIX = "_meta"

# Physical plan
DEFAULT_PARTITION_SIZE = 128 * MB
MAX_PARALLELISM = 500
DEFAULT_OUT_PARTITIONS = 10000

# System
DEFAULT_DRIVER_PORT = 50051
DEFAULT_DRIVER_LISTEN_IP = "0.0.0.0"
DEFAULT_DRIVER_IP = "0.0.0.0"
DEFAULT_EXECUTOR_SERVER_PORT = 50058
DEFAULT_EXECUTOR_SERVER_IP = "0.0.0.0"
MAX_SERVER_THREADS = 500
NEXT_TASK_TIMEOUT = 3

DEFAULT_RUNTIME = "ortzi:1"
SINGLE_CPU_MEMORY_SHARE_MB = 1769

PREFIX_LEN = 5
NUM_PREFIXES = 1000

# Last logs fname environment variable
CONFIG_FILE = os.path.join(ORTZI_DIR, "config")
LAST_LOGS_VARNAME = "ORTZI_LAST_LOGS"
ORTZI_DEFAULT_SECTION = "ortzi"


def gen_prefixes() -> List[str]:
    """Generates a list of random prefixes."""
    prefixes = []
    for n in range(NUM_PREFIXES):
        prefix = hashlib.sha256(str(n).encode()).hexdigest()[:6]
        prefixes.append(prefix)
    return prefixes


config = Config(PKG_NAME)
config.set(debug_logging=False)
config.set(local_mode=False)
config.set(prefixes=gen_prefixes())

# CLOUD
ON_PREMISE_BACKEND = "aws_ec2"
