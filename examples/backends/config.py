
from lithops.config import load_config
from lithops.storage import Storage

from ortzi.backends.aws import (
    get_private_ip,
    get_ssh_key_filename
)
from ortzi.backends.backend import BackendType

lithops_config = load_config()

DOCKER_USERNAME = lithops_config["k8s"]["docker_user"]

# Functional variables
BUCKET = "benchmarks-objects"
AUX_BUCKET = "aux-bucket"
# Deployment variables
DRIVER_IP = get_private_ip()
RUNTIMES = {
    BackendType.CLOUD_FUNCTION: "ortzi-runtime:1.0",
    BackendType.CLOUD_VM: "<public-image>",
    BackendType.MULTIPROCESSING: None,
    BackendType.K8S: f"{DOCKER_USERNAME}/ortzi-runtime:1.0"
}
SSH_KEY_NAME = get_ssh_key_filename()
EXECUTOR_INSTANCE_ID = ""
INSTANCE_TYPE = "m4.xlarge"
# INSTANCE_TYPE = "m4.16xlarge"


def remove_disk_intermediates():
    storage = Storage(backend="localhost")
    bucket = storage.bucket
    obj_metas = storage.list_objects(
        bucket=bucket
    )
    keys = [obj_meta['Key'] for obj_meta in obj_metas]
    for key in keys:
        storage.delete_object(
            bucket=bucket,
            key=key
        )
