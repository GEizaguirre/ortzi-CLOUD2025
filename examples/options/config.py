
from lithops.config import load_config

from ortzi.backends.backend import BackendType


lithops_config = load_config()

AUX_BUCKET = "aux-bucket"
DOCKER_USERNAME = lithops_config["k8s"]["docker_user"]
RUNTIMES = {
    BackendType.CLOUD_FUNCTION: "ortzi-runtime:1.0",
    BackendType.CLOUD_VM: "<public-image>",
    BackendType.MULTIPROCESSING: None,
    BackendType.K8S: f"{DOCKER_USERNAME}/ortzi-runtime:1.0"
}
