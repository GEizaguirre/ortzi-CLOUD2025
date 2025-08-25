from enum import Enum


class BackendType(Enum):
    CLOUD_FUNCTION = "cloud_function"
    CLOUD_VM = "cloud_vm"
    MULTIPROCESSING = "multiprocessing"
    K8S = "k8s"


def is_container_backend(backend: BackendType | str):
    if isinstance(backend, str):
        backend = BackendType(backend)
    return backend == BackendType.CLOUD_FUNCTION or \
        backend == BackendType.CLOUD_VM or \
        backend == BackendType.K8S
