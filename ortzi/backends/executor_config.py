from dataclasses import dataclass, field
from typing import Dict, List
import copy

from lithops.config import load_config
import boto3

from ortzi.backends.aws import (
    get_instance_private_ip,
    get_shared_memory_size,
    setup_instance
)
from ortzi.io_utils.info import IOBackend
from ortzi.backends.backend import BackendType, is_container_backend
from ortzi.config import (
    DEFAULT_DRIVER_PORT,
    DEFAULT_EXECUTOR_SERVER_IP,
    DEFAULT_EXECUTOR_SERVER_PORT,
    DEFAULT_RUNTIME,
    ON_PREMISE_BACKEND,
    SINGLE_CPU_MEMORY_SHARE_MB,
    DEFAULT_DRIVER_IP
)
from ortzi.task import Task
from ortzi.logging.execution_log import get_execution_logs_dir


@dataclass
class ExecutorInstance:
    id: int
    executor_ip: str
    port: int
    executor_info: 'ExecutorInfo'
    runtime_memory: int
    num_workers: int
    task_register: Dict[int, Task]
    cpus_per_worker: int = None
    exec_info: List = field(default_factory=list)


@dataclass
class ExecutorInfo:
    bucket: str
    task_register: Dict[int, Task]
    lithops_config: dict
    execution_backend: BackendType
    driver_ip: str = DEFAULT_DRIVER_IP
    driver_port: int = DEFAULT_DRIVER_PORT
    internal_backend: IOBackend = IOBackend.DISK
    runtime: str = DEFAULT_RUNTIME
    instance_id: str = None
    instance_type: str = None
    eager_io: bool = True
    default_runtime_memory: int = SINGLE_CPU_MEMORY_SHARE_MB
    default_num_workers: int = 1

    def to_dict(self):

        d = {
            "bucket": self.bucket,
            "config": self.lithops_config["lithops"],
            "execution_backend": self.execution_backend.value,
            "internal_backend": self.internal_backend.value,
            "runtime": self.runtime
        }
        if self.instance_id is not None:
            d["instance_id"] = self.instance_id
        if self.instance_type is not None:
            d["instance_type"] = self.instance_type
        return d


class ExecutorConfig:

    current_executor_id = 0

    def __init__(
        self,
        bucket: str,
        driver_ip: str = DEFAULT_DRIVER_IP,
        driver_port: int = DEFAULT_DRIVER_PORT,
        ssh_key_name: str = None,
        log_dir: str = None,
        lithops_config: dict = None,
        autoscaling_backend: BackendType = BackendType.MULTIPROCESSING,
        asynchronous_perc: int = 20,
        eager_io: bool = False
    ):
        self.executor_infos: Dict[BackendType, ExecutorInfo] = \
            {
                backend: None
                for backend in BackendType
            }
        self.initial_instances: Dict[BackendType, List[ExecutorInstance]] = {}
        self.task_register: Dict[int, Task] = dict()
        self.bucket = bucket
        self.driver_ip = driver_ip
        self.driver_port = driver_port
        self.ssh_key_name = ssh_key_name
        self.eager_io = eager_io
        if log_dir is None:
            self.log_dir = get_execution_logs_dir()
        else:
            self.log_dir = log_dir

        if lithops_config is None:
            self.lithops_config = load_config()
        else:
            self.lithops_config = lithops_config
        self.default_autoscaling_backend = autoscaling_backend
        self.asynchronous_perc = asynchronous_perc

    def get_initial_fanout(self) -> int:
        return sum(
            len(instances)
            for instances in self.initial_instances.values()
        )

    def get_region(self):
        if self.lithops_config["aws"]["region"] is not None:
            return self.lithops_config["aws"]["region"]
        else:
            raise Exception("Region not set in config")

    def get_backend(self):
        if self.lithops_config["lithops"]["backend"] is not None:
            return self.lithops_config["lithops"]["backend"]
        else:
            raise Exception("Backend not set in config")

    def get_external_storage_backend(self):
        if self.lithops_config["lithops"]["storage"] is not None:
            return self.lithops_config["lithops"]["storage"]
        else:
            raise Exception("External storage not set in config.")

    def set_executor_info(
        self,
        execution_backend: BackendType = BackendType.MULTIPROCESSING,
        internal_storage_backend: IOBackend = IOBackend.EXTERNAL,
        instance_id: int = None,
        instance_type: str = None,
        runtime: str = DEFAULT_RUNTIME,
        default_num_workers: int = 1,
        default_runtime_memory: int = SINGLE_CPU_MEMORY_SHARE_MB
    ):
        driver_ip = self.driver_ip
        if (
            execution_backend == BackendType.K8S and
            driver_ip in {"127.0.1.1", "127.0.0.1", "0.0.0.0", "localhost"}
        ):
            driver_ip = "host.minikube.internal"

        executor_info = ExecutorInfo(
            bucket=self.bucket,
            task_register={},
            lithops_config=self.lithops_config,
            execution_backend=execution_backend,
            driver_ip=driver_ip,
            driver_port=self.driver_port,
            internal_backend=internal_storage_backend,
            runtime=runtime,
            instance_id=instance_id,
            instance_type=instance_type,
            eager_io=self.eager_io,
            default_runtime_memory=default_runtime_memory,
            default_num_workers=default_num_workers
        )

        self.executor_infos[execution_backend] = executor_info

    def register_task(
        self,
        task_id: int,
        task: Task
    ):
        self.task_register[task_id] = task

    def container_config(
        self,
        instance_id: int,
        config: dict
    ):

        op_config = copy.deepcopy(config)
        op_config["lithops"]["backend"] = ON_PREMISE_BACKEND
        op_config["aws_ec2"] = {
            "region": config["aws"]["region"],
            "exec_mode": "consume",
            "instance_id": instance_id,
            "ssh_key_filename": self.ssh_key_name,
            "worker_processes": 1,
            "auto_dismantle": False
        }

        return op_config

    def to_dict(self):

        d = {
            "bucket": self.bucket,
            "driver_ip": self.driver_ip,
            "driver_port": self.driver_port,
            "log_dir": self.log_dir,
            "executor_infos": [
                executor_info.to_dict()
                for executor_info in self.executor_infos.values()
                if executor_info is not None
            ]
        }

        return d

    def add_executors(
        self,
        backend: BackendType,
        runtime_memory: int = SINGLE_CPU_MEMORY_SHARE_MB,
        runtime: str = DEFAULT_RUNTIME,
        cpus_per_worker: int = None,
        num_workers: int = 1,
        num: int = 1,
        server_ip: str = DEFAULT_EXECUTOR_SERVER_IP
    ):
        if backend not in self.initial_instances:
            self.initial_instances[backend] = []

        for _ in range(num):
            self.initial_instances[backend].append(
                self.prepare_executors(
                    backend=backend,
                    runtime_memory=runtime_memory,
                    runtime=runtime,
                    num_workers=num_workers,
                    cpus_per_worker=cpus_per_worker,
                    server_ip=server_ip
                )
            )

    def prepare_executors(
        self,
        backend: BackendType,
        runtime_memory: int = None,
        runtime: str = DEFAULT_RUNTIME,
        num_workers: int = None,
        cpus_per_worker: int = 1,
        server_ip: str = DEFAULT_EXECUTOR_SERVER_IP,
        num: int = 1
    ) -> List[ExecutorInstance]:

        _executor_info = self.executor_infos.get(
            backend,
            None
        )
        if _executor_info is None:
            raise Exception("No suitable executor")

        instance_id = _executor_info.instance_id
        instance_type = _executor_info.instance_type
        if runtime_memory is None:
            runtime_memory = _executor_info.default_runtime_memory
        if num_workers is None:
            num_workers = _executor_info.default_num_workers

        if backend == BackendType.CLOUD_VM:
            executor_port = DEFAULT_EXECUTOR_SERVER_PORT
            ec2 = boto3.client("ec2")
            config = self.container_config(
                instance_id,
                self.lithops_config
            )
            instance_id, instance_type = setup_instance(
                ec2,
                instance_id,
                instance_type
            )
            if "standalone" not in config:
                config["standalone"] = {}

            if "shm_size" not in config["standalone"]:
                runtime_memory = get_shared_memory_size(
                    ec2,
                    instance_id,
                    instance_type
                )
                config["standalone"]["shm_size"] = runtime_memory
            server_ip = get_instance_private_ip(instance_id)

        executor_instances = []
        for _ in range(num):
            executor_id = ExecutorConfig.current_executor_id
            ExecutorConfig.current_executor_id += 1

            if not is_container_backend(backend):
                # Reachable via network
                executor_port = DEFAULT_EXECUTOR_SERVER_PORT + executor_id
            else:
                executor_port = None

            executor_instance = ExecutorInstance(
                id=executor_id,
                executor_ip=server_ip,
                port=executor_port,
                executor_info=_executor_info,
                runtime_memory=runtime_memory,
                num_workers=num_workers,
                task_register=self.task_register,
                cpus_per_worker=cpus_per_worker
            )

            executor_instances.append(executor_instance)

        return executor_instances


def get_initial_execution_config(
    bucket: str,
    driver_ip: str = None,
    ssh_key_name: str = None,
    execution_backend: BackendType = None,
    internal_storage: IOBackend = None,
    runtime: str = None,
    instance_id: str = None,
    instance_ip: str = None,
    num_executors: int = 0,
    default_num_workers: int = 1,
    default_runtime_memory: int = SINGLE_CPU_MEMORY_SHARE_MB,
    instance_type: str = None,
    log_dir: str = None,
    eager_io: bool = True
) -> ExecutorConfig:

    print("bucket:", bucket)
    print("driver_ip:", driver_ip)
    print("ssh_key_name:", ssh_key_name)
    print("execution_backend:", execution_backend)
    print("internal_storage:", internal_storage)
    print("runtime:", runtime)
    print("instance_id:", instance_id)
    print("instance_ip:", instance_ip)
    print("num_executors:", num_executors)
    print("default_num_workers:", default_num_workers)
    print("default_runtime_memory:", default_runtime_memory)
    print("instance_type:", instance_type)
    print("log_dir:", log_dir)

    execution_config = ExecutorConfig(
        bucket=bucket,
        driver_ip=driver_ip,
        ssh_key_name=ssh_key_name,
        log_dir=log_dir,
        eager_io=eager_io
    )

    execution_config.set_executor_info(
        execution_backend=execution_backend,
        internal_storage_backend=internal_storage,
        runtime=runtime,
        instance_id=instance_id,
        instance_type=instance_type
    )

    execution_config.add_executors(
        backend=execution_backend,
        runtime=runtime,
        cpus_per_worker=1,
        server_ip=instance_ip,
        num=num_executors,
        num_workers=default_num_workers
    )

    return execution_config
