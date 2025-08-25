from enum import Enum
from typing import Dict
import pyarrow


class EventType(Enum):
    END = "end"
    MEMORY = "memory"
    DELETE_KEY_S3 = "delete_key_s3"
    DELETE_KEY_DISK = "delete_key_disk"
    TASK = "task"
    TASK_END = "task_end"
    VOID = "void"
    ADD_PARTITION = "add_partition"
    ADD_PARTITION_STAGE = "add_partition_stage"
    UPDATE_PARTITION = "update_partition"
    END_PARTITION_MANAGER = "end_partition_manager"
    REQUEST_PARTITION = "requested_partition"
    PARTITION_IN_QUEUES = "partition_in_queues"
    QUEUE_PARTITION_READY = "queue_partition_ready"
    PARTITION_READY = "partition_ready"
    SAVE_PARTITION_CLOUD = "save_partition_cloud"
    PARTITION_NOT_IN_QUEUES = "partition_not_in_queues"
    PARTITION_SAVED_IN_CLOUD = "partition_saved_in_cloud"
    STAGE_SAVE_PARTITION_INFO = "stage_save_partition_info"
    STAGE_PARTITION_SAVED_INFO = "stage_partition_saved_info"
    STAGE_FINISH_EXECUTOR = "stage_finish_executor"


class Event():

    event_type: EventType

    def description(self) -> str:
        return ""


class EventEnd(Event):

    def __init__(self):
        self.event_type = EventType.END


class EventMemory(Event):

    def __init__(self, shmem_key="", unlink_all=False):
        self.event_type = EventType.MEMORY
        self.shmem_key = shmem_key
        self.unlink_all = unlink_all


class EventDeleteKeyS3(Event):

    def __init__(self, key):
        self.event_type = EventType.DELETE_KEY_S3
        self.key = key


class EventDeleteKeyDisk(Event):

    def __init__(self, key):
        self.event_type = EventType.DELETE_KEY_DISK
        self.key = key


class EventTask(Event):

    def __init__(self, task_info):
        self.event_type = EventType.TASK
        self.task_info = task_info


class EventVoidTask(Event):

    def __init__(
        self,
        sleep: float = 0.1
    ):
        self.event_type = EventType.VOID
        self.sleep = sleep


class EventTaskEnd(Event):

    def __init__(
        self,
        task_info,
        worker_id: int,
        executor_id: int,
        storage_output_backend: str,
        storage_input_keys,
        storage_output_key: str,
        serialized_data: Dict[int, pyarrow.RecordBatch],
        exec_info: bytes
    ):
        self.event_type = EventType.TASK_END
        self.task_info = task_info
        self.worker_id = worker_id
        self.executor_id = executor_id
        self.storage_output_backend = storage_output_backend
        self.storage_input_keys = storage_input_keys
        self.storage_output_key = storage_output_key
        self.serialized_data = serialized_data
        self.exec_info = exec_info


class EventAddPartition(Event):

    def __init__(
        self,
        key,
        num_read,
        worker_id,
        executor_id,
        storage,
        serialized_data
    ):
        self.event_type = EventType.ADD_PARTITION
        self.key = key
        self.num_read = num_read
        self.worker_id = worker_id
        self.executor_id = executor_id
        self.storage = storage
        # In the case of using queues the PM saves the data
        self.serialized_data = serialized_data


class EventAddPartitionStage(Event):

    def __init__(
        self,
        key,
        num_read,
        worker_id,
        executor_id,
        storage
    ):
        self.event_type = EventType.ADD_PARTITION_STAGE
        self.key = key
        self.num_read = num_read
        self.worker_id = worker_id
        self.executor_id = executor_id
        self.storage = storage


class EventUpdatePartitionStage(Event):

    def __init__(self, key, num_read, worker_id, executor_id):
        self.event_type = EventType.UPDATE_PARTITION
        self.key = key
        self.num_read = num_read
        self.worker_id = worker_id
        self.executor_id = executor_id

    def description(self) -> str:
        return f"{self.event_type} - executor:{self.executor_id}|" + \
            "worker:{self.worker_id} - {self.num_read} reads on {self.key}"


class EventEndPartitionManager(Event):

    def __init__(self):
        self.event_type = EventType.END_PARTITION_MANAGER


class EventRequestShmem(Event):

    def __init__(self, shmem_key, worker_id):
        self.event_type = EventType.REQUEST_PARTITION
        self.shmem_key = shmem_key
        self.worker_id = worker_id


class EventRequestPartition(Event):

    def __init__(self, key, worker_id, exchange_id, partition_id):
        self.event_type = EventType.REQUEST_PARTITION
        self.key = key
        self.worker_id = worker_id
        self.exchange_id = exchange_id
        self.partition_id = partition_id


class EventPartitionInQueues(Event):

    def __init__(self, key, worker_id, exchange_id, partition_id):
        self.event_type = EventType.PARTITION_IN_QUEUES
        self.key = key
        self.worker_id = worker_id
        self.exchange_id = exchange_id
        self.partition_id = partition_id


class EventSavePartitionCloud(Event):
    def __init__(self, key, storage, worker_id):
        self.event_type = EventType.SAVE_PARTITION_CLOUD 
        self.key = key
        self.storage = storage
        self.worker_id = worker_id


class EventPartitionSavedInCloud(Event):
    def __init__(self, key):
        self.event_type = EventType.PARTITION_SAVED_IN_CLOUD
        self.key = key


class EventPartitionReady(Event):

    def __init__(
        self,
        storage_backend: str,
        exchange_id,
        key,
        partition_id,
        serialized_data
    ):
        self.event_type = EventType.PARTITION_READY
        self.storage_backend = storage_backend
        self.exchange_id = exchange_id
        self.key = key
        self.partition_id = partition_id
        # In the case of having the partition in queues
        # we have to send to the worker the serialized data
        self.serialized_data = serialized_data                       


class EventPartitionNotInQueues(Event):

    def __init__(
        self,
        exchange_id,
        key,
        partition_id
    ):
        self.event_type = EventType.PARTITION_NOT_IN_QUEUES
        self.exchange_id = exchange_id
        self.key = key
        self.partition_id = partition_id


class EventStageSavePartitionInfo(Event):

    def __init__(
        self,
        key,
        storage,
        worker_id
    ):
        self.event_type = EventType.STAGE_SAVE_PARTITION_INFO
        self.key = key
        self.storage = storage
        self.worker_id = worker_id


class EventStagePartitionSavedInfo(Event):

    def __init__(
        self,
        key,
        num_read,
        worker_id,
        executor_id, 
        storage
    ):
        self.event_type = EventType.STAGE_PARTITION_SAVED_INFO
        self.key = key
        self.num_read = num_read
        self.worker_id = worker_id
        self.executor_id = executor_id
        self.storage = storage


class EventStageFinishExecutor(Event):

    def __init__(self, mess):
        self.event_type = EventType.STAGE_FINISH_EXECUTOR
        self.mess = mess
