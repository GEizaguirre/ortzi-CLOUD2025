from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CollectInformation(_message.Message):
    __slots__ = ("bucket", "key", "partition_id")
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    PARTITION_ID_FIELD_NUMBER: _ClassVar[int]
    bucket: str
    key: str
    partition_id: int
    def __init__(self, bucket: _Optional[str] = ..., key: _Optional[str] = ..., partition_id: _Optional[int] = ...) -> None: ...

class IOInformation(_message.Message):
    __slots__ = ("bucket", "key", "partition_id", "num_partitions", "io_type", "low_memory", "parse_args", "out_partitions")
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    PARTITION_ID_FIELD_NUMBER: _ClassVar[int]
    NUM_PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    IO_TYPE_FIELD_NUMBER: _ClassVar[int]
    LOW_MEMORY_FIELD_NUMBER: _ClassVar[int]
    PARSE_ARGS_FIELD_NUMBER: _ClassVar[int]
    OUT_PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    bucket: str
    key: str
    partition_id: int
    num_partitions: int
    io_type: str
    low_memory: bool
    parse_args: bytes
    out_partitions: int
    def __init__(self, bucket: _Optional[str] = ..., key: _Optional[str] = ..., partition_id: _Optional[int] = ..., num_partitions: _Optional[int] = ..., io_type: _Optional[str] = ..., low_memory: bool = ..., parse_args: _Optional[bytes] = ..., out_partitions: _Optional[int] = ...) -> None: ...

class PartitionInformation(_message.Message):
    __slots__ = ("exchange_id", "partition_ids", "source_tasks", "num_partitions", "num_read")
    EXCHANGE_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITION_IDS_FIELD_NUMBER: _ClassVar[int]
    SOURCE_TASKS_FIELD_NUMBER: _ClassVar[int]
    NUM_PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    NUM_READ_FIELD_NUMBER: _ClassVar[int]
    exchange_id: int
    partition_ids: _containers.RepeatedScalarFieldContainer[int]
    source_tasks: int
    num_partitions: int
    num_read: int
    def __init__(self, exchange_id: _Optional[int] = ..., partition_ids: _Optional[_Iterable[int]] = ..., source_tasks: _Optional[int] = ..., num_partitions: _Optional[int] = ..., num_read: _Optional[int] = ...) -> None: ...

class IOMessage(_message.Message):
    __slots__ = ("partition_id", "type", "io_info", "partition_info", "collect_info")
    PARTITION_ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    IO_INFO_FIELD_NUMBER: _ClassVar[int]
    PARTITION_INFO_FIELD_NUMBER: _ClassVar[int]
    COLLECT_INFO_FIELD_NUMBER: _ClassVar[int]
    partition_id: int
    type: int
    io_info: IOInformation
    partition_info: PartitionInformation
    collect_info: CollectInformation
    def __init__(self, partition_id: _Optional[int] = ..., type: _Optional[int] = ..., io_info: _Optional[_Union[IOInformation, _Mapping]] = ..., partition_info: _Optional[_Union[PartitionInformation, _Mapping]] = ..., collect_info: _Optional[_Union[CollectInformation, _Mapping]] = ...) -> None: ...

class AuxiliaryInformation(_message.Message):
    __slots__ = ("exchange_id", "type")
    EXCHANGE_ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    exchange_id: int
    type: str
    def __init__(self, exchange_id: _Optional[int] = ..., type: _Optional[str] = ...) -> None: ...

class TaskInformation(_message.Message):
    __slots__ = ("stage_id", "task_id", "input", "output", "auxiliary_input_info", "auxiliary_output_info")
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    AUXILIARY_INPUT_INFO_FIELD_NUMBER: _ClassVar[int]
    AUXILIARY_OUTPUT_INFO_FIELD_NUMBER: _ClassVar[int]
    stage_id: int
    task_id: int
    input: _containers.RepeatedCompositeFieldContainer[IOMessage]
    output: IOMessage
    auxiliary_input_info: AuxiliaryInformation
    auxiliary_output_info: AuxiliaryInformation
    def __init__(self, stage_id: _Optional[int] = ..., task_id: _Optional[int] = ..., input: _Optional[_Iterable[_Union[IOMessage, _Mapping]]] = ..., output: _Optional[_Union[IOMessage, _Mapping]] = ..., auxiliary_input_info: _Optional[_Union[AuxiliaryInformation, _Mapping]] = ..., auxiliary_output_info: _Optional[_Union[AuxiliaryInformation, _Mapping]] = ...) -> None: ...

class NumberOfTasks(_message.Message):
    __slots__ = ("num_tasks", "executor_id")
    NUM_TASKS_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    num_tasks: int
    executor_id: int
    def __init__(self, num_tasks: _Optional[int] = ..., executor_id: _Optional[int] = ...) -> None: ...

class ListTaskInformation(_message.Message):
    __slots__ = ("list_task_info",)
    LIST_TASK_INFO_FIELD_NUMBER: _ClassVar[int]
    list_task_info: _containers.RepeatedCompositeFieldContainer[TaskInformation]
    def __init__(self, list_task_info: _Optional[_Iterable[_Union[TaskInformation, _Mapping]]] = ...) -> None: ...

class EventAddPartitionStage(_message.Message):
    __slots__ = ("key", "num_read", "executor_id", "worker_id", "storage")
    KEY_FIELD_NUMBER: _ClassVar[int]
    NUM_READ_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    STORAGE_FIELD_NUMBER: _ClassVar[int]
    key: str
    num_read: int
    executor_id: int
    worker_id: int
    storage: str
    def __init__(self, key: _Optional[str] = ..., num_read: _Optional[int] = ..., executor_id: _Optional[int] = ..., worker_id: _Optional[int] = ..., storage: _Optional[str] = ...) -> None: ...

class EventUpdatePartitionStage(_message.Message):
    __slots__ = ("key", "num_read", "executor_id", "worker_id")
    KEY_FIELD_NUMBER: _ClassVar[int]
    NUM_READ_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    key: str
    num_read: int
    executor_id: int
    worker_id: int
    def __init__(self, key: _Optional[str] = ..., num_read: _Optional[int] = ..., executor_id: _Optional[int] = ..., worker_id: _Optional[int] = ...) -> None: ...

class TaskCompletedInfo(_message.Message):
    __slots__ = ("task_id", "stage_id", "executor_id", "worker_id", "list_add_partition_events", "list_upd_partition_events")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    LIST_ADD_PARTITION_EVENTS_FIELD_NUMBER: _ClassVar[int]
    LIST_UPD_PARTITION_EVENTS_FIELD_NUMBER: _ClassVar[int]
    task_id: int
    stage_id: int
    executor_id: int
    worker_id: int
    list_add_partition_events: _containers.RepeatedCompositeFieldContainer[EventAddPartitionStage]
    list_upd_partition_events: _containers.RepeatedCompositeFieldContainer[EventUpdatePartitionStage]
    def __init__(self, task_id: _Optional[int] = ..., stage_id: _Optional[int] = ..., executor_id: _Optional[int] = ..., worker_id: _Optional[int] = ..., list_add_partition_events: _Optional[_Iterable[_Union[EventAddPartitionStage, _Mapping]]] = ..., list_upd_partition_events: _Optional[_Iterable[_Union[EventUpdatePartitionStage, _Mapping]]] = ...) -> None: ...

class ListTaskCompletedInfo(_message.Message):
    __slots__ = ("task_completed_info",)
    TASK_COMPLETED_INFO_FIELD_NUMBER: _ClassVar[int]
    task_completed_info: _containers.RepeatedCompositeFieldContainer[TaskCompletedInfo]
    def __init__(self, task_completed_info: _Optional[_Iterable[_Union[TaskCompletedInfo, _Mapping]]] = ...) -> None: ...

class RequestPartition(_message.Message):
    __slots__ = ("key", "worker_id", "exchange_id", "partition_id", "executor_id")
    KEY_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    EXCHANGE_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITION_ID_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    key: str
    worker_id: int
    exchange_id: int
    partition_id: int
    executor_id: int
    def __init__(self, key: _Optional[str] = ..., worker_id: _Optional[int] = ..., exchange_id: _Optional[int] = ..., partition_id: _Optional[int] = ..., executor_id: _Optional[int] = ...) -> None: ...

class ExecutorAddress(_message.Message):
    __slots__ = ("executor_id", "backend_type", "executor_server_ip", "executor_server_port", "workers", "runtime_memory")
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    BACKEND_TYPE_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_SERVER_IP_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_SERVER_PORT_FIELD_NUMBER: _ClassVar[int]
    WORKERS_FIELD_NUMBER: _ClassVar[int]
    RUNTIME_MEMORY_FIELD_NUMBER: _ClassVar[int]
    executor_id: int
    backend_type: str
    executor_server_ip: str
    executor_server_port: int
    workers: int
    runtime_memory: int
    def __init__(self, executor_id: _Optional[int] = ..., backend_type: _Optional[str] = ..., executor_server_ip: _Optional[str] = ..., executor_server_port: _Optional[int] = ..., workers: _Optional[int] = ..., runtime_memory: _Optional[int] = ...) -> None: ...

class SavePartitionInfo(_message.Message):
    __slots__ = ("key", "storage", "worker_id_creator")
    KEY_FIELD_NUMBER: _ClassVar[int]
    STORAGE_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_CREATOR_FIELD_NUMBER: _ClassVar[int]
    key: str
    storage: str
    worker_id_creator: int
    def __init__(self, key: _Optional[str] = ..., storage: _Optional[str] = ..., worker_id_creator: _Optional[int] = ...) -> None: ...

class PartitionSavedInformation(_message.Message):
    __slots__ = ("key", "num_read", "worker_id", "executor_id", "storage")
    KEY_FIELD_NUMBER: _ClassVar[int]
    NUM_READ_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    STORAGE_FIELD_NUMBER: _ClassVar[int]
    key: str
    num_read: int
    worker_id: int
    executor_id: int
    storage: str
    def __init__(self, key: _Optional[str] = ..., num_read: _Optional[int] = ..., worker_id: _Optional[int] = ..., executor_id: _Optional[int] = ..., storage: _Optional[str] = ...) -> None: ...

class EventWriteOrRead(_message.Message):
    __slots__ = ("save_partition_info", "partition_saved_info", "update_partition_info", "finish_executor")
    SAVE_PARTITION_INFO_FIELD_NUMBER: _ClassVar[int]
    PARTITION_SAVED_INFO_FIELD_NUMBER: _ClassVar[int]
    UPDATE_PARTITION_INFO_FIELD_NUMBER: _ClassVar[int]
    FINISH_EXECUTOR_FIELD_NUMBER: _ClassVar[int]
    save_partition_info: SavePartitionInfo
    partition_saved_info: PartitionSavedInformation
    update_partition_info: EventUpdatePartitionStage
    finish_executor: str
    def __init__(self, save_partition_info: _Optional[_Union[SavePartitionInfo, _Mapping]] = ..., partition_saved_info: _Optional[_Union[PartitionSavedInformation, _Mapping]] = ..., update_partition_info: _Optional[_Union[EventUpdatePartitionStage, _Mapping]] = ..., finish_executor: _Optional[str] = ...) -> None: ...

class Fanout(_message.Message):
    __slots__ = ("stage_id", "fanout")
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    FANOUT_FIELD_NUMBER: _ClassVar[int]
    stage_id: int
    fanout: int
    def __init__(self, stage_id: _Optional[int] = ..., fanout: _Optional[int] = ...) -> None: ...

class ExecutorInformation(_message.Message):
    __slots__ = ("executor_id",)
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    executor_id: int
    def __init__(self, executor_id: _Optional[int] = ...) -> None: ...

class PartitionKey(_message.Message):
    __slots__ = ("key", "executor_id")
    KEY_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    key: str
    executor_id: int
    def __init__(self, key: _Optional[str] = ..., executor_id: _Optional[int] = ...) -> None: ...

class SaveCloudRequestDone(_message.Message):
    __slots__ = ("request_send",)
    REQUEST_SEND_FIELD_NUMBER: _ClassVar[int]
    request_send: bool
    def __init__(self, request_send: bool = ...) -> None: ...

class CloseExecutorEvent(_message.Message):
    __slots__ = ("event",)
    EVENT_FIELD_NUMBER: _ClassVar[int]
    event: str
    def __init__(self, event: _Optional[str] = ...) -> None: ...

class PingRequest(_message.Message):
    __slots__ = ("message",)
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    message: str
    def __init__(self, message: _Optional[str] = ...) -> None: ...

class PingResponse(_message.Message):
    __slots__ = ("reply", "timestamp")
    REPLY_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    reply: str
    timestamp: int
    def __init__(self, reply: _Optional[str] = ..., timestamp: _Optional[int] = ...) -> None: ...

class EmptyResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class BooleanResponse(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bool
    def __init__(self, value: bool = ...) -> None: ...
