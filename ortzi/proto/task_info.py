from typing import Tuple, List
from dataclasses import dataclass

import cloudpickle as pickle

from ortzi.task import TaskInfo
from ortzi.event import (
    EventAddPartitionStage,
    EventUpdatePartitionStage
)
import ortzi.proto.services_pb2 as pb2
from ortzi.io_utils.info import (
    AuxiliaryInfo,
    AuxiliaryType,
    IOInfo,
    IOType,
    PartitionInfo,
    CollectInfo
)


def io_info_msg_to_IOInfo(io_info_grpc: pb2.IOInformation) -> IOInfo:

    serialized_args = io_info_grpc.parse_args

    pyth_args = pickle.loads(serialized_args)
    if io_info_grpc.io_type == IOType.CSV.value:
        type_io = IOType.CSV
    elif io_info_grpc.io_type == IOType.TERASORT.value:
        type_io = IOType.TERASORT
    else:
        type_io = IOType.TEXT

    input_info = IOInfo(
        bucket=io_info_grpc.bucket,
        key=io_info_grpc.key,
        partition_id=io_info_grpc.partition_id,
        num_partitions=io_info_grpc.num_partitions,
        io_type=type_io,
        low_memory=io_info_grpc.low_memory,
        parse_args=pyth_args,
        out_partitions=io_info_grpc.out_partitions
    )

    return input_info


def io_msg_to_IOInfo(io_msg: pb2.IOMessage) -> Tuple[int, IOInfo]:

    io_info_grpc = io_msg.io_info

    io_info = io_info_msg_to_IOInfo(io_info_grpc)

    return io_msg.partition_id, io_info


def partition_msg_to_PartitionInfo(
    partition_grpc: pb2.PartitionInformation
) -> PartitionInfo:

    partition_info = PartitionInfo(
        exchange_id=partition_grpc.exchange_id,
        partition_ids=list(partition_grpc.partition_ids),
        source_tasks=partition_grpc.source_tasks,
        num_partitions=partition_grpc.num_partitions,
        num_read=partition_grpc.num_read
    )

    return partition_info


def io_msg_to_PartitionInfo(io_msg: pb2.IOMessage) -> PartitionInfo:

    partition_grpc = io_msg.partition_info

    partition_info = partition_msg_to_PartitionInfo(partition_grpc)

    return io_msg.partition_id, partition_info


def io_msg_to_CollectInfo(io_msg: pb2.IOMessage) -> CollectInfo:

    collect_info = CollectInfo(
        bucket=io_msg.collect_info.bucket,
        key=io_msg.collect_info.key,
        partition_id=io_msg.collect_info.partition_id
    )

    return io_msg.partition_id, collect_info


def grpc_msg_to_AuxiliaryInfo(
    aux_msg: pb2.AuxiliaryInformation
) -> AuxiliaryInfo:

    exchange_id = aux_msg.exchange_id
    if aux_msg.type == AuxiliaryType.META.value:
        aux_type = AuxiliaryType.META
    elif aux_msg.type == AuxiliaryType.SEGMENTS.value:
        aux_type = AuxiliaryType.SEGMENTS

    auxiliary_output_info = AuxiliaryInfo(
        exchange_id=exchange_id,
        auxiliary_type=aux_type
    )
    return auxiliary_output_info


def task_grpc_to_python(task_info: pb2.TaskInformation) -> TaskInfo:

    input_info = {}
    for input_grpc in task_info.input:
        if input_grpc.type == 1:
            input_i, input_python = io_msg_to_IOInfo(input_grpc)
        else:
            input_i, input_python = io_msg_to_PartitionInfo(input_grpc)
        input_info[input_i] = input_python

    output_grpc = task_info.output
    if output_grpc.type == 1:
        _, output_info = io_msg_to_IOInfo(output_grpc)
    elif output_grpc.type == 2:
        _, output_info = io_msg_to_PartitionInfo(output_grpc)
    elif output_grpc.type == 3:
        _, output_info = io_msg_to_CollectInfo(output_grpc)
    else:
        output_info = None

    if task_info.HasField('auxiliary_input_info'):
        auxiliary_input_info = \
            grpc_msg_to_AuxiliaryInfo(task_info.auxiliary_input_info)
    else:
        auxiliary_input_info = None

    if task_info.HasField('auxiliary_output_info'):
        auxiliary_output_info = \
            grpc_msg_to_AuxiliaryInfo(task_info.auxiliary_output_info)
    else:
        auxiliary_output_info = None

    task_info_python = TaskInfo(
        stage_id=task_info.stage_id,
        task_id=task_info.task_id,
        input_info=input_info,
        output_info=output_info,
        auxiliary_input_info=auxiliary_input_info,
        auxiliary_output_info=auxiliary_output_info
    )

    return task_info_python


def IOInfo_to_grpc_msg(
    io_info: IOInfo,
    partition_id: int = 0
) -> pb2.IOMessage:
    serialized_args = pickle.dumps(io_info.parse_args)
    grpc_io_info = pb2.IOInformation(
        bucket=io_info.bucket,
        key=io_info.key,
        partition_id=io_info.partition_id,
        num_partitions=io_info.num_partitions,
        io_type=io_info.io_type.value,
        low_memory=io_info.low_memory,
        parse_args=serialized_args,
        out_partitions=io_info.out_partitions
    )
    msg = pb2.IOMessage(
        partition_id=partition_id,
        type=1,
        io_info=grpc_io_info
    )
    return msg


def PartitionInfo_to_grpc_msg(
    partition_info: PartitionInfo,
    partition_id: int = 0
) -> pb2.IOMessage:
    grpc_partition_info = pb2.PartitionInformation(
        exchange_id=partition_info.exchange_id,
        partition_ids=partition_info.partition_ids,
        source_tasks=partition_info.source_tasks,
        num_partitions=partition_info.num_partitions,
        num_read=partition_info.num_read
    )
    msg = pb2.IOMessage(
        partition_id=partition_id,
        type=2,
        partition_info=grpc_partition_info
    )
    return msg


def CollectInfo_to_grpc_msg(
    collect_info: CollectInfo,
    partition_id: int = 0
) -> pb2.IOMessage:
    grpc_partition_info = pb2.CollectInformation(
        bucket=collect_info.bucket,
        key=collect_info.key,
        partition_id=collect_info.partition_id
    )
    msg = pb2.IOMessage(
        partition_id=partition_id,
        type=3,
        collect_info=grpc_partition_info
    )
    return msg


def AuxiliaryInfo_to_grpc_msg(auxiliary_info: AuxiliaryInfo):
    exchange_id = auxiliary_info.exchange_id
    grpc_aux_info = pb2.AuxiliaryInformation(
        exchange_id=exchange_id,
        type=auxiliary_info.type.value
    )
    return grpc_aux_info


def task_python_to_grpc(act_task: TaskInfo | int) -> pb2.TaskInformation:

    if act_task == -1:
        task_info = pb2.TaskInformation(stage_id=-1, task_id=-1)
        return task_info
    elif act_task == -2:
        task_info = pb2.TaskInformation(stage_id=-2, task_id=-2)
        return task_info

    inputs = []
    for input_i, python_input in act_task.input_info.items():

        if isinstance(python_input, IOInfo):
            grpc_input = IOInfo_to_grpc_msg(
                python_input,
                partition_id=input_i
            )
            inputs.append(grpc_input)
        else:
            grpc_input = PartitionInfo_to_grpc_msg(
                python_input,
                partition_id=input_i
            )
            inputs.append(grpc_input)

    task_info = pb2.TaskInformation(
        stage_id=act_task.stage_id,
        task_id=act_task.task_id,
        input=inputs
    )

    if isinstance(act_task.output_info, IOInfo):
        output_io_info = IOInfo_to_grpc_msg(act_task.output_info)
        task_info.output.CopyFrom(output_io_info)
    elif isinstance(act_task.output_info, PartitionInfo):
        output_io_info = PartitionInfo_to_grpc_msg(act_task.output_info)
        task_info.output.CopyFrom(output_io_info)
    elif isinstance(act_task.output_info, CollectInfo):
        output_io_info = CollectInfo_to_grpc_msg(act_task.output_info)
        task_info.output.CopyFrom(output_io_info)

    if act_task.auxiliary_input_info is not None:
        auxiliary_input_info =  \
            AuxiliaryInfo_to_grpc_msg(act_task.auxiliary_input_info)
        task_info.auxiliary_input_info.CopyFrom(auxiliary_input_info)

    if act_task.auxiliary_output_info is not None:
        auxiliary_output_info = \
            AuxiliaryInfo_to_grpc_msg(act_task.auxiliary_output_info)
        task_info.auxiliary_output_info.CopyFrom(auxiliary_output_info)

    return task_info


def event_add_p_to_grpc(event):
    event_grpc = pb2.EventAddPartitionStage(
        key=event.key,
        num_read=event.num_read,
        executor_id=event.executor_id,
        worker_id=event.worker_id,
        storage=event.storage
    )

    return event_grpc


def event_add_p_to_python(event):
    event_python = EventAddPartitionStage(
        key=event.key,
        num_read=event.num_read,
        worker_id=event.worker_id,
        executor_id=event.executor_id,
        storage=event.storage
    )
    return event_python


def event_upd_p_to_grpc(event):
    event_grpc = pb2.EventUpdatePartitionStage(
        key=event.key,
        num_read=event.num_read,
        executor_id=event.executor_id,
        worker_id=event.worker_id
    )

    return event_grpc


def event_upd_p_to_python(event):
    event_python = EventUpdatePartitionStage(
        key=event.key,
        num_read=event.num_read,
        worker_id=event.worker_id,
        executor_id=event.executor_id
    )

    return event_python


@dataclass
class PartitionKey:
    key: str
    executor_id: int
    event_id: int = None


@dataclass
class TaskCompletedInfo:
    task_id: int
    stage_id: int
    executor_id: int
    worker_id: int
    list_add_partition_events: List[EventAddPartitionStage]
    list_upd_partition_events: List[EventUpdatePartitionStage]
    event_id: int = None


@dataclass
class RequestPartition:
    key: str
    worker_id: int
    exchange_id: int
    partition_id: int
    executor_id: int
    event_id: int = None


def proto_sched_msg_to_python(proto_msg):
    """Converts a Scheduler protobuf message to a Python-native dataclass."""
    if isinstance(proto_msg, pb2.PartitionKey):
        return PartitionKey(
            key=proto_msg.key,
            executor_id=proto_msg.executor_id
        )
    elif isinstance(proto_msg, pb2.TaskCompletedInfo):

        return TaskCompletedInfo(
            task_id=proto_msg.task_id,
            stage_id=proto_msg.stage_id,
            executor_id=proto_msg.executor_id,
            worker_id=proto_msg.worker_id,
            list_add_partition_events=[
                EventAddPartitionStage(
                    key=event.key,
                    num_read=event.num_read,
                    executor_id=event.executor_id,
                    worker_id=event.worker_id,
                    storage=event.storage
                ) for event in proto_msg.list_add_partition_events
            ],
            list_upd_partition_events=[
                EventUpdatePartitionStage(
                    key=event.key,
                    num_read=event.num_read,
                    executor_id=event.executor_id,
                    worker_id_part=event.worker_id
                ) for event in proto_msg.list_upd_partition_events
            ]
        )
    elif isinstance(proto_msg, pb2.RequestPartition):
        return RequestPartition(
            key=proto_msg.key,
            worker_id=proto_msg.worker_id,
            exchange_id=proto_msg.exchange_id,
            partition_id=proto_msg.partition_id,
            executor_id=proto_msg.executor_id
        )
    else:
        raise ValueError(f"Unsupported proto message type: {type(proto_msg)}")
