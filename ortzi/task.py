from dataclasses import dataclass
from typing import (
    Union,
    Dict,
    Optional,
    List,
    Callable,
)
import sys

import numpy as np
import polars as pl

from ortzi.backends.backend import BackendType
from ortzi.config import DEFAULT_OUT_PARTITIONS
from ortzi.io_utils.queues import Queue
from ortzi.manipulations.manipulation import (
    Manipulation,
    ManipulationType,
)
from ortzi.manipulations.sample import segment
from ortzi.io_utils.io_handler import IOHandler
from ortzi.io_utils.info import (
    IOInfo,
    PartitionInfo,
    AuxiliaryInfo,
    IOBackend,
    AuxiliaryType,
    CollectInfo,
)
from ortzi.struct.dataframe import _DataFrame
from ortzi.logging.ui_log import (
    OrtziLogEmitter,
    get_logger,
)
from ortzi.logging.execution_log import TaskLogger
from ortzi.io_utils.read_terasort_data import remap_partitions


@dataclass
class TaskInfo:
    """
    Information about a task.

    Attributes:
        stage_id (int): ID of the Stage.
        task_id (int): ID of the task.
        input_info (Union[IOInfo, PartitionInfo]):
            Information about input data.
        output_info (Union[IOInfo, PartitionInfo]):
            Information about output data.
    """
    stage_id: int
    task_id: int
    input_info: Dict[int, IOInfo | PartitionInfo]
    output_info: Union[IOInfo, PartitionInfo, CollectInfo]
    auxiliary_input_info: Optional[AuxiliaryInfo] = None
    auxiliary_output_info: Optional[AuxiliaryInfo] = None
    backend: Optional[BackendType] = None
    runtime_memory: Optional[int] = None


class Task():
    """
    Represents a task to be executed.

    Attributes:
        df (Union[DataFrame, Dict[int, DataFrame]]):
            DataFrames used in the task.
        partitions (np.ndarray): Partitions of data.
    """

    def __init__(
        self,
        manipulations: Manipulation | List[Manipulation] = [],
        partition_function: Callable = None
    ):

        if not isinstance(manipulations, list):
            manipulations = [manipulations]
        self.manipulations = manipulations
        self.partition_function = partition_function
        self.task_info: TaskInfo = None
        self.task_id: int = None
        self.stage_id: int = None
        self.input_info = None
        self.output_info = None
        self.auxiliary_input_info: AuxiliaryInfo = None
        self.auxiliary_output_info: AuxiliaryInfo = None
        self.auxiliary_data = None
        self.io_handler = None
        self.act_shmem = None
        self.task_logger: TaskLogger = None
        self.logger: OrtziLogEmitter = None
        self.data: Union[_DataFrame, Dict[int, _DataFrame]]
        self.action_data = dict()
        self.partitions: np.ndarray

    def run(
        self,
        worker_id: int,
        executor_id: int,
        task_info: TaskInfo,
        io_handler: IOHandler,
        backend: BackendType,
    ):
        """
        Runs the task.

        Args:
            task_info (TaskInfo): Information about the task.
            io_handler (IOHandler): Handler for I/O operations.

        Returns:
            Storage backend (where the data is saved), storage
            input keys (keys used to read the partitions) output
            object reference (key of the output partitions), and
            data output (serialized data, useful when the queues
            are used to exchange information).
        """

        self.task_logger = TaskLogger(
            task_info.task_id,
            task_info.stage_id,
            executor_id,
            worker_id,
            backend
        )

        self.task_logger.init_time('task')
        self.logger = get_logger(
            f"Task{task_info.task_id}-" +
            f"{task_info.stage_id}"
        )
        print(
            "(%d) Running task %d" %
            (executor_id, task_info.task_id)
        )

        self.task_info = task_info
        self.task_id = task_info.task_id
        self.stage_id = task_info.stage_id
        self.input_info = task_info.input_info
        self.output_info = task_info.output_info
        if task_info.auxiliary_input_info is not None:
            self.auxiliary_input_info = \
                task_info.auxiliary_input_info
        if task_info.auxiliary_output_info is not None:
            self.auxiliary_output_info = \
                task_info.auxiliary_output_info
        self.io_handler = io_handler

        # Run steps
        try:
            storage_input_keys = self.input()
        except Exception as e:
            print(f"Error reading input for task {task_info.task_id}")
            print(e)
            raise e
        print(
            "(%d) Task %d read input" %
            (executor_id, task_info.task_id)
        )

        self.manipulate()
        print(
            "(%d) Task %d manipulated data" %
            (executor_id, task_info.task_id)
        )
        repartition = False
        self.partition(repartition)

        print(
            "(%d) Task %d partitioned data" %
            (executor_id, task_info.task_id)
        )

        (
            storage_output_backend,
            storage_output_key,
            data_output
        ) = self.output()

        print(
            "(%d) Task %d wrote output" %
            (executor_id, task_info.task_id)
        )

        del self.data

        self.task_logger.finalize_time('task')
        logs = self.task_logger.to_dict()

        print(
            "(%d) Finished task %d" %
            (executor_id, task_info.task_id)
        )

        return (
            storage_output_backend,
            storage_input_keys,
            storage_output_key,
            data_output,
            logs
        )

    def input(self):
        """
        Reads input data.

        Returns:
            Storage input keys used for reading the partitions.
        """

        self.task_logger.init_time('input')
        storage_input_keys = []

        if self.input_info is None:
            self.data = None

        elif isinstance(self.input_info, dict):

            self.data = {}
            self.act_shmem = []

            for data_i, input_info in self.input_info.items():

                if isinstance(input_info, IOInfo):
                    read_result = self.io_handler.scan_data(input_info)

                else:
                    read_result = self.io_handler.get_partitions(
                        self.task_id,
                        input_info
                    )

                io_sizes = read_result.partition_sizes
                deser_times = read_result.ser_times
                io_times = read_result.io_times
                self.task_logger.add_read_stats(
                    io_sizes,
                    deser_times,
                    io_times
                )
                self.data[data_i] = read_result.data
                self.act_shmem.extend(read_result.shmems)
                storage_input_keys.append(read_result.keys)

        else:
            self.data[-1] = _DataFrame(None)

        self.task_logger.finalize_time('input')

        if self.auxiliary_input_info is not None:
            aux_info = self.auxiliary_input_info
            self.auxiliary_data = \
                self.io_handler.get_auxiliary(aux_info)

        return storage_input_keys

    def manipulate(self):

        self.task_logger.init_time('manipulate')

        for man in self.manipulations:
            fn = man.func
            data_id = man.data_id
            if man.type == ManipulationType.TRANSFORMATION:
                if (
                    not self.auxiliary_data or
                    self.auxiliary_input_info.type != AuxiliaryType.META
                ):
                    if data_id is None:
                        d = fn()
                        data_id = -1
                    else:
                        d = fn(df=self.data[data_id].dataframe)
                    self.data[data_id] = _DataFrame(d)
                else:
                    df = self.data[data_id].dataframe
                    aux_data = self.auxiliary_data
                    self.data[data_id] = fn(df=df, aux_data=aux_data)
            elif man.type == ManipulationType.MERGE:
                fn(self.data)
            elif man.type == ManipulationType.ACTION:
                fn(self.action_data, self.data[data_id].dataframe)

        if len(self.action_data) > 0:
            self.data = self.action_data
        else:
            self.data = self.data[-1]

        self.task_logger.finalize_time('manipulate')

    def partition(
        self,
        repartition: bool = None
    ):
        """
        Partitions data.
        """
        self.task_logger.init_time('partition')

        part_func = self.partition_function

        if isinstance(self.data, dict):
            self.task_logger.finalize_time('partition')
            return

        if (
            self.data.partitions is None and
            part_func is not None
        ):
            df = self.data.dataframe
            stage_fanout = self.output_info.source_tasks
            total_partitions = self.output_info.num_partitions
            num_partitions = total_partitions // stage_fanout
            if (
                self.auxiliary_input_info is not None and
                self.auxiliary_input_info.type == AuxiliaryType.SEGMENTS
            ):
                # e.g. in sort
                base_segments = self.auxiliary_data
                segment_bounds = segment(
                    base_segments,
                    num_partitions
                )
                parts = part_func(
                    df=df,
                    segm_bounds=segment_bounds
                )
            else:
                # e.g. in groupby
                parts = part_func(
                    df=df,
                    num_partitions=num_partitions
                )
            self.partitions = parts
        else:
            if self.data.partitions is None or not repartition:
                self.partitions = self.data.partitions
            else:
                stage_fanout = self.output_info.source_tasks
                total_partitions = self.output_info.num_partitions
                num_partitions = total_partitions // stage_fanout
                self.partitions = remap_partitions(
                    self.data.partitions,
                    DEFAULT_OUT_PARTITIONS,
                    num_partitions
                )

        self.task_logger.finalize_time('partition')

    def output(self):

        self.task_logger.init_time('output')

        storage_backend = None
        output_obj_ref = None
        data_output = None

        # Exchange_id: data
        if self.auxiliary_output_info:
            write_result = self.io_handler.write_auxiliary(
                self.data,
                self.auxiliary_output_info
            )
            storage_backend = IOBackend.EXTERNAL.value

        elif isinstance(
            self.output_info,
            CollectInfo
        ):
            partition_id = self.output_info.partition_id
            output_obj_ref = f"{self.output_info.key}/{partition_id}"
            write_result = self.io_handler.write_object(
                self.data,
                self.output_info.bucket,
                output_obj_ref
            )
            storage_backend = IOBackend.EXTERNAL.value

        elif isinstance(
            self.output_info,
            IOInfo
        ):
            partition_id = self.output_info.partition_id
            output_info = self.output_info
            output_obj_ref = f"{self.output_info.key}/{partition_id}"
            write_result = self.io_handler.write_dataframe(
                self.data,
                output_info
            )
            storage_backend = IOBackend.EXTERNAL.value

        elif isinstance(
            self.output_info,
            PartitionInfo
        ):
            output_info = self.output_info
            write_result = self.io_handler.put_partitions(
                self.data,
                self.partitions,
                self.output_info
            )
            output_obj_ref = write_result.keys[0]
            data_output = write_result.data
            storage_backend = self.io_handler.io_backend.value

        self.task_logger.finalize_time('output')
        partition_sizes = write_result.partition_sizes
        ser_times = write_result.ser_times
        io_times = write_result.io_times
        self.task_logger.add_write_stats(
            partition_sizes,
            ser_times,
            io_times
        )

        return storage_backend, output_obj_ref, data_output


#  Auxiliary functions only for Task
def merge_dicts(
    d1,
    d2
):
    max_key = max(
        d1.keys(),
        default=0
    )
    for i, key in enumerate(sorted(d2.keys()), start=1):
        d1[max_key + i] = d2[key]
    return d1
