import time
import os
from typing import (
    Any,
    Dict
)

import numpy as np

from ortzi.backends.backend import BackendType
from ortzi.config import ORTZI_TMP_DIR

LOGGER_VERSION = 1.0

LOG_DIR = "logs"


def get_execution_logs_dir():

    if not os.path.exists(ORTZI_TMP_DIR):
        os.makedirs(ORTZI_TMP_DIR)
    log_dir = os.path.join(
        ORTZI_TMP_DIR,
        LOG_DIR
    )
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    return log_dir


class Logger:
    start_time: float = None
    end_time: float = None
    execution_time: float = None

    def __init__(self):
        self.start_time = time.time()

    def end(self):
        self.end_time = time.time()
        self.execution_time = self.end_time - self.start_time

    def to_dict(self) -> Dict[str, Any]:
        return self.__dict__


class TaskLogger(Logger):

    def __init__(
        self,
        task_id: int,
        stage_id: int,
        executor_id: int,
        worker_id: int,
        execution_backend: BackendType
    ):
        super().__init__()

        self.task_id = task_id
        self.stage_id = stage_id
        self.executor_id = executor_id
        self.worker_id = worker_id
        self.execution_backend = execution_backend.value

        self.read_sizes = np.array([], dtype=np.int64)
        self.deserialization_times = np.array([], dtype=np.float32)
        self.read_times = np.array([], dtype=np.float32)

        self.write_sizes = np.array([], dtype=np.int64)
        self.serialization_times = np.array([], dtype=np.float32)
        self.write_times = np.array([], dtype=np.float32)

        self.start_time: float = None
        self.end_time: float = None

        self.input_start_time: float = None
        self.input_end_time: float = None

        self.manipulate_start_time: float = None
        self.manipulate_end_time: float = None

        self.partition_start_time: float = None
        self.partition_end_time: float = None

        self.output_start_time: float = None
        self.output_end_time: float = None

    def init_time(self, type: str):
        if type == 'task':
            self.start_time = time.time()
        elif type == 'input':
            self.input_start_time = time.time()
        elif type == 'manipulate':
            self.manipulate_start_time = time.time()
        elif type == 'partition':
            self.partition_start_time = time.time()
        elif type == 'output':
            self.output_start_time = time.time()

    def finalize_time(self, type: str):
        if type == 'task':
            self.end_time = time.time()
        elif type == 'input':
            self.input_end_time = time.time()
        elif type == 'manipulate':
            self.manipulate_end_time = time.time()
        elif type == 'partition':
            self.partition_end_time = time.time()
        elif type == 'output':
            self.output_end_time = time.time()

    def get_time(self, type: str) -> float:
        if type == 'task':
            return (
                self.end_time - self.start_time
                if self.end_time and self.start_time
                else None
            )
        elif type == 'input':
            return (
                self.input_end_time - self.input_start_time
                if self.input_start_time and self.input_end_time
                else None
            )
        elif type == 'manipulate':
            return (
                self.manipulate_end_time - self.manipulate_start_time
                if self.manipulate_start_time and self.manipulate_end_time
                else None
            )
        elif type == 'partition':
            return (
                self.partition_end_time - self.partition_start_time
                if self.partition_end_time and self.partition_start_time
                else None
            )
        elif type == 'output':
            return (
                self.output_end_time - self.output_start_time
                if self.output_start_time and self.output_end_time
                else None
            )
        else:
            return None

    def add_io_stats(
        self,
        sizes: np.ndarray,
        ser_times: np.ndarray,
        io_times: np.ndarray,
        new_sizes: np.ndarray,
        new_ser_times: np.ndarray,
        new_io_times: np.ndarray
    ):
        sizes = np.concatenate((sizes, new_sizes))
        ser_times = np.concatenate((ser_times, new_ser_times))
        io_times = np.concatenate((io_times, new_io_times))
        return sizes, ser_times, io_times

    def add_read_stats(
        self,
        new_sizes: np.ndarray,
        new_deser_times: np.ndarray,
        new_io_times: np.ndarray
    ):

        self.read_sizes, self.deserialization_times, self.read_times = \
            self.add_io_stats(
                self.read_sizes,
                self.deserialization_times,
                self.read_times,
                new_sizes,
                new_deser_times,
                new_io_times
            )

    def add_write_stats(
        self,
        new_sizes: np.ndarray,
        new_ser_times: np.ndarray,
        new_io_times: np.ndarray
    ):
        self.write_sizes, self.serialization_times, self.write_times = \
            self.add_io_stats(
                self.write_sizes,
                self.serialization_times,
                self.write_times,
                new_sizes,
                new_ser_times,
                new_io_times
            )

    def to_dict(self) -> Dict[str, Any]:
        obj_dict = self.__dict__
        return obj_dict
