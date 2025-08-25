import time
from typing import (
    Dict,
    List,
    Tuple,
    Union
)
from io import BytesIO
import sys

import pickle
import numpy as np
import polars as pl
import pyarrow as pa
import pandas as pd

from ortzi.struct.dataframe import _DataFrame


def serialize_pickle(obj: object) -> bytes:

    serialized_bytes = pickle.dumps(obj)
    return serialized_bytes


def deserialize_pickle(serialized_bytes: bytes) -> object:

    obj = pickle.loads(serialized_bytes)
    return obj


def get_partition(
    partition_id: int,
    dataframe: pl.DataFrame | pd.DataFrame,
    hash_list: np.ndarray
):

    """
    Get a partition from a dataframe, given a previously
    calculated list of destinations per row.
    """

    pointers_ni = np.where(hash_list == partition_id)[0]
    if len(pointers_ni) == 0:
        pointers_ni = []
    if isinstance(dataframe, pl.DataFrame):
        partition = dataframe[pointers_ni]
    else:
        partition = dataframe.iloc[pointers_ni]
    return partition


def warmup_serialization():

    """
    Warmup serialization method.
    """

    dataframe = pl.DataFrame({"a": ["aaa", "bbb", "ccc"]})
    b = serialize(dataframe)
    _ = deserialize(b)


def serialize(dataframe: object) -> bytes:

    """
    Serialization method for queues, disk storage and external object storage.
    """

    if isinstance(dataframe, _DataFrame):
        _dataframe = dataframe.dataframe
    else:
        _dataframe = dataframe

    if isinstance(_dataframe, pl.DataFrame):
        obj = BytesIO()
        _dataframe.write_parquet(
            obj,
            compression="uncompressed",
            use_pyarrow=True
        )
        serialized_bytes = obj.getvalue()
    elif isinstance(_dataframe, pd.DataFrame):
        obj = BytesIO()
        _dataframe.to_parquet(
            obj,
            compression="snappy",
            engine="pyarrow"
        )
        serialized_bytes = obj.getvalue()
    else:
        serialized_bytes = serialize_pickle(_dataframe)

    return serialized_bytes


def deserialize(
    serialized_bytes: bytes
) -> pl.DataFrame | pd.DataFrame:

    """
    Deserialization method for queues, disk storage and external
    object storage.
    """
    try:
        io_obj = BytesIO(serialized_bytes)
        dataframe = pl.read_parquet(
            io_obj,
            parallel="none",
            use_pyarrow=True,
            memory_map=True
        )
    except pa.lib.ArrowInvalid:
        dataframe = deserialize_pickle(serialized_bytes)

    return dataframe


def serialize_memory(dataframe: pl.DataFrame) -> pa.RecordBatch:

    """
    Serialization method for memory.
    """
    if isinstance(dataframe, pd.DataFrame):
        raise TypeError(
            "DataFrame must be a polars DataFrame for memory serialization."
        )

    table = dataframe.to_arrow().combine_chunks()
    if len(dataframe) > 0:
        record_batch = table.to_batches(max_chunksize=sys.maxsize)[0]
        del table
    else:
        schema = table.schema
        record_batch = pa.record_batch([[] for _ in schema], schema=schema)

    return record_batch


def deserialize_memory(arrow_batch: pa.RecordBatch) -> pl.DataFrame:

    """
    Deserialization method for memory.
    """

    dataframe = pl.from_arrow(arrow_batch)

    return dataframe


def serialize_partitions(
    num_partitions: int,
    dataframe: _DataFrame,
    hash_list: Union[np.ndarray, int],
    destination_partitions: List[int] = None
) -> Tuple[Dict[int, bytes], List[int], List[float]]:

    serialized_partitions = {}
    serialization_sizes = []
    serialization_times = []

    if isinstance(hash_list, int):
        part_id = destination_partitions[0]
        serialized_partitions[part_id] = serialize(dataframe.dataframe)
    else:
        for partition in range(num_partitions):
            btime = time.time()
            serialization_result = serialize_partition(
                partition,
                dataframe.dataframe,
                hash_list
            )
            atime = time.time()
            if destination_partitions is not None:
                part_id = destination_partitions[partition]
            else:
                part_id = partition
            serialized_partitions[part_id] = serialization_result
            serialization_sizes.append(len(serialization_result))
            serialization_times.append(atime - btime)

    return (
        serialized_partitions,
        serialization_sizes,
        serialization_times
    )


def serialize_partition(
    partition_id: int,
    dataframe: pl.DataFrame,
    hash_list: np.ndarray
) -> bytes:

    partition = get_partition(
        partition_id,
        dataframe,
        hash_list
    )
    serialized_partition = serialize(partition)

    return serialized_partition


def serialize_partitions_memory(
    num_partitions: int,
    dataframe: _DataFrame,
    hash_list: np.ndarray,
    destination_partitions: List[int] = None
) -> Tuple[Dict[int, pa.RecordBatch], List[int], List[float]]:

    serialized_partitions = {}
    partition_sizes = []
    serialization_times = []

    if isinstance(hash_list, int):
        part_id = destination_partitions[0]
        btime = time.time()
        serialized_partitions[part_id] = serialize_memory(dataframe.dataframe)
        atime = time.time()
        partition_sizes.append(len(serialized_partitions[part_id]))
        serialization_times.append(atime - btime)
    else:
        for partition in range(num_partitions):
            btime = time.time()
            serialization_result = serialize_partition_memory(
                partition,
                dataframe.dataframe,
                hash_list
            )
            atime = time.time()
            if destination_partitions is not None:
                part_id = destination_partitions[partition]
            else:
                part_id = partition
            serialized_partitions[part_id] = serialization_result
            partition_sizes.append(len(serialization_result))
            serialization_times.append(atime - btime)

    return serialized_partitions, partition_sizes, serialization_times


def serialize_partition_memory(
        partition_id: int,
        dataframe: pl.DataFrame,
        hash_list: np.ndarray
) -> pa.RecordBatch:

    partition = get_partition(partition_id, dataframe, hash_list)
    serialized_partition = serialize_memory(partition)

    return serialized_partition
