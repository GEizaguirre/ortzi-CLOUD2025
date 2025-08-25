import gc
from io import BytesIO
from typing import List, Tuple
import os
import random
import string

import polars as pl
import numpy as np

from ortzi.config import DEFAULT_OUT_PARTITIONS
from ortzi.io_utils.read_terasort_data import read_terasort_data
from ortzi.io_utils.info import IOInfo


class _DataFrame():

    """
    Internal DataFrame structure for ortzi (not distributed).
    """

    def __init__(self, dataframe: pl.DataFrame = None):
        self.dataframe = dataframe
        self.partitions: np.ndarray = None

    def __str__(self):
        return str(self.dataframe)

    def __len__(self):
        return len(self.dataframe)


def concat_progressive(
    dataframe_list: List[pl.DataFrame]
) -> pl.DataFrame:

    """
    Concatenates a list of polars DataFrames into one.
    """

    df = None

    # We do not concat all dataframes at once to avoid memory issues
    # TODO: choose concatenation method based on memory
    for r_i, _ in enumerate(dataframe_list):

        if len(dataframe_list[r_i]) > 0:

            new_chunk = dataframe_list[r_i]

            # Remove data for memory efficiency
            dataframe_list[r_i] = b""
            gc.collect()

            if df is None:
                df = new_chunk
            else:
                df = pl.concat([df, new_chunk])

    return _DataFrame(df)


def scan_csv(
    data: memoryview,
    io_info: IOInfo,
    low_memory: bool = False
) -> _DataFrame:

    """
    Scans a CSV input into a polars DataFrame.
    """

    print(
        "Scanning CSV data for partition %d, low memory %s" % (
        io_info.partition_id, low_memory
        )
    )

    if low_memory:
        fname = f"/tmp/{''.join(random.choices(string.ascii_uppercase, k=5))}"
        with open(fname, "wb") as f:
            f.write(data)
        df = pl.read_csv(
            fname,
            n_threads=1,
            low_memory=True,
            use_pyarrow=True,
            **io_info.parse_args
        )
        os.remove(fname)

    else:
        try:
            df = pl.read_csv(
                BytesIO(data),
                n_threads=1,
                use_pyarrow=True,
                **io_info.parse_args
            )
            data = b""
        except Exception as e:
            print(f"Error reading CSV data: {e}")
            raise e

    print(
        "Got CSV data for partition %d" % (
        io_info.partition_id
        )
    )

    return _DataFrame(df)


def scan_text(data: memoryview, io_info: IOInfo) -> _DataFrame:

    """
    Currently focused only on WordCount (or CountWords) cases.
    """

    words = data.tobytes().decode()

    if "eol_char" in io_info.parse_args:
        words = words.split(io_info.parse_args["eol_char"])
        words = [word for word in words if len(word) > 0]

    df = pl.DataFrame({"0": words})

    return _DataFrame(df)


def scan_terasort(
    partition_bytes: bytes,
    io_info: IOInfo
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:

    num_partitions = io_info.out_partitions
    if num_partitions is None or num_partitions == 0:
        num_partitions = DEFAULT_OUT_PARTITIONS
    keys, values, partitions = read_terasort_data(
        partition_bytes,
        num_partitions
    )

    return keys, values, partitions
