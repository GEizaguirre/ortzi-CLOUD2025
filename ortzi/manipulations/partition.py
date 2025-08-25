import polars as pl
import pandas as pd
import numpy as np


def gather_partition(df: pl.DataFrame, num_partitions: int):
    return 0


def raw_partition(
    df: pl.DataFrame | pd.DataFrame,
    by: str,
    num_partitions: int
):
    if isinstance(df, pl.DataFrame):
        return df[by].hash() % num_partitions
    else:
        return df[by].apply(hash) % num_partitions


def partition_segments(
    df: pl.DataFrame | pd.DataFrame,
    by: str,
    segment_bounds: np.ndarray
) -> np.ndarray:

    col = df[by]
    return np.searchsorted(segment_bounds, col)
