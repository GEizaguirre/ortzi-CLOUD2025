from typing import Tuple, List
import polars as pl
import numpy as np
from ortzi.config import MAX_PARALLELISM, MB

# Sample size in MB
DEFAULT_SAMPLE_SIZE = 50
DEFAULT_SAMPLE_CHUNK_SIZE = 5


def segment(s: pl.Series, num_partitions: int = MAX_PARALLELISM):

    """
    Segment a column of keys into segments of
    approximate equal size.
    Returns an array with the boundaries of the segments.
    """

    if num_partitions == 1:
        return np.array([])

    quantiles = np.linspace(0, 1, num_partitions + 1)
    positions = quantiles * len(s)
    positions = positions.astype(np.int32)[1:-1]
    elements = s[positions]
    el_array = np.array(elements)
    el_array = el_array[~np.isnan(el_array)]
    return el_array


def sample(df: pl.DataFrame, by: str, descending: bool) -> np.ndarray:

    col = df[by]
    col.sort(in_place=True, descending=descending)
    return segment(s=col)


def gen_sample_partitions_input(
    obj_size: int,
    sample_size: int = DEFAULT_SAMPLE_SIZE
) -> Tuple[List[int], int]:

    obj_size = obj_size / MB
    num_possible_partitions = obj_size / DEFAULT_SAMPLE_CHUNK_SIZE
    num_partitions = sample_size / DEFAULT_SAMPLE_CHUNK_SIZE
    num_partitions = int(min(num_partitions, num_possible_partitions))
    partitions = np.random.choice(
        range(int(num_possible_partitions)),
        num_partitions,
        replace=False
    )
    return partitions, num_partitions


def partition_function(
    df: pl.DataFrame,
    by: str,
    segm_bounds: np.ndarray
):
    partition_indices = np.searchsorted(segm_bounds, df[by])

    return partition_indices
