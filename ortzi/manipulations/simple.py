from typing import List
from polars import DataFrame


def _select(
    df: DataFrame,
    column_names: List[str]
):
    return df.select(column_names)


def _unique(
    df: DataFrame,
):
    return df.unique()


def _drop(
    df: DataFrame,
    column_names: List[str]
):
    return df.drop(column_names)
