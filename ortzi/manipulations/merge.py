from typing import (
    Dict,
    List
)

import polars as pl
import pandas as pd

from ortzi.struct.dataframe import _DataFrame


def _isin(
    data_1: int,
    data_2: int,
    left_on: str,
    right_on: str
):

    def isin_func(df_dict: Dict[int, _DataFrame]):
        df_dict[data_1].dataframe = \
              df_dict[data_1].dataframe.filter(
                  pl.col(left_on).is_in(
                      df_dict[data_2].dataframe[right_on]
                      )
                )
    return isin_func


def _join(
    data_1: int,
    data_2: int,
    left_on: str,
    right_on: str
):

    def polars_join_func(
        dataframe1: pl.DataFrame,
        dataframe2: pl.DataFrame
    ):

        return dataframe1.join(
                dataframe2,
                left_on=left_on,
                right_on=right_on,
                how="inner"
            )

    def pandas_join_func(
        dataframe1: pd.DataFrame,
        dataframe2: pd.DataFrame
    ):

        return dataframe1.merge(
                dataframe2,
                left_on=left_on,
                right_on=right_on,
                how="inner"
            )

    def join_func(df_dict: Dict[int, _DataFrame]):

        dataframe1 = df_dict[data_1].dataframe
        dataframe2 = df_dict[data_2].dataframe
        if (
            isinstance(dataframe1, pl.DataFrame) and
            isinstance(dataframe2, pl.DataFrame)
        ):
            df_dict[data_1].dataframe = polars_join_func(
                dataframe1,
                dataframe2
            )
        elif (
            isinstance(dataframe1, pd.DataFrame) and
            isinstance(dataframe2, pd.DataFrame)
        ):
            df_dict[data_1].dataframe = pandas_join_func(
                dataframe1,
                dataframe2
            )
        else:
            if isinstance(dataframe1, pd.DataFrame):
                dataframe1 = pl.from_pandas(dataframe1)
            if isinstance(dataframe2, pd.DataFrame):
                dataframe2 = pl.from_pandas(dataframe2)
            df_dict[data_1].dataframe = polars_join_func(
                dataframe1,
                dataframe2
            )

    return join_func


def _concatenate(
    data_indexes: List[int] = None,
    destination_index: int = -1
):
    
    if data_indexes is not None:
        def concatenate_func(df_dict: Dict[int, _DataFrame]):
            if destination_index not in df_dict:
                df_dict[destination_index] = _DataFrame()
            df_dict[destination_index].dataframe = pl.concat(
                [df_dict[i].dataframe for i in data_indexes]
            )
        return concatenate_func
    else:
        def concatenate_func_all(df_dict: Dict[int, _DataFrame]):
            if destination_index not in df_dict:
                concat_indexes = list(df_dict.keys())
                df_dict[destination_index] = _DataFrame()
            else:
                concat_indexes = list(df_dict.keys())
            df_dict[destination_index].dataframe = pl.concat(
                [df_dict[i].dataframe for i in concat_indexes]
            )
        return concatenate_func_all

