from functools import partial

import polars as pl


def _max(col: str, over: str, alias: str):

    def transform_func(df: pl.DataFrame, col: str, over: str, alias: str):
        return df.with_columns(
            pl.col(col).max().over(over).alias(alias)
        )

    return partial(transform_func, col=col, over=over, alias=alias)


def _n_unique(col: str, over: str, alias: str):

    def transform_func(df: pl.DataFrame, col: str, over: str, alias: str):
        return df.with_columns(
            pl.col(col).n_unique().over(over).alias(alias)
        )
    return partial(transform_func, col=col, over=over, alias=alias)
