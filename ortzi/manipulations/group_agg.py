from typing import List

import polars as pl
from polars.expr.expr import Expr


def _n_unique(col: str, alias: str):
    return pl.col(col).n_unique().alias(alias)


def _first(col: str, alias: str):
    return pl.col(col).first().alias(alias)


def _sum(col: str, alias: str):
    return pl.col(col).sum().alias(alias)


def _group(df: pl.DataFrame, by: str | List[str], aggs: List[Expr]):
    return df.group_by(by).agg(*aggs)
