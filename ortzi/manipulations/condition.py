from functools import partial
from typing import List

import polars as pl


def _condition(cols: List[str], func: callable):

    cols = [pl.col(col) for col in cols]

    def _condition_function(df: pl.DataFrame, vars):
        return df.filter(func(*vars))

    return partial(_condition_function, vars=cols)
