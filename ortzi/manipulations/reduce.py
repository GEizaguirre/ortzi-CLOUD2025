from typing import Dict

import polars as pl


def _n_unique(data: Dict, df: pl.DataFrame, col: str, dest: str):
    data[dest] = df[col].n_unique()


def _sum(data: Dict, df: pl.DataFrame, col: str, dest: str):
    data[dest] = df[col].sum()
