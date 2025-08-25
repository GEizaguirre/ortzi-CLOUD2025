import polars as pl


def _flag(
    df: pl.DataFrame,
    func: callable,
    col: str,
    alias: str,
    bool_type: bool
):
    exp = func(pl.col(col))
    if not bool_type:
        exp = exp.cast(pl.Int8())
    exp = exp.alias(alias)
    df = df.with_columns(exp)
    return df
