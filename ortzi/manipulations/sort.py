from polars import DataFrame


def _sort(
    by: str,
    descending: bool,
    df: DataFrame
):
    null_last = not descending
    return df.sort(
        by=by,
        descending=descending,
        multithreaded=False,
        nulls_last=null_last
    )
