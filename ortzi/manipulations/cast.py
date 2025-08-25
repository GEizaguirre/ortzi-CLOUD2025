import polars as pl


def _to_datetime(*args):
    if len(args) == 1:
        return args[0].str.strptime(pl.Date)
    elif len(args) == 3:
        return pl.date(args[0], args[1], args[2])
