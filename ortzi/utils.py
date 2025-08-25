from enum import Enum
import random
import string
import math
from typing import (
    Any,
    Union,
    List
)
import logging
import configparser
import os

from ortzi.config import (
    CONFIG_FILE,
    ORTZI_DEFAULT_SECTION,
    config
)


def setup_debugging():
    logging.basicConfig(level=logging.DEBUG)


class Colors(Enum):
    READ = "#FF8080"
    READ_EXCHANGE = "#FFC0D9"
    WRITE = "#AFC8AD"
    WRITE_EXCHANGE = "#FF90BC"
    TRANSFORMATION = "#F6FDC3"
    JOIN = "#E1ACAC"
    PARTITION = "#FFB6C1"


class AsciiColors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    RED = '\033[91m'
    PEACHY_PASTEL = '\033[38;5;217m'


def color_string(s: str, color: Union[AsciiColors, List[AsciiColors]]):
    if not isinstance(color, list):
        color = [color]
    _s = ""
    for c in color:
        _s += c
    _s += s
    for c in color:
        _s += AsciiColors.ENDC
    return _s


def print_color(s: str, color: Union[AsciiColors, List[AsciiColors]]):
    print(color_string(s, color))


def rands(N: int = 3):
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(N))


def partition_from_prefix(prefix, num_workers, num_partitions):
    prefix = prefix.split("_")[0]
    prefixes = config.get("prefixes")
    position = prefixes.index(prefix)
    partition_ids = [
        position * num_workers + i
        for i in range(num_partitions)
    ]
    return partition_ids


def is_equal_with_resolution(expected: float, result: float) -> bool:
    resolution = abs(expected) * 1e-4  # Adjust based on precision needs
    return math.isclose(
        expected,
        result,
        rel_tol=resolution,
        abs_tol=resolution
    )


def ensure_config_file():
    if not os.path.exists(CONFIG_FILE):
        os.makedirs(
            os.path.dirname(CONFIG_FILE),
            exist_ok=True
        )
        with open(CONFIG_FILE, 'w') as f:
            f.write('')


def set_config_value(
    varname: str,
    value: Any
):

    config_parser = configparser.ConfigParser()
    ensure_config_file()
    config_parser.read(CONFIG_FILE)
    section = ORTZI_DEFAULT_SECTION
    if not config_parser.has_section(section):
        config_parser.add_section(section)
    config_parser.set(section, varname, str(value))
    with open(CONFIG_FILE, 'w') as f:
        config_parser.write(f)


def get_config_value(
    varname: str
) -> Any:

    config_parser = configparser.ConfigParser()
    config_parser.read(CONFIG_FILE)
    section = ORTZI_DEFAULT_SECTION
    if config_parser.has_option(section, varname):
        return config_parser.get(section, varname)
    else:
        return None
