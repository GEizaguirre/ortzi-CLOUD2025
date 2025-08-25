from enum import Enum
from typing import Dict, List
from ortzi.config import KB


AUXILIARY_PREFIX = "aux"


class IOBackend(Enum):
    EXTERNAL = "external"
    MEMORY = "memory"
    QUEUE = "queues"
    DISK = "disk"


def iobackend_to_lithops(lithops_config: Dict, iobackend: IOBackend) -> str:
    if iobackend == IOBackend.EXTERNAL:
        return lithops_config["lithops"]["storage"]
    elif iobackend == IOBackend.DISK:
        return "localhost"
    else:
        raise NotImplementedError(f"IOBackend {iobackend} not implemented")


class PartitionInfo:

    def __init__(
        self,
        exchange_id: int,
        partition_ids: List[int],
        source_tasks: int,
        num_partitions: int,
        num_read: int
    ):
        self.exchange_id = exchange_id
        self.partition_ids = partition_ids
        self.source_tasks = source_tasks
        self.num_partitions = num_partitions
        # If it's an output_info is the number of reads that will be done
        # If it's an input_info is the number of reads that has to be done
        self.num_read = num_read

    def __str__(self):
        return (f"PartitionInfo(exchange_id={self.exchange_id}, partition_ids={self.partition_ids}, "
                f"source_tasks={self.source_tasks}, num_partitions={self.num_partitions}, num_read={self.num_read})")


class AuxiliaryType(Enum):
    SEGMENTS = "segments"
    META = "meta"


class AuxiliaryInfo:

    # We used this, for instance, for segment data in sorts (sample results)    
    def __init__(
        self,
        exchange_id: int,
        auxiliary_type: AuxiliaryType
    ):
        self.exchange_id = exchange_id
        self.type = auxiliary_type


class IOType(Enum):
    CSV = "csv"
    TEXT = "text"
    TERASORT = "terasort"


class IOMargins(Enum):
    CSV = KB
    TEXT = 50


class CollectInfo:

    def __init__(
        self,
        bucket: str,
        key: str,
        partition_id: int
    ):
        
        self.bucket = bucket
        self.key = key
        self.partition_id = partition_id

    def __str__(self):
        return (f"CollectInfo(bucket={self.bucket}, key={self.key}, partition_id={self.partition_id})")


class IOInfo:

    def __init__(
        self,
        bucket: str,
        key: str,
        partition_id: int,
        num_partitions: int,
        io_type: IOType = IOType.CSV,
        low_memory: bool = False,
        parse_args: Dict = dict(),
        out_partitions: int = None
    ):

        self.bucket = bucket
        self.key = key
        self.partition_id = partition_id
        self.num_partitions = num_partitions
        self.low_memory = low_memory
        self.parse_args = parse_args
        self.io_type = io_type
        self.out_partitions = out_partitions

    def __str__(self):
        return (f"IOInfo(bucket={self.bucket}, key={self.key}, partition_id={self.partition_id}, "
                f"num_partitions={self.num_partitions}, io_type={self.io_type}, low_memory={self.low_memory}, "
                f"parse_args={self.parse_args}, out_partitions={self.out_partitions})")
