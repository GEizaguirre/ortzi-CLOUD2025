from typing import (
    Dict,
    List,
    Union
)
from functools import partial

from lithops.storage import Storage

from ortzi.backends.backend import BackendType
from ortzi.io_utils.external_utils import get_data_size
from ortzi.io_utils.info import IOType
from ortzi.manipulations.partition import (
    gather_partition,
    raw_partition
)
from ortzi.manipulations.sample import partition_function
from ortzi.manipulations.manipulation import (
    ManipulationType,
    Manipulation
)


class StageDescriptor:

    _scale_up = BackendType.MULTIPROCESSING

    def __init__(self):
        self.stage_id: int = None
        self.input: Dict[int, OperationDescriptor] = dict()
        self.manipulations: List[ManipulationDescriptor] = []
        self.partition: PartitionDescriptor = None
        self.output: OperationDescriptor = None
        self.fanout: int = None
        self.synchronous: bool = False
        self.scale_backend = StageDescriptor._scale_up
        self.autoscale = True
        self.data_conservative = True
        # Force to execute the stage in a specific backend
        self.backend = None
        # Force to execute the stage in functions with a
        # specific runtime memory size
        self.runtime_memory = None

    def to_dict(self):

        input_dicts = {
            input_i: input.to_dict()
            for input_i, input in self.input.items()
        }

        d = {
            "stage_id": self.stage_id,
            "input": input_dicts,
            "output": self.output.to_dict() if self.output else None,
            "manipulations": [
                descriptor.to_dict()
                for descriptor in self.manipulations
            ],
            "partition": self.partition.to_dict() if self.partition else None,
        }

        return d


class OperationDescriptor:
    # Descriptors of operations, without number of partitions
    # specified and further fanout-dependent attributes
    # specified.
    # Create the logical plan of the pipeline.

    current_operation_id = 0

    def __init__(self, **kwargs):

        self.operation_id = OperationDescriptor.get_operation_id()
        self.name = "(%d) " % (self.operation_id)

    @classmethod
    def get_operation_id(cls) -> int:
        operation_id = cls.current_operation_id
        cls.current_operation_id += 1
        return operation_id

    def to_dict(self):
        return {
            "operation_id": self.operation_id,
            "name": self.name
        }


class ReadDescriptor(OperationDescriptor):

    def __init__(
        self,
        bucket: str,
        key: str,
        io_type: IOType,
        low_memory: bool = False,
        parse_args: Dict = dict(),
        eol_char: str = "\n",
        fanout: int = None,
        partition: bool = True,
        **kwargs
    ):

        super().__init__(**kwargs)

        self.name += f"Read_{io_type.value} [{key}]"
        self.bucket = bucket
        self.key = key
        self.io_type = io_type
        self.low_memory = low_memory
        self.parse_args = parse_args
        self.fanout = fanout
        self.eol_char = eol_char
        self.partition = partition

        # Input data size
        self.data_size = get_data_size(
            Storage(),
            bucket,
            key
        )
        self.sample = False

    def to_dict(self):
        parse_args_str = {
            k: (
                str(v)
                if not isinstance(v, (int, float, str, bool))
                else v
            )
            for k, v in self.parse_args.items()
        }

        d = {
            **super().to_dict(),
            "bucket": self.bucket,
            "key": self.key,
            "io_type": self.io_type.value,
            "low_memory": self.low_memory,
            "parse_args": parse_args_str,
            "fanout": self.fanout,
            "eol_char": self.eol_char,
            "data_size": self.data_size,
            "sample": self.sample
        }
        return d


class WriteDescriptor(OperationDescriptor):

    def __init__(
        self,
        bucket: str = None,
        key: str = None,
        collect: bool = False,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.name += "Write"
        self.bucket = bucket
        self.key = key
        self.collect = collect

    def to_dict(self):
        d = {
            **super().to_dict(),
            "bucket": self.bucket,
            "key": self.key
        }
        return d


class ExchangeWriteDescriptor(OperationDescriptor):

    current_exchange_id: int = 0

    def __init__(
        self,
        single_partition: bool = False,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.name += "ExchangeWrite"
        self.single_partition = single_partition
        self.exchange_id = ExchangeWriteDescriptor.get_exchange_id()

    @classmethod
    def get_exchange_id(cls) -> int:
        exchange_id = cls.current_exchange_id
        cls.current_exchange_id += 1
        return exchange_id

    def to_dict(self):
        d = {
            **super().to_dict(),
            "exchange_id": self.exchange_id
        }
        return d


class ExchangeReadDescriptor(OperationDescriptor):

    def __init__(
        self,
        source_exchange_ids: List[int],
        read_all: bool = False,
        fanout: int = None,
        **kwargs
    ):

        super().__init__(**kwargs)

        self.name += "ExchangeRead"
        self.source_exchange_ids = source_exchange_ids
        self.read_all = read_all
        self.fanout = fanout

        # if broadcast, every reducer reads all the data from all mappers
        if self.read_all:
            self.name += " (READ ALL)"

    def to_dict(self):
        d = {
            **super().to_dict(),
            "source_exchange_ids": self.source_exchange_ids,
            "broadcast": self.read_all,
            "fanout": self.fanout
        }
        return d


class PartitionDescriptor(OperationDescriptor):

    def __init__(
        self,
        by: Union[str, List[str]] = None,
        sample: bool = False,
        gather: bool = False,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.by = by
        self.gather = gather
        self.name += f"Partition [{by}]"

        if gather:
            self.partition_function = gather_partition
        elif not sample:
            self.partition_function = partial(raw_partition, by=by)
        else:
            self.partition_function = partial(partition_function, by=by)


class ManipulationDescriptor(OperationDescriptor):

    def __init__(
        self,
        manipulation: Manipulation,
        name: str = "",
        **kwargs
    ):

        super().__init__(**kwargs)
        if manipulation.type == ManipulationType.ACTION:
            self.name += f"Action [{name}]"
        elif manipulation.type == ManipulationType.TRANSFORMATION:
            self.name += f"Transformation [{name}]"
        elif manipulation.type == ManipulationType.MERGE:
            self.name += f"Merge [{name}]"
        else:
            self.name += f"Manipulation [{name}]"
        self.manipulation = manipulation
