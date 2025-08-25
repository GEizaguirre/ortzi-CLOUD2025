import collections
import copy
from functools import partial
from typing import (
    Dict,
    List,
    Tuple,
    Union,
    Callable
)
from enum import Enum
from dataclasses import dataclass

from networkx import DiGraph
from lithops import Storage
import polars as pl

from ortzi.backends.backend import BackendType
from ortzi.io_utils.info import IOType
from ortzi.client import Client
from ortzi.backends.executor_config import ExecutorConfig
from ortzi.manipulations.sort import _sort
from ortzi.manipulations import simple
from ortzi.manipulations.flag import _flag
from ortzi.manipulations.condition import _condition
from ortzi.manipulations import group_agg
from ortzi.manipulations import transform
from ortzi.manipulations import merge
from ortzi.manipulations import cast
from ortzi.manipulations import reduce
from ortzi.manipulations.sample import sample
from ortzi.manipulations.manipulation import (
    Manipulation,
    ManipulationType
)
from ortzi.logical.logical import LogicalPlan
from ortzi.physical.provisioning.resource_provisioner import (
    ProvisionerMode,
    ResourceProvisioner,
    ProvisionerPartitionSize
)
from ortzi.physical.physical import PhysicalPlan
from ortzi.logical.descriptors import (
    ExchangeReadDescriptor,
    ExchangeWriteDescriptor,
    PartitionDescriptor,
    ReadDescriptor,
    StageDescriptor,
    WriteDescriptor,
    ManipulationDescriptor
)
from ortzi.utils import rands


def get_valid_parse_args(parse_args: Dict) -> Dict:
    # This is necessart because 'dict_keys' type is not serializable
    for key, value in parse_args.items():
        if isinstance(value, collections.abc.KeysView):
            parse_args[key] = list(value)
        if isinstance(key, collections.abc.KeysView):
            new_key = list(key)
            parse_args[new_key] = parse_args.pop(key)
    return parse_args


class DataFrame:

    current_stage_id = 0

    def __init__(self):

        self.logical_plan = LogicalPlan()
        self.current_stage: StageDescriptor = None

    def set_synch(self, synchronous: bool):
        if synchronous:
            self.current_stage.synchronous = True

    @classmethod
    def _tune_stage(
        cls,
        stage: StageDescriptor,
        fanout: int = None,
        synchronous: bool = False,
        autoscale: bool = True,
        scale_backend: BackendType = None,
        backend: BackendType = None,
        runtime_memory: int = None
    ):
        if fanout is not None:
            stage.fanout = fanout
        if synchronous:
            stage.synchronous = True
        if not autoscale:
            stage.autoscale = False
        if scale_backend is not None:
            stage.scale_backend = scale_backend
        if runtime_memory is not None:
            stage.runtime_memory = runtime_memory
        if backend is not None:
            stage.backend = backend

    @classmethod
    def _gen_stage(cls) -> Tuple[int, StageDescriptor]:
        stage = StageDescriptor()
        stage_id = DataFrame._get_stage_id()
        stage.stage_id = stage_id
        return stage_id, stage

    @classmethod
    def _new_stage(
        cls,
        fanout: int = None,
        synchronous: bool = False,
        autoscale: bool = False,
        scale_backend: BackendType = None,
        backend: BackendType = None,
        runtime_memory: int = None
    ):

        stage_id, stage = DataFrame._gen_stage()
        DataFrame._tune_stage(
            stage,
            fanout=fanout,
            synchronous=synchronous,
            autoscale=autoscale,
            scale_backend=scale_backend,
            backend=backend,
            runtime_memory=runtime_memory
        )
        return stage_id, stage

    def _add_stage(
        self,
        stage_id: int,
        stage: StageDescriptor,
        current: bool = True
    ):
        self.logical_plan.add_stage(
            stage_id,
            stage
        )
        if current:
            self.current_stage = stage

    @classmethod
    def void(cls) -> 'DataFrame':
        stage_id, stage = DataFrame._new_stage()
        df = DataFrame()
        df._add_stage(stage_id, stage)
        return df

    @classmethod
    def from_text(
        cls,
        bucket: str,
        key: str,
        delimiter: str = b" ",
        eol_char: str = b" ",
        partition: bool = True,
        **kwargs
    ) -> 'DataFrame':

        stage_id, stage = DataFrame._new_stage(**kwargs)
        df = DataFrame()
        operator_descriptor = ReadDescriptor(
            bucket=bucket,
            key=key,
            io_type=IOType.TEXT,
            parse_args={
                "delimiter": delimiter,
                "eol_char": eol_char
            },
            partition=partition
        )
        stage.input[-1] = operator_descriptor
        df._add_stage(stage_id, stage)
        df.current_stage = stage

        return df

    @classmethod
    def from_csv(
        cls,
        bucket: str,
        key: str,
        has_header: bool = True,
        separator: str = ",",
        eol_char: str = "\n",
        new_columns: List[str] = None,
        parse_args: Dict = dict(),
        partition: bool = True,
        **kwargs
    ) -> 'DataFrame':

        stage_id, stage = DataFrame._new_stage(**kwargs)
        fanout = kwargs.pop("fanout", None)
        df = DataFrame()
        parse_args["new_columns"] = new_columns
        parse_args["has_header"] = has_header
        parse_args["separator"] = separator

        operator_descriptor = ReadDescriptor(
            bucket=bucket,
            key=key,
            io_type=IOType.CSV,
            eol_char=eol_char,
            parse_args=get_valid_parse_args(parse_args),
            fanout=fanout,
            partition=partition
        )
        stage.input[-1] = operator_descriptor
        df._add_stage(stage_id, stage)

        return df

    @classmethod
    def from_teragen(
        cls,
        bucket: str,
        key: str,
        partition: bool = True,
        **kwargs
    ) -> 'DataFrame':
        stage_id, stage = DataFrame._new_stage(**kwargs)
        fanout = kwargs.pop("fanout", None)
        df = DataFrame()

        operator_descriptor = ReadDescriptor(
            bucket=bucket,
            key=key,
            io_type=IOType.TERASORT,
            eol_char="\n",
            fanout=fanout,
            partition=partition
        )

        stage.input[-1] = operator_descriptor
        df._add_stage(stage_id, stage)
        return df

    def map(
        self,
        func: Callable | Manipulation,
        **kwargs
    ) -> 'DataFrame':

        DataFrame._tune_stage(
            self.current_stage,
            **kwargs
        )
        stage = self.current_stage
        stage_id = stage.stage_id
        df = self

        if isinstance(func, Callable):
            func = Manipulation(
                ManipulationType.TRANSFORMATION,
                func
            )
        man_descriptor = ManipulationDescriptor(
            func,
            name="map"
        )
        stage.manipulations.append(man_descriptor)
        stage.data_conservative = False
        df._add_stage(
            stage_id,
            stage
        )

        return df

    def select(
        self,
        column_names: List[str],
        **kwargs
    ) -> 'DataFrame':

        select_function = partial(simple._select, column_names=column_names)
        man = Manipulation(ManipulationType.TRANSFORMATION, select_function)
        man_descriptor = ManipulationDescriptor(
            man,
            name="select"
        )
        self.current_stage.manipulations.append(man_descriptor)
        self.current_stage.data_conservative = False
        DataFrame._tune_stage(
            self.current_stage,
            **kwargs
        )
        return self

    def purgue(
        self,
        data_ids: List[int],
    ) -> 'DataFrame':
        purgue_func = purgue(data_ids)
        man = Manipulation(ManipulationType.MERGE, purgue_func)
        descriptor = ManipulationDescriptor(man, "purgue")
        self.current_stage.manipulations.append(descriptor)
        self.current_stage.data_conservative = False
        return self

    def switch(
        self,
        data1: int,
        data2: int = None
    ) -> 'DataFrame':
        switch_func = switch(data1, data2)
        man = Manipulation(ManipulationType.MERGE, switch_func)
        descriptor = ManipulationDescriptor(man, "switch")
        self.current_stage.manipulations.append(descriptor)
        return self

    def drop(
        self,
        column_names: List[str],
        data_id: int = -1,
        **kwargs
    ) -> 'DataFrame':

        drop_func = partial(simple._drop, column_names=column_names)
        man = Manipulation(ManipulationType.TRANSFORMATION, drop_func, data_id)
        man_descriptor = ManipulationDescriptor(
            man,
            name="drop"
        )
        self.current_stage.manipulations.append(man_descriptor)
        self.current_stage.data_conservative = False
        DataFrame._tune_stage(
            self.current_stage,
            **kwargs
        )
        return self

    def unique(
        self,
        **kwargs
    ) -> 'DataFrame':
        man = Manipulation(simple._unique, name="unique")
        man_descriptor = ManipulationDescriptor(
            man,
            name="unique"
        )
        self.current_stage.manipulations.append(man_descriptor)
        self.current_stage.data_conservative = False
        DataFrame._tune_stage(
            self.current_stage,
            **kwargs
        )
        return self

    def gather(
        self,
        **kwargs
    ) -> 'DataFrame':
        partition_descriptor = PartitionDescriptor(gather=True)
        self.current_stage.partition = partition_descriptor
        write_exchange_descr = ExchangeWriteDescriptor()
        self.current_stage.output = write_exchange_descr

        stage_id, stage = DataFrame._new_stage(**kwargs)
        stage.fanout = 1
        read_exchange_descr = ExchangeReadDescriptor(
            source_exchange_ids=[write_exchange_descr.exchange_id]
        )
        stage.input[-1] = read_exchange_descr
        self._add_stage(stage_id, stage)

        return self

    def reduce(
        self,
        reduce_functions: List[Callable],
        **kwargs
    ) -> 'DataFrame':
        for reduce_function in reduce_functions:
            man = Manipulation(ManipulationType.ACTION, reduce_function)
            man_descriptor = ManipulationDescriptor(
                man,
                "reduce"
            )
            self.current_stage.manipulations.append(man_descriptor)
        self.current_stage.data_conservative = False
        DataFrame._tune_stage(
            self.current_stage,
            **kwargs
        )

        return self

    def sort(
        self,
        by: Union[str, List[str]],
        descending: bool = False,
        **kwargs
    ) -> 'DataFrame':
        input = self.current_stage.input[-1]
        is_terasort = isinstance(input, ReadDescriptor) and \
            input.io_type == IOType.TERASORT

        if not is_terasort:
            # In the special Terasort case, the partitioning
            # is done during the scan.
            sample_stage_id = self.sample(
                by,
                descending
            )
            self.logical_plan.add_auxiliary_dependency(
                sample_stage_id,
                self.current_stage.stage_id
            )
            partition_descriptor = PartitionDescriptor(by=by, sample=True)
            self.current_stage.partition = partition_descriptor
        else:
            by = "0"

        write_exchange_descr = ExchangeWriteDescriptor()
        self.current_stage.output = write_exchange_descr

        stage_id, stage = DataFrame._new_stage(**kwargs)
        read_exchange_descriptor = ExchangeReadDescriptor(
            source_exchange_ids=[write_exchange_descr.exchange_id]
        )
        stage.input[-1] = read_exchange_descriptor

        sort_function = partial(_sort, by=by, descending=descending)
        man_sort = Manipulation(ManipulationType.TRANSFORMATION, sort_function)
        reduce_descriptor = ManipulationDescriptor(
            man_sort,
            name="sort"
        )
        stage.manipulations = [reduce_descriptor]
        self._add_stage(stage_id, stage)

        return self

    def sample(
        self,
        by: str,
        descending: bool
    ):

        # Depending of input:
        if isinstance(self.current_stage.input[-1], ReadDescriptor):
            if self.current_stage.input[-1].io_type == IOType.TERASORT.value:
                return None
            else:

                sample_stage = StageDescriptor()
                sample_stage_id = DataFrame._get_stage_id()
                sample_stage.stage_id = sample_stage_id
                # Scan
                sample_stage.input[-1] = copy.deepcopy(
                    self.current_stage.input[-1]
                )
                sample_stage.input[-1].sample = True
                # Sample
                merge_op = Merge.concatenate()
                man_merge = Manipulation(ManipulationType.MERGE, merge_op)
                merge_descriptor = ManipulationDescriptor(
                    man_merge,
                    name="concatenate",
                )
                sample_function = partial(
                    sample,
                    by=by,
                    descending=descending
                )
                sample_descriptor = ManipulationDescriptor(
                    Manipulation(
                        ManipulationType.TRANSFORMATION,
                        sample_function
                    ),
                    name="sample"
                )
                sample_stage.manipulations = [
                    merge_descriptor,
                    sample_descriptor
                ]
                # Write
                write_descriptor = ExchangeWriteDescriptor()
                sample_stage.output = write_descriptor
                sample_stage.fanout = 1
                self.logical_plan.add_stage(
                    sample_stage_id,
                    sample_stage
                )
                return sample_stage_id

        elif isinstance(self.current_stage.input[-1], ExchangeReadDescriptor):
            # Not supported
            raise NotImplementedError(
                "Cannot perform a sort operation on partitioned data"
            )

    def groupby(
        self,
        by: str,
        aggs: callable | List[callable],
        transforms: callable | List[callable] = [],
        aggs2: callable | List[callable] = [],
        **kwargs
    ) -> 'DataFrame':

        if not isinstance(aggs, list):
            aggs = [aggs]
        group_func = partial(
            group_agg._group,
            by=by,
            aggs=aggs
        )
        man_group = Manipulation(
            ManipulationType.TRANSFORMATION,
            group_func
        )
        transformation_descriptor = ManipulationDescriptor(
            man_group,
            name="group"
        )
        self.current_stage.manipulations.append(
            transformation_descriptor
        )
        if not isinstance(transforms, list):
            transforms = [transforms]
        for t_i, tr in enumerate(transforms):
            man = Manipulation(ManipulationType.TRANSFORMATION, tr)
            transformation_descriptor = ManipulationDescriptor(
                man,
                name=f"transform_{t_i}"
            )
            self.current_stage.manipulations.append(
                transformation_descriptor
            )
        if len(transforms) > 0:
            self.current_stage.data_conservative = False

        partition_descriptor = PartitionDescriptor(by=by, sample=False)
        self.current_stage.partition = partition_descriptor
        write_exchange_descr = ExchangeWriteDescriptor()
        self.current_stage.output = write_exchange_descr

        stage_id, stage = DataFrame._new_stage(**kwargs)
        read_exchange_descr = ExchangeReadDescriptor(
            source_exchange_ids=[write_exchange_descr.exchange_id]
        )
        stage.input[-1] = read_exchange_descr
        if aggs2 is None:
            aggs2 = aggs
        if not isinstance(aggs2, list):
            aggs2 = [aggs2]
        for group_i, group_func in enumerate(aggs2):
            man_group = Manipulation(
                ManipulationType.TRANSFORMATION,
                group_func
            )
            transformation_descriptor = ManipulationDescriptor(
                group_func,
                name=f"group_{group_i}"
            )
            stage.manipulations.append(transformation_descriptor)
        if len(aggs2) > 0:
            stage.data_conservative = False
        self._add_stage(stage_id, stage)
        return self

    def filter(
        self,
        filter_function: callable,
        **kwargs
    ) -> 'DataFrame':
        DataFrame._tune_stage(
            self.current_stage,
            **kwargs
        )
        return self.map(filter_function)

    def merge(
        self,
        others: Dict[int, "MergeSource"] = dict(),
        merge_ops: callable | List[callable] = [],
        **kwargs
    ) -> 'DataFrame':

        if not isinstance(merge_ops, list):
            merge_ops = [merge_ops]

        for merge_i, merge_info in others.items():

            dataframe = merge_info.dataframe
            by = merge_info.on
            merge_pattern = merge_info.pattern

            if isinstance(dataframe, DataFrame):

                self.logical_plan.merge(dataframe.logical_plan)
                partition_descriptor = PartitionDescriptor(
                    by=by,
                    sample=False
                )
                dataframe.current_stage.partition = partition_descriptor

                write_exchange_descr = ExchangeWriteDescriptor(
                    single_partition=merge_pattern == MergePattern.ALL
                )
                dataframe.current_stage.output = write_exchange_descr

                read_exchange_descriptor = ExchangeReadDescriptor(
                    source_exchange_ids=[write_exchange_descr.exchange_id],
                    read_all=merge_pattern == MergePattern.ALL
                )
                self.current_stage.input[merge_i] = read_exchange_descriptor

        for merge_op in merge_ops:
            man_merge = Manipulation(ManipulationType.MERGE, merge_op)
            merge_descriptor = ManipulationDescriptor(
                man_merge,
                name="merge",
            )
            self.current_stage.manipulations.append(merge_descriptor)
        self.current_stage.data_conservative = False

        DataFrame._tune_stage(
            self.current_stage,
            **kwargs
        )
        return self

    def write(
        self,
        bucket: str,
        key: str,
        execution_config: ExecutorConfig,
        provisioner_mode: ProvisionerMode = ProvisionerMode.PROACTIVE,
        enhanced_logging: bool = True,
        clean: bool = True,
        debug: bool = False,
        log_file: str = None,
        pipeline_name: str = None,
        **kwargs
    ):

        write_descriptor = WriteDescriptor(
            bucket,
            key
        )
        self.current_stage.output = write_descriptor
        DataFrame._tune_stage(
            self.current_stage,
            **kwargs
        )

        self._run(
            execution_config,
            enhanced_logging=enhanced_logging,
            provisioner_mode=provisioner_mode,
            clean=clean,
            debug=debug,
            log_file=log_file,
            pipeline_name=pipeline_name
        )

    def collect(
        self,
        execution_config: ExecutorConfig,
        provisioner_mode: ProvisionerMode = ProvisionerMode.PROACTIVE,
        enhanced_logging: bool = False,
        clean: bool = True,
        debug: bool = False,
        log_file: str = None,
        pipeline_name: str = None
    ):
        bucket = execution_config.bucket
        key = rands(6)
        collect_desc = WriteDescriptor(bucket, key, collect=True)
        self.current_stage.output = collect_desc

        result = self._run(
            execution_config,
            enhanced_logging=enhanced_logging,
            provisioner_mode=provisioner_mode,
            clean=clean,
            debug=debug,
            log_file=log_file,
            pipeline_name=pipeline_name
        )

        return result

    def compute(
        self,
        execution_config: ExecutorConfig,
        provisioner_mode: ProvisionerMode = ProvisionerMode.PROACTIVE,
        enhanced_logging: bool = False,
        clean: bool = True,
        debug: bool = False,
        log_file: str = None,
        pipeline_name: str = None
    ):

        self._run(
            execution_config,
            enhanced_logging=enhanced_logging,
            provisioner_mode=provisioner_mode,
            clean=clean,
            debug=debug,
            log_file=log_file,
            pipeline_name=pipeline_name
        )

    def _run(
        self,
        execution_config: ExecutorConfig,
        enhanced_logging: bool = False,
        provisioner_mode: ProvisionerMode = ProvisionerMode.PROACTIVE,
        clean: bool = True,
        debug: bool = False,
        log_file: str = None,
        pipeline_name: str = None
    ):

        storage = Storage()
        physical_plan = PhysicalPlan(
            self.logical_plan,
            execution_config=execution_config
        )

        if isinstance(provisioner_mode, str):
            provisioner_mode = ProvisionerMode[provisioner_mode.upper()]
        print(f"Provisioner mode: {provisioner_mode.name}")

        if provisioner_mode == ProvisionerMode.PROACTIVE:
            provisioner = ProvisionerPartitionSize(
                self.logical_plan,
                physical_plan.dag,
                storage=storage
            )

        physical_plan.resource_provisioner = provisioner
        physical_plan.initialize_fanouts()

        if debug:
            stages_data = [
                {"Stage": s_i, "Fanout": s.fanout}
                for s_i, s in physical_plan.dag.stages.items()
            ]
            stages_df = pl.DataFrame(stages_data)
            print(stages_df)
            physical_plan.dag.print_dag()

        for stage_id, stage in physical_plan.dag.stages.items():
            execution_config.register_task(
                stage_id,
                stage.task
            )

        client = Client(
            execution_config,
            enhanced_logging,
            storage=storage,
            clean=clean,
            log_file=log_file
        )

        result = client.run(
            physical_plan
        )

        return result

    @classmethod
    def _get_stage_id(cls) -> int:
        stage_id = cls.current_stage_id
        cls.current_stage_id += 1
        return stage_id

    def __str__(self):
        # Collect and to string
        return ""

    def to_physical(
        self,
        oracle: ResourceProvisioner = None
    ) -> DiGraph:

        # TODO: dynamic physical plans
        physical_plan = PhysicalPlan(
            self.logical_plan,
            oracle
        )
        return physical_plan.dag

    def set_scaleup(self, backend: BackendType):
        StageDescriptor._scale_up = backend


class Agg:

    def n_unique(col: str, alias: str):
        return group_agg._n_unique(col, alias)

    def first(col: str, alias: str):
        return group_agg._first(col, alias)

    def sum(col: str, alias: str):
        return group_agg._sum(col, alias)


class Reduce:

    def n_unique(col: str, dest: str):
        return partial(
            reduce._n_unique,
            col=col,
            dest=dest
        )

    def sum(col: str, dest: str):
        return partial(
            reduce._sum,
            col=col,
            dest=dest
        )


class Transform:

    def max(col: str, over: str, alias: str):
        return transform._max(col, over, alias)

    def n_unique(col: str, over: str, alias: str):
        return transform._n_unique(col, over, alias)


class Ops:

    def flag(
        func: Callable,
        col: str,
        alias: str,
        bool_type: bool = False
    ):
        return partial(
            _flag,
            func=func,
            col=col,
            alias=alias,
            bool_type=bool_type
        )


class Condition:

    def condition(cols: str | List[str], func: Callable):

        if not isinstance(cols, list):
            cols = [cols]

        return _condition(cols, func)


class Merge:

    def isin(data_2: int, left_on: str, right_on: str, data_1: int = -1):
        return merge._isin(data_1, data_2, left_on, right_on)

    def join(data_2: int, left_on: str, right_on: str, data_1: int = -1):
        return merge._join(data_1, data_2, left_on, right_on)

    def concatenate(
        data_indexes: List[int] = None,
        destination_index: int = -1
    ):
        return merge._concatenate(
            data_indexes,
            destination_index
        )


class Cast:

    def to_datetime(*args):
        if len(args) == 1:
            return cast._to_datetime(*args)
        else:
            return cast._to_datetime(args[0], args[1], args[2])


class MergePattern(Enum):
    PARTIAL = "partial"
    ALL = "all"


@dataclass
class MergeSource:
    dataframe: DataFrame
    on: str
    pattern: MergePattern = MergePattern.PARTIAL


def purgue(data_ids: List[int]):

    def purgue_func(data: Dict[int, DataFrame]):
        for data_id in data_ids:
            del data[data_id]

    return purgue_func


def switch(data1: int, data2: int = None):

    if data2 is None:
        data2 = -1

    def switch_func(data: Dict[int, DataFrame]):
        data[data1], data[data2] = data[data2], data[data1]

    return switch_func
