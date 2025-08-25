import itertools
from typing import (
    Dict,
    List,
    Set,
    Tuple,
    Any
)

from ortzi.backends.executor_config import ExecutorConfig
from ortzi.manipulations.sample import (
    gen_sample_partitions_input
)
from ortzi.io_utils.info import (
    AuxiliaryInfo,
    AuxiliaryType,
    IOInfo,
    IOType,
    PartitionInfo,
    CollectInfo
)
from ortzi.config import (
    MAX_PARALLELISM
)
from ortzi.logical.descriptors import (
    ExchangeReadDescriptor,
    ExchangeWriteDescriptor,
    ReadDescriptor,
    StageDescriptor,
    WriteDescriptor
)
from ortzi.logical.logical import (
    LogicalPlan
)
from ortzi.physical.dag import (
    Dag,
    _DAGStageWrapper
)
from ortzi.physical.provisioning.resource_provisioner import (
    ResourceProvisioner
)
from ortzi.physical.utils import ThreadSafeStageCounter
from ortzi.task import (
    Task,
    TaskInfo
)


MAX_DISTANCE_STAGE = 3


class PhysicalPlan():

    '''
    Creates physical plan from logical plan at the beginning,
    and remains the same for the whole execution.
    '''

    current_task_info_id = 0

    def __init__(
        self,
        logical_plan: LogicalPlan,
        resource_provisioner: ResourceProvisioner = None,
        execution_config: ExecutorConfig = None
    ):
        self.logical_plan = logical_plan
        self.resource_provisioner = resource_provisioner
        self.written_exchanges = set()
        self.sample_stages = {}
        self.unsent_stages = set()
        self.pending_stages = set()
        self.finished_stages = set()
        self.running_tasks = 0
        self.stage_counters: Dict[int, ThreadSafeStageCounter] = {}
        self.collect_data: List[CollectInfo] = []
        self.execution_config = execution_config

        self.build_dag()

    def initialize_stage_counter(
        self,
        stage_id: int
    ) -> Dict[int, ThreadSafeStageCounter]:

        fanout = self.dag.stages[stage_id].fanout
        self.stage_counters[stage_id] = ThreadSafeStageCounter(
            fanout,
            asynchronous_perc=self.execution_config.asynchronous_perc
        )

    def build_dag(self) -> Dag:

        remaining_stage_ids = [
            stage_id
            for stage_id in self.logical_plan.stages.keys()
        ]
        written_exchanges = set()

        num_iteration = 0
        self.dag = Dag()

        while len(remaining_stage_ids) > 0 and num_iteration < 100:
            runnable_stages = _get_runnable_stages_exchanges(
                written_exchanges,
                remaining_stage_ids,
                self.logical_plan
            )

            for stage_id, parent_exchange_ids in runnable_stages.items():

                self.unsent_stages.add(stage_id)

                source_exchange_ids, \
                    auxiliary_exchange_ids = parent_exchange_ids

                stage = self.logical_plan.stages[stage_id]

                if isinstance(stage.output, ExchangeWriteDescriptor):
                    write_exchange_id = _get_write_exchange_id(stage.output)
                    written_exchanges.add(write_exchange_id)

                task = _stage_descriptor_to_task(stage)

                self.dag.add_stage(
                    stage_id,
                    task,
                    stage.input,
                    stage.output,
                    fanout=stage.fanout,
                    synchronous=stage.synchronous,
                    autoscale=stage.autoscale,
                    scale_backend=stage._scale_up,
                    data_conservative=stage.data_conservative,
                    backend=stage.backend,
                    runtime_memory=stage.runtime_memory
                )
                remaining_stage_ids.remove(stage_id)

                stage_ids = auxiliary_exchange_ids + source_exchange_ids
                for parent_exchange_id in stage_ids:
                    source_stage_id, source_stage = \
                        _get_stage_with_write_exchange_id(
                            parent_exchange_id,
                            self.logical_plan
                        )
                    if source_stage is not None:
                        self.dag.add_dependency(
                            source_stage_id,
                            stage_id
                        )

            num_iteration += 1

    def initialize_fanouts(self):
        self.resource_provisioner.set_fanouts()
        for stage_id, stage in self.dag.stages.items():
            self.logical_plan.stages[stage_id].fanout = stage.fanout

    def finished_stage(
        self,
        stage_id: int
    ):
        output_descriptor = self.dag.stages[stage_id].output_descriptor
        if isinstance(output_descriptor, ExchangeWriteDescriptor):
            exchange_id = output_descriptor.exchange_id
            self.written_exchanges.add(exchange_id)
        if stage_id in self.pending_stages:
            self.pending_stages.remove(stage_id)
        self.finished_stages.add(stage_id)

    def get_runnable_stages(self):

        runnable_stages = set()

        for stage_id in self.unsent_stages:
            predecessor_ids = self.dag.get_immediate_predecessors(stage_id)
            if len(predecessor_ids) == 0:
                runnable_stages.add(stage_id)
                continue
            runnable = True
            for predecessor_id in predecessor_ids:
                if predecessor_id in self.unsent_stages:
                    runnable = False
                    break
                if self.logical_plan.is_auxiliary(predecessor_id):
                    continue
                if not self.stage_counters[predecessor_id].async_end():
                    runnable = False
                    break
            if runnable:
                runnable_stages.add(stage_id)

        return runnable_stages

    def next_stage(self) -> Dict[int, _DAGStageWrapper]:

        runnable_stages = dict()

        if self.empty():
            return runnable_stages

        stage_ids = self.get_runnable_stages()

        for stage_id in stage_ids:
            if self.eligible_stage(stage_id):
                stage = self.dag.stages[stage_id]
                stage.task_infos = self.gen_task_infos(stage_id)
                runnable_stages[stage_id] = stage
                self.pending_stages.add(stage_id)
                self.initialize_stage_counter(stage_id)

        return runnable_stages

    def task_completed(
        self,
        stage_id: int
    ) -> int:
        # Counter update
        self.stage_counters[stage_id].increment()
        if self.stage_counters[stage_id].async_end():
            output_descriptor = self.dag.stages[stage_id].output_descriptor
            if isinstance(output_descriptor, ExchangeWriteDescriptor):
                exchange_id = output_descriptor.exchange_id
                self.written_exchanges.add(exchange_id)
        if self.stage_counters[stage_id].end():
            self.finished_stage(stage_id)
            return stage_id
        else:
            return None

    def eligible_stage(
        self,
        stage_id: int
    ) -> bool:

        if len(self.unsent_stages) == len(self.dag.stages):
            return True

        launched_stages = self.finished_stages | self.pending_stages
        source_stage_ids = []
        for source_stage_id in launched_stages:
            successor_ids = self.dag.get_immediate_successors(source_stage_id)
            if len(successor_ids) == 1:
                successor = self.dag.stages[successor_ids[0]]
                if successor.fanout == 1:
                    source_stage_ids.append(successor_ids[0])
            else:
                source_stage_ids.append(source_stage_id)

        for source_stage_id in source_stage_ids:
            distance = self.dag.get_distance(
                source_stage_id,
                stage_id
            )
            if distance < MAX_DISTANCE_STAGE:
                return True

        return False

    def gen_exchange_writes(self, stage_id: int):

        stage_wrapper = self.dag.stages[stage_id]
        output_descriptor: ExchangeWriteDescriptor = (
            stage_wrapper.output_descriptor
        )
        stage_fanout = stage_wrapper.fanout

        child_exchange_id = (
                _get_write_exchange_id(stage_wrapper.output_descriptor)
        )
        child_stage_ids = \
            _get_stages_with_read_exchange_id(
                child_exchange_id,
                self.logical_plan
            )
        # TODO: currently, only one child stage is accepted
        child_fanout = self.dag.stages[child_stage_ids[0]].fanout
        if output_descriptor.single_partition:
            num_partitions = stage_fanout
        else:
            num_partitions = child_fanout * stage_fanout
        output_infos = [
            PartitionInfo(
                child_exchange_id,
                partition_ids=[task_num],
                source_tasks=stage_fanout,
                num_partitions=num_partitions,
                num_read=child_fanout
            )
            for task_num in range(stage_fanout)
        ]
        for task_num, out in enumerate(output_infos):
            stage_wrapper.task_infos[task_num].output_info = out

    def signal_finished_stage(self, stage_id: int):

        stage_descriptor = self.logical_plan.stages[stage_id]
        exchange_id = _get_child_exchange_id(stage_descriptor)
        if exchange_id is not None:
            self.written_exchanges.add(exchange_id)

    def gen_task_id(self) -> int:
        task_id = PhysicalPlan.current_task_info_id
        PhysicalPlan.current_task_info_id += 1
        return task_id

    def gen_auxiliary_task_info(
        self,
        stage_id: int
    ) -> TaskInfo:

        stage_wrapper = self.dag.stages[stage_id]
        input_descriptor: ReadDescriptor = stage_wrapper.input_descriptor[-1]

        if input_descriptor.sample:
            obj_size = input_descriptor.data_size
            partition_ids, \
                num_partitions = gen_sample_partitions_input(obj_size)
        else:
            partition_ids = [0]
            num_partitions = 1

        input_info = {
            partition_id: IOInfo(
                bucket=input_descriptor.bucket,
                key=input_descriptor.key,
                partition_id=partition_id,
                num_partitions=num_partitions,
                io_type=input_descriptor.io_type,
                low_memory=input_descriptor.low_memory,
                parse_args=input_descriptor.parse_args,
                out_partitions=num_partitions
            )
            for partition_id in partition_ids
        }

        child_exchange_id = \
            _get_write_exchange_id(stage_wrapper.output_descriptor)
        if input_descriptor.sample:
            type = AuxiliaryType.SEGMENTS
        else:
            type = AuxiliaryType.META
        aux_output_info = AuxiliaryInfo(
            exchange_id=child_exchange_id,
            auxiliary_type=type
        )

        return [
            TaskInfo(
                stage_id=stage_id,
                task_id=(stage_id * MAX_PARALLELISM) + 0,
                input_info=input_info,
                output_info=None,
                auxiliary_output_info=aux_output_info,
                backend=stage_wrapper.backend,
                runtime_memory=stage_wrapper.runtime_memory
            )
        ]

    def gen_task_infos(self, stage_id: int) -> List[TaskInfo]:

        if self.logical_plan.is_auxiliary(stage_id):
            return self.gen_auxiliary_task_info(stage_id)

        stage_wrapper = self.dag.stages[stage_id]
        output_descriptor = stage_wrapper.output_descriptor
        input_descriptor = stage_wrapper.input_descriptor
        stage_fanout = stage_wrapper.fanout

        if isinstance(output_descriptor, ExchangeWriteDescriptor):
            child_exchange_id = \
                    _get_write_exchange_id(stage_wrapper.output_descriptor)
            child_stage_ids = \
                _get_stages_with_read_exchange_id(
                    child_exchange_id,
                    self.logical_plan
                )
            # TODO: currently, only one child stage is accepted
            child_fanout = self.dag.stages[child_stage_ids[0]].fanout
            if child_fanout is not None:
                if output_descriptor.single_partition:
                    num_partitions = stage_fanout
                else:
                    num_partitions = child_fanout * stage_fanout
                output_infos = [
                    PartitionInfo(
                        child_exchange_id,
                        partition_ids=[task_num],
                        source_tasks=stage_fanout,
                        num_partitions=num_partitions,
                        num_read=child_fanout
                    )
                    for task_num in range(stage_fanout)
                ]
            else:
                output_infos = [None for _ in range(stage_fanout)]

        elif isinstance(output_descriptor, WriteDescriptor):
            if output_descriptor.collect:
                output_infos = [
                    CollectInfo(
                        output_descriptor.bucket,
                        output_descriptor.key,
                        task_num
                    )
                    for task_num in range(stage_fanout)
                ]
                self.collect_data.extend(output_infos)
            else:
                child_fanout = None
                output_infos = [
                    IOInfo(
                        bucket=output_descriptor.bucket,
                        key=output_descriptor.key,
                        partition_id=task_num,
                        num_partitions=stage_fanout
                    )
                    for task_num in range(stage_fanout)
                ]
        else:
            output_infos = [
                None for _ in range(stage_fanout)
            ]

        input_infos = [
            {} for _ in range(stage_fanout)
        ]
        for in_i, input_descriptor in stage_wrapper.input_descriptor.items():
            if isinstance(input_descriptor, ExchangeReadDescriptor):
                parent_exchange_ids = _get_read_exchange_ids(
                    input_descriptor
                )
                parent_stage_ids = [
                    _get_stage_with_write_exchange_id(
                        exchange_id,
                        self.logical_plan
                    )[0]
                    for exchange_id in parent_exchange_ids
                ]
                parent_fanouts = [
                    self.dag.stages[parent_stage_id].fanout
                    for parent_stage_id in parent_stage_ids
                ]
                exchange_num = 0
                exchange_id = parent_exchange_ids[0]
                if input_descriptor.read_all:
                    num_partitions = parent_fanouts[exchange_num]
                    partition_ids = list(range(num_partitions))
                else:
                    partition_ids = None
                    num_partitions = (
                        parent_fanouts[exchange_num] * stage_fanout
                    )
                for task_num in range(stage_fanout):
                    input_infos[task_num][in_i] = PartitionInfo(
                            exchange_id=exchange_id,
                            partition_ids=[
                                task_num + (parent_num * stage_fanout)
                                for parent_num
                                in range(parent_fanouts[exchange_num])
                            ] if partition_ids is None else partition_ids,
                            source_tasks=parent_fanouts[exchange_num],
                            num_partitions=num_partitions,
                            num_read=1
                        )

            elif isinstance(input_descriptor, ReadDescriptor):

                io_type = input_descriptor.io_type
                out_partitions = child_fanout \
                    if io_type == IOType.TERASORT else None

                if input_descriptor.partition:
                    partition_id = None
                    num_partitions = stage_fanout
                else:
                    partition_id = 0
                    num_partitions = 1

                for task_num in range(stage_fanout):
                    input_infos[task_num][in_i] = IOInfo(
                            bucket=input_descriptor.bucket,
                            key=input_descriptor.key,
                            partition_id=(
                                partition_id
                                if partition_id is not None
                                else task_num
                            ),
                            num_partitions=num_partitions,
                            io_type=io_type,
                            low_memory=input_descriptor.low_memory,
                            parse_args=input_descriptor.parse_args,
                            out_partitions=out_partitions
                    )

        auxiliary_infos = self.logical_plan.get_auxiliaries(stage_id)
        if len(auxiliary_infos) > 0:
            # only one auxiliary stage accepted
            aux_id = auxiliary_infos[0]
            aux_stage = self.logical_plan.stages[aux_id]
            exchange_id = aux_stage.output.exchange_id
            if aux_stage.input[-1].sample:
                type = AuxiliaryType.SEGMENTS
            else:
                type = AuxiliaryType.META
            aux_input_info = AuxiliaryInfo(
                exchange_id=exchange_id,
                auxiliary_type=type
            )
        else:
            aux_input_info = None

        task_infos = [
            TaskInfo(
                stage_id=stage_id,
                task_id=(stage_id * MAX_PARALLELISM) + task_num,
                input_info=input_infos[task_num],
                output_info=output_infos[task_num],
                auxiliary_input_info=aux_input_info,
                backend=stage_wrapper.backend,
                runtime_memory=stage_wrapper.runtime_memory
            )
            for task_num in range(stage_wrapper.fanout)
        ]

        return task_infos

    def empty(self) -> bool:

        return len(self.unsent_stages) == 0

    def to_dict(self) -> dict:

        descriptor = []
        for stage_id, stage in self.logical_plan.stages.items():
            fanout = self.dag.stages[stage_id].fanout
            stage_descriptor = stage.to_dict()
            stage_descriptor["fanout"] = fanout
            descriptor.append(stage_descriptor)

        return descriptor

    def get_child_stages(
        self,
        stage_id: int
    ) -> List[int]:

        return [
            child_stage_id
            for child_stage_id in self.dag.tree.successors(stage_id)
        ]

    def get_required_fanout_old(self) -> int:

        required_fanout = 0
        for stage_id in self.pending_stages:
            stage = self.dag.stages[stage_id]
            if stage.autoscale:
                fanout = self.dag.stages[stage_id].fanout
                required_fanout += fanout
        return required_fanout

    def get_required_fanout(
        self,
        unsent_tasks: Set[int],
        pending_tasks: Set[int],
        sent_stage_ids: Set[int] = None
    ) -> dict:

        required_fanouts = {}

        # Find stages with all their tasks in unsent_tasks
        unsent_stage_ids = set(
            task_id // MAX_PARALLELISM
            for task_id in unsent_tasks
        )
        pending_stage_ids = set(
            task_id // MAX_PARALLELISM
            for task_id in pending_tasks
        )
        all_unsent_tasks = set(unsent_tasks)

        # (1) Stages with all their tasks in unsent_tasks
        for stage_id in unsent_stage_ids:
            stage = self.dag.stages.get(stage_id)
            if not stage or not stage.autoscale:
                continue
            fanout = stage.fanout
            stage_task_ids = set(
                (stage_id * MAX_PARALLELISM) + i for i in range(fanout)
            )
            if stage_task_ids.issubset(all_unsent_tasks):
                required_fanouts[stage_id] = fanout

        # (2) Stages that will be directly runnable when unsent and
        # pending tasks are finished
        finished_stages = (
            set(self.finished_stages) | pending_stage_ids | unsent_stage_ids
        )
        for stage_id in self.unsent_stages:
            stage = self.dag.stages.get(stage_id)
            if not stage or not stage.autoscale:
                continue
            predecessors = set(self.dag.get_immediate_predecessors(stage_id))
            if predecessors.issubset(finished_stages):
                required_fanouts[stage_id] = stage.fanout

        for stage_id, fanout in required_fanouts.items():
            required_fanouts[stage_id] = max(fanout, 1)

        if sent_stage_ids is not None:
            # Ensure that sent stages are not scaled down
            for sent_stage_id in sent_stage_ids:
                if sent_stage_id not in required_fanouts:
                    stage = self.dag.stages.get(sent_stage_id)
                    required_fanouts[sent_stage_id] = max(
                        stage.fanout, 1
                    )

        return required_fanouts


def _get_read_exchange_ids(
    descriptors: (
        ExchangeReadDescriptor |
        Dict[Any, ReadDescriptor | ExchangeReadDescriptor]
    )
) -> List[int]:

    if isinstance(descriptors, ExchangeReadDescriptor):
        return descriptors.source_exchange_ids
    else:
        source_exchange_ids = [
            descriptor.source_exchange_ids
            for _, descriptor in descriptors.items()
            if isinstance(descriptor, ExchangeReadDescriptor)
        ]

        return [
            source_id
            for sublist in source_exchange_ids
            for source_id in sublist
        ]


def _get_write_exchange_id(descriptor: ExchangeWriteDescriptor) -> int:

    return descriptor.exchange_id


def _get_parent_exchange_ids(descriptor: StageDescriptor) -> List[int]:

    return _get_read_exchange_ids(descriptor.input)


def _get_child_exchange_id(descriptor: StageDescriptor) -> List[int]:

    if (
        isinstance(descriptor.output, WriteDescriptor) or
        descriptor.output is None
    ):
        return None
    else:
        return _get_write_exchange_id(descriptor.output)


def _get_runnable_stages_exchanges(
    finished_exchanges: List[int],
    remaining_stage_ids: Set[int],
    logical_plan: LogicalPlan,
    pending_stages: Set[int] = set()
) -> Dict[int, List[int]]:

    '''
    A runnable stage is that with unfinished source exchanges
    '''

    runnable_stages = {}

    for stage_id in remaining_stage_ids:

        stage_descriptor = logical_plan.stages[stage_id]

        if stage_id in pending_stages:
            continue

        source_exchange_ids = _get_parent_exchange_ids(stage_descriptor)
        pendent_exchange_ids = [
            e for e in source_exchange_ids
            if e not in finished_exchanges
        ]
        if len(pendent_exchange_ids) == 0:
            # Check auxiliary stages
            auxiliaries = logical_plan.get_auxiliaries(stage_id)

            auxiliary_exchange_ids = [
                logical_plan.stages[aux_id].output.exchange_id
                for aux_id in auxiliaries
            ]
            pendent_aux_exchange_ids = [
                e for e in auxiliary_exchange_ids
                if e not in finished_exchanges
            ]
            if len(pendent_aux_exchange_ids) == 0:
                runnable_stages[stage_id] = (
                    source_exchange_ids,
                    auxiliary_exchange_ids
                )

    return runnable_stages


def _get_stage_with_write_exchange_id(
        exchange_id: int,
        logical_plan: LogicalPlan) -> Tuple[int, StageDescriptor]:

    for stage_id, stage_descriptor in logical_plan.stages.items():
        if (
            isinstance(stage_descriptor.output, ExchangeWriteDescriptor) and
            exchange_id == stage_descriptor.output.exchange_id
        ):
            return stage_id, stage_descriptor
    return None, None


def _get_stages_with_read_exchange_id(
        exchange_id: int,
        logical_plan: LogicalPlan
) -> List[int]:

    stages = []
    for stage_id, stage_descriptor in logical_plan.stages.items():
        for input in stage_descriptor.input.values():
            if (
                isinstance(input, ExchangeReadDescriptor) and
                exchange_id in input.source_exchange_ids
            ):
                stages.append(stage_id)
    return stages


class TaskCounter:
    task_counter = 0


def _stage_descriptor_to_task(stage_descriptor: StageDescriptor) -> Task:

    manipulations = [
        descriptor.manipulation
        for descriptor in stage_descriptor.manipulations
    ]

    if stage_descriptor.partition is None:
        partition_function = None
    else:
        partition_function = stage_descriptor.partition.partition_function
    task_id = TaskCounter.task_counter
    TaskCounter.task_counter += 1

    task = Task(
        manipulations,
        partition_function=partition_function
    )
    task.task_id = task_id

    return task
