from lithops.storage import Storage
from enum import Enum
import math

from ortzi.io_utils.external_utils import get_data_size
from ortzi.config import DEFAULT_PARTITION_SIZE
from ortzi.logical.descriptors import ReadDescriptor
from ortzi.logical.logical import LogicalPlan
from ortzi.physical.dag import Dag


class ProvisionerMode(Enum):
    PROACTIVE = 1


class ResourceProvisioner():

    def __init__(
        self,
        logical_plan: LogicalPlan,
        dag: Dag
    ):
        self.provisioner_mode: ProvisionerMode = None
        self.logical_plan = logical_plan
        self.dag = dag

    def infer_fanout(self, stage_id: int) -> int:
        pass

    def initialize_fanout(self, stage_id: int) -> int:
        pass

    def set_fanouts(self):

        for stage_id in self.dag.get_execution_order():
            stage = self.dag.stages[stage_id]
            if stage.fanout is None:
                if self.logical_plan.is_auxiliary(stage_id):
                    stage.fanout = 1
                else:
                    if stage.task_infos is None or len(stage.task_infos) == 0:
                        stage.fanout = self.initialize_fanout(stage_id)

    def is_source(self, stage_id: int) -> bool:
        return (
            len(self.dag.stages[stage_id].input_descriptor) < 3 and
            isinstance(
                self.dag.stages[stage_id].input_descriptor[-1],
                ReadDescriptor
            )
        )


class ProvisionerPartitionSize(ResourceProvisioner):

    def __init__(
        self,
        logical_plan: LogicalPlan,
        dag: Dag,
        partition_size: int = DEFAULT_PARTITION_SIZE,
        storage: Storage = None
    ):

        super().__init__(
            logical_plan,
            dag
        )
        if storage is None:
            self.storage = Storage()
        else:
            self.storage = storage
        self.partition_size = partition_size
        self.provisioner_mode = ProvisionerMode.PROACTIVE

    def infer_fanout(
        self,
        stage_id: int
    ):

        if self.dag.stages[stage_id].fanout is not None:
            return self.dag.stages[stage_id].fanout
        else:
            predecessor_ids = self.dag.get_immediate_predecessors(
                stage_id
            )
            predecessor_ids = [
                predecessor_id for predecessor_id in predecessor_ids
                if not self.logical_plan.is_auxiliary(predecessor_id)
            ]

            if len(predecessor_ids) > 0:
                fanout = 0
                for predecessor_id in predecessor_ids:
                    fanout += self.dag.stages[predecessor_id].fanout
                return fanout

            elif isinstance(
                self.dag.stages[stage_id].input_descriptor[-1],
                ReadDescriptor
            ):
                read_descriptor: ReadDescriptor = (
                    self.dag.stages[stage_id].input_descriptor[-1]
                )
                key = read_descriptor.key
                bucket = read_descriptor.bucket
                data_size = get_data_size(
                    self.storage,
                    bucket,
                    key
                )
                num_partitions = math.ceil(data_size / self.partition_size)
                return num_partitions

    def initialize_fanout(
        self,
        stage_id: int
    ) -> int:
        fanout = self.infer_fanout(stage_id)
        return fanout
