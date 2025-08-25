import copy
from typing import Dict

from ortzi.logical.descriptors import StageDescriptor


class LogicalPlan():

    def __init__(self, stages: Dict[int, StageDescriptor] = dict()):
        self.stages: Dict[int, StageDescriptor] = stages
        self.auxiliary_dependencies: Dict[int, int] = dict()

    def add_stage(
        self,
        stage_id: int,
        stage: StageDescriptor
    ):
        self.stages[stage_id] = stage

    def add_auxiliary_dependency(
        self,
        auxiliary_stage_id: int,
        consumer_stage_id: int
    ):
        self.auxiliary_dependencies[auxiliary_stage_id] = consumer_stage_id

    def is_auxiliary(
        self,
        stage_id: int
    ):
        return stage_id in self.auxiliary_dependencies.keys()

    def get_auxiliaries(
        self, stage_id: int
    ):
        aux = []
        for aux_id, consumer_id in self.auxiliary_dependencies.items():
            if consumer_id == stage_id:
                aux.append(aux_id)
        return aux

    def copy(self):
        copy_plan = LogicalPlan()
        copy_plan.stages = copy.deepcopy(self.stages)
        return copy_plan

    def merge(
        self,
        logical_plan2: "LogicalPlan"
    ):

        for stage_id, stage in logical_plan2.stages.items():
            self.stages[stage_id] = stage

        for aux_id, consumer_id in (
            logical_plan2.auxiliary_dependencies.items()
        ):
            self.auxiliary_dependencies[aux_id] = consumer_id
