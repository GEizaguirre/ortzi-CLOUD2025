from typing import (
    Dict,
    List,
    Any
)

from networkx import DiGraph
import networkx as nx

from ortzi.backends.backend import BackendType
from ortzi.logical.descriptors import OperationDescriptor
from ortzi.task import (
    Task,
    TaskInfo
)

from asciidag.graph import Graph
from asciidag.node import Node
from dataclasses import dataclass, field


@dataclass
class _DAGStageWrapper:
    stage_id: int
    task: Task
    input_descriptor: Dict[Any, OperationDescriptor]
    output_descriptor: OperationDescriptor
    task_infos: List[TaskInfo] = field(default_factory=list)
    fanout: int = None
    synchronous: bool = False
    autoscale: bool = False
    scale_backend: BackendType = None
    data_conservative: bool = True
    backend: BackendType = None
    runtime_memory: int = None
    data_size: Any = None

    def __str__(self):
        return "Stage %d:\n" % (self.stage_id)


class Dag:

    def __init__(self):
        self.tree = DiGraph()
        self.stages: Dict[int, _DAGStageWrapper] = dict()

    def add_stage(
        self,
        stage_id: int,
        task: Task,
        input_descriptors: Dict[int, OperationDescriptor],
        output_descriptor: OperationDescriptor,
        task_infos: List[TaskInfo] = [],
        fanout: int = None,
        synchronous: bool = False,
        autoscale: bool = False,
        scale_backend: BackendType = BackendType.MULTIPROCESSING,
        data_conservative: bool = True,
        backend: BackendType = None,
        runtime_memory: int = None
    ):

        stage = _DAGStageWrapper(
            stage_id,
            task,
            input_descriptors,
            output_descriptor,
            task_infos,
            synchronous=synchronous,
            autoscale=autoscale,
            scale_backend=scale_backend,
            data_conservative=data_conservative,
            backend=backend,
            runtime_memory=runtime_memory
        )
        if fanout is not None:
            stage.fanout = fanout
        self.stages[stage_id] = stage
        self.tree.add_node(stage_id)

    def add_task_infos(
        self,
        stage_id: int,
        task_infos: List[TaskInfo]
    ):

        self.stages[stage_id].task_infos = task_infos

    def add_dependency(
        self,
        parent_stage_id: int,
        child_stage_id: int
    ):

        self.tree.add_edge(
            parent_stage_id,
            child_stage_id
        )

    def set_fanout(
        self,
        stage_id: int,
        fanout: int
    ):

        self.stages[stage_id].fanout = fanout

    def get_source_stage_ids(self):

        return [
            n for n in self.tree.nodes()
            if self.tree.in_degree(n) == 0
        ]

    def get_bfs_tree(
        self,
        stage_id: int
    ):

        tree = nx.bfs_tree(
            self.tree,
            stage_id
        )
        return tree

    def get_immediate_predecessors(
        self,
        stage_id: int,
        include_auxiliaries=True
    ) -> List[int]:

        return list(self.tree.predecessors(stage_id))

    def get_immediate_successors(
        self,
        stage_id: int
    ) -> List[int]:
        return list(self.tree.successors(stage_id))

    def __str__(self):

        return self.tree.__str__()

    def get_execution_order(self):

        return list(nx.topological_sort(self.tree))

    def get_distance(
        self,
        stage1: int,
        stage2: int
    ) -> int:

        undirected_graph = self.tree.to_undirected()
        distance = nx.shortest_path_length(
            undirected_graph,
            source=stage1,
            target=stage2
        )
        return distance

    def are_dependent(
        self,
        stage1: int,
        stage2: int
    ) -> bool:

        dep1 = nx.has_path(
            self.tree,
            source=stage1,
            target=stage2
        )

        dep2 = nx.has_path(
            self.tree,
            source=stage2,
            target=stage1
        )

        return dep1 or dep2

    def print_dag(self):

        node_map = {name: Node(str(name)) for name in self.tree.nodes()}

        for name, node_obj in node_map.items():
            parent_names = self.tree.predecessors(name)

            parent_nodes = [node_map[p_name] for p_name in parent_names]

            node_obj.parents = parent_nodes

        ascii_graph = Graph()
        print("\n--- DAG Visualization ---\n")
        ascii_graph.show_nodes(list(node_map.values()))
        print("-------------------------")
