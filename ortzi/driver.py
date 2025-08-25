import collections
from dataclasses import dataclass
from enum import Enum
import math
import multiprocessing
import queue
from concurrent import futures
from multiprocessing.synchronize import Event as EventType
from multiprocessing.managers import BaseManager as ManagerType
from multiprocessing import (
    Queue,
    Manager,
    Process,
    Event
)
import threading
import time
from typing import (
    Dict,
    List,
    NamedTuple,
    Tuple,
    Union,
    Set
)

import grpc
from lithops import (
    FunctionExecutor,
    Storage
)
from lithops.utils import FuturesList
from lithops.future import ResponseFuture
from lithops.wait import (
    wait,
    get_result
)

from ortzi.backends.executor_config import (
    ExecutorConfig,
    ExecutorInfo,
    ExecutorInstance
)
from ortzi.physical.physical import PhysicalPlan
from ortzi.io_utils.info import (
    CollectInfo,
    IOBackend
)
from ortzi.backends.backend import (
    BackendType,
    is_container_backend
)
from ortzi.event import (
    EventStageFinishExecutor,
    EventStagePartitionSavedInfo,
    EventStageSavePartitionInfo,
    EventUpdatePartitionStage
)
from ortzi.executor import run_executor
from ortzi.logging.ui_log import get_logger
from ortzi.physical.provisioning.resource_provisioner import ProvisionerMode
from ortzi.proto.task_info import (
    PartitionInfo_to_grpc_msg,
    event_add_p_to_python,
    event_upd_p_to_grpc,
    event_upd_p_to_python,
    proto_sched_msg_to_python,
    task_python_to_grpc,
    PartitionKey,
    TaskCompletedInfo,
    RequestPartition
)
import ortzi.proto.services_pb2_grpc as services_pb2_grpc
import ortzi.proto.services_pb2 as services_pb2
from ortzi.task import TaskInfo
from ortzi.utils import (
    AsciiColors,
    color_string
)
from ortzi.config import (
    DEFAULT_DRIVER_LISTEN_IP,
    DEFAULT_DRIVER_PORT,
    MAX_PARALLELISM,
    MAX_SERVER_THREADS
)
from ortzi.backends.k8s import patch_k8s_invoker


class TaskState(Enum):
    UNSENT = 1
    SUBMITTED = 2
    FINISHED = 3


@dataclass
class ResourceProxy:
    id: int
    future: ResponseFuture | Process


class ExecutorState(Enum):
    ACTIVE = 0
    WAITING_FOR_FINALIZATION = 1
    FINISHED = 2


@dataclass
class ExecutorMeta:
    id: int
    backend: BackendType
    runtime_memory: int
    server_ip: str
    server_port: int
    stub: Union[services_pb2_grpc.ExecutorStub, Queue]
    state: ExecutorState
    workers: int


class ExecutionConstraint(NamedTuple):
    backend: str
    runtime_memory: int


@dataclass
class TaskControl:
    task_id: int
    status: TaskState
    executor_id: List[int]
    worker_id: int
    instance: TaskInfo


@dataclass
class PartitionInfo:
    storage: IOBackend
    num_read: int
    worker_id: int
    executor_id: int
    managed: bool


@dataclass
class ScaleEvent:
    type: BackendType
    runtime_memory: int
    num: int


@dataclass
class ProvisioningEvent:
    stage_id: int
    data_size: int


@dataclass
class ProvisioningState:
    parent_stage_ids: List[int]
    stage_sizes: Dict[int, List[int]]


WAIT_PROPORTION = 0.1


class ResourceProvisioner():

    def __init__(
        self,
        rprovisioner_q: Queue,
        physical_plan: PhysicalPlan
    ):
        self.logger = get_logger("ResourceProvisioner")
        self.rprovisioner_q = rprovisioner_q
        self.physical_plan = physical_plan
        self.provisioning_stages: Dict[int, ProvisioningState] = dict()
        self.provisioned_stages = set()
        driver_address = "127.0.0.1:%d" % (DEFAULT_DRIVER_PORT)
        self.channel = grpc.insecure_channel(target=driver_address)
        self.stub = services_pb2_grpc.SchedulerStub(self.channel)

    def run(self):
        while True:
            event = self.rprovisioner_q.get(block=True)
            if isinstance(event, ProvisioningEvent):
                stage_id = event.stage_id
                data_size = event.data_size
                self.initialize_provisioning(stage_id)
                self.update_provisioning(stage_id, data_size)

            elif event == "EXIT":
                self.logger.info(
                    color_string(
                        "Finalizing Resource Provisioner",
                        [AsciiColors.BOLD, AsciiColors.PEACHY_PASTEL]
                    )
                )
                self.channel.close()
                return 0

    def initialize_provisioning(
        self,
        stage_id: int
    ):
        dag = self.physical_plan.dag
        child_stage_ids = dag.get_immediate_successors(stage_id)
        for child_id in child_stage_ids:
            if child_id not in self.provisioning_stages.keys():
                parent_ids = dag.get_immediate_predecessors(child_id)
                self.provisioning_stages[child_id] = ProvisioningState(
                    parent_stage_ids=parent_ids,
                    stage_sizes={
                        parent_stage_id: [] for parent_stage_id in parent_ids
                    }
                )

    def update_provisioning(
        self,
        stage_id: int,
        data_size: int
    ):
        child_stage_ids = [
            child_stage_id
            for child_stage_id in self.provisioning_stages.keys()
            if stage_id in (
                self.provisioning_stages[child_stage_id]
                .parent_stage_ids
            )
        ]
        for child_stage_id in child_stage_ids:
            provisioning_state = self.provisioning_stages[child_stage_id]
            provisioning_state.stage_sizes[stage_id].append(data_size)
            if (
                self.physical_plan.dag.stages[child_stage_id].fanout is None
                and self.check_provisioning(provisioning_state)
            ):
                fanout = (
                    self.physical_plan
                    .resource_provisioner
                    .infer_fanout(child_stage_id)
                )
                self.physical_plan.dag.stages[child_stage_id].fanout = fanout
                fanout_message = services_pb2.Fanout(
                    stage_id=child_stage_id,
                    fanout=fanout
                )

                _ = self.stub.UpdateFanout(fanout_message)
                self.provisioned_stages.add(child_stage_id)
                self.provisioning_stages.pop(child_stage_id)

    def check_provisioning(
        self,
        provisioning_state: ProvisioningState
    ):

        for parent_stage_id in provisioning_state.parent_stage_ids:
            parent_stage = self.physical_plan.dag.stages[parent_stage_id]
            fanout = parent_stage.fanout
            if fanout is None:
                return False
            gathered_sizes = provisioning_state.stage_sizes[parent_stage_id]
            if len(gathered_sizes) < max(fanout * WAIT_PROPORTION, 1):
                return False
            else:
                if parent_stage.data_size is None:
                    current_size = sum(gathered_sizes)
                    fanout_proportion = len(gathered_sizes) / fanout
                    predicted_size = current_size / fanout_proportion
                    parent_stage.data_size = predicted_size
        return True


def runnable_executor(
    task_backend: BackendType,
    task_runtime_mem: int,
    executor_backend: BackendType,
    executor_runtime_mem: int
):
    compatible_backend = (
        task_backend is None or (
            task_backend == executor_backend
        )
    )
    compatible_runtime_mem = (
        task_runtime_mem is None or (
            executor_runtime_mem >= task_runtime_mem
        )
    )
    return compatible_backend and compatible_runtime_mem


def equivalent_executor(
    executor1_backend: BackendType,
    executor1_runtime_mem: int,
    executor2_backend: BackendType,
    executor2_runtime_mem: int
):
    equivalent_backend = (
        executor1_backend is None or (
            executor1_backend == executor2_backend
        )
    )
    equivalent_runtime_memory = (
        executor1_runtime_mem is None or (
            executor2_runtime_mem == executor1_runtime_mem
        )
    )
    return equivalent_backend and equivalent_runtime_memory


class Scheduler(services_pb2_grpc.SchedulerServicer):

    def __init__(
        self,
        physical_plan: PhysicalPlan,
        rmanager_q: Queue,
        pmanager_q: Queue,
        rprovisioner_q: Queue,
        scheduler_rm_q: Queue,
        scheduler_pm_q: Queue,
        registered_executors: Queue,
        data_collects: Queue,
        initial_fanout: int
    ):

        self.logger = get_logger("Scheduler")
        self.stages = physical_plan.dag.stages
        self.physical_plan = physical_plan
        self.current_fanout = initial_fanout
        self.task_control: Dict[int, TaskControl] = {}
        self.executor_meta: Dict[int, ExecutorMeta] = {}

        # Tasks with specific backend and memory requirements
        self.constrained_tasks_to_schedule: \
            Dict[ExecutionConstraint, Queue] = {}
        # Tasks without specific requirements
        self.default_tasks_to_schedule: Queue = queue.Queue()
        self.unsent_tasks: Set[int] = set()
        self.pending_tasks: List[int] = list()

        self.rmanager_q = rmanager_q
        self.pmanager_q = pmanager_q
        self.rprovisioner_q = rprovisioner_q
        self.scheduler_rm_q = scheduler_rm_q
        self.scheduler_pm_q = scheduler_pm_q
        self.registered_executors = registered_executors

        self.data_collects = data_collects
        self.event_counter = MonotonicCounter()
        self._unprocessed_responses = collections.deque()
        self._response_lock = threading.Lock()

        self.finalized_executors = set()
        self.update_tasks()
        self.end = False

    def finalize_executors(
        self,
        executors: Set[int] = None
    ):

        if executors is None:
            executor_d = self.executor_meta
        else:
            executor_d = {
                exec_id: self.executor_meta[exec_id]
                for exec_id in executors
            }

        for executor_id, executor_meta in executor_d.items():
            is_container = is_container_backend(executor_meta.backend)
            if is_container:
                finish_event = EventStageFinishExecutor(
                    mess="Close the connection to finish the executor"
                )
                executor_meta.stub.put(finish_event)

            else:
                try:
                    _ = executor_meta.stub.FinishExecutor(
                        services_pb2.EmptyResponse()
                    )
                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.UNAVAILABLE:
                        print(
                            f"Executor {executor_id} already finished."
                        )
                    else:
                        raise e
            executor_meta.state = ExecutorState.FINISHED

    def RegisterExecutorAddress(
        self,
        request: services_pb2.ExecutorAddress,
        ctx
    ):

        executor_id = request.executor_id
        backend_type = request.backend_type
        executor_server_ip = request.executor_server_ip
        executor_server_port = request.executor_server_port
        workers = request.workers
        runtime_memory = request.runtime_memory

        print(
            "Registering executor: %d (%d workers, %d MB)" % (
                executor_id,
                workers,
                runtime_memory
            )
        )

        if self.check_end():
            self.logger.info(f"Cancelling executor {request.executor_id} \
                    ({BackendType(backend_type)}) (no longer necessary)")
            return services_pb2.BooleanResponse(value=False)

        is_container = is_container_backend(backend_type)

        if is_container:
            # Queue used to notify when an event has to be sent
            # to a cloud function executor.
            stub = Queue()

        else:
            executor_address = '{}:{}'.format(
                executor_server_ip,
                executor_server_port
            )
            channel = grpc.insecure_channel(executor_address)
            stub = services_pb2_grpc.ExecutorStub(channel)

        self.executor_meta[executor_id] = ExecutorMeta(
            id=executor_id,
            backend=BackendType(backend_type),
            runtime_memory=runtime_memory,
            server_ip=executor_server_ip,
            server_port=executor_server_port,
            stub=stub,
            state=ExecutorState.ACTIVE,
            workers=workers
        )

        self.registered_executors.put(
            (
                BackendType(backend_type),
                executor_id
            )
        )

        return services_pb2.BooleanResponse(value=True)

    def pull_task(
        self,
        host_backend: BackendType,
        host_memory: int
    ) -> Tuple[int, bool]:

        next_task_id = None
        for constraint, q in self.constrained_tasks_to_schedule.items():
            valid = runnable_executor(
                constraint.backend,
                constraint.runtime_memory,
                host_backend,
                host_memory
            )
            if valid:
                try:
                    next_task_id = q.get(block=False)
                except queue.Empty:
                    continue

        if next_task_id is None:
            q = self.default_tasks_to_schedule
            try:
                next_task_id = q.get(block=False)
            except queue.Empty:
                if (
                    self.physical_plan.empty() and
                    len(self.unsent_tasks) == 0
                ):
                    return (-1, False)
                else:
                    return (-2, False)

        return (next_task_id, True)

    def next_task(
        self,
        num_tasks: int,
        executor_id: int
    ):
        executor_meta = self.executor_meta[executor_id]
        executor_backend = executor_meta.backend
        executor_memory = executor_meta.runtime_memory

        next_task_id, got_task = self.pull_task(
            executor_backend,
            executor_memory
        )
        if got_task and next_task_id == -1:
            self.default_tasks_to_schedule.put(-1)
            return None

        if not got_task:
            if next_task_id == -1:
                return -1
            elif next_task_id == -2 and num_tasks > 1:
                return None
            else:
                return -2

        self.unsent_tasks.remove(next_task_id)
        self.physical_plan.running_tasks += 1
        next_task_info = self.task_control[next_task_id].instance

        return next_task_info

    def get_feasible_executors(
        self,
        backend: BackendType,
        runtime_memory: int,
        lax: bool = True
    ) -> List[int]:

        compatible_executors = []
        for executor_id, executor_meta in self.executor_meta.items():

            executor_backend = executor_meta.backend
            executor_runtime_mem = executor_meta.runtime_memory
            if lax:
                compatible_func = runnable_executor
            else:
                compatible_func = equivalent_executor

            compatible = compatible_func(
                backend,
                runtime_memory,
                executor_backend,
                executor_runtime_mem
            )

            if compatible and executor_meta.state == ExecutorState.ACTIVE:
                compatible_executors.append(executor_id)

        return compatible_executors

    def get_optimized_executors(
        self,
        required_executors: dict
    ) -> dict:

        optimized_executors = {}

        backend_none_cases = {}
        memory_none_cases = {}
        none_none_case = 0

        keys_to_remove = []
        for key, fanout in required_executors.items():
            backend, memory = key
            if backend is None and memory is not None:
                backend_none_cases[key] = fanout
                keys_to_remove.append(key)
            elif backend is not None and memory is None:
                memory_none_cases[key] = fanout
                keys_to_remove.append(key)
            elif backend is None and memory is None:
                none_none_case = fanout
                keys_to_remove.append(key)
            else:
                optimized_executors[key] = fanout

        for key, fanout in memory_none_cases.items():
            backend, _ = key
            merged = False
            for optimized_key, _ in optimized_executors.items():
                optimized_backend, _ = optimized_key
                if backend == optimized_backend:
                    optimized_executors[optimized_key] += fanout
                    merged = True
                    break
            if not merged:
                optimized_executors[key] = fanout

        for key, fanout in backend_none_cases.items():
            _, memory = key
            merged = False
            for optimized_key, _ in optimized_executors.items():
                _, optimized_memory = optimized_key
                if optimized_memory is not None and optimized_memory >= memory:
                    optimized_executors[optimized_key] += fanout
                    merged = True
                    break
            if not merged:
                optimized_executors[key] = fanout

        if none_none_case > 0:
            if optimized_executors:
                first_key = next(iter(optimized_executors))
                optimized_executors[first_key] += none_none_case
            else:
                optimized_executors[(None, None)] = none_none_case

        return optimized_executors

    def scale(
        self,
        sent_stage_ids: List[int] = None,
        finished_stage_id: int = None
    ):

        required_fanouts = self.physical_plan.get_required_fanout(
            self.unsent_tasks,
            self.pending_tasks,
            sent_stage_ids=sent_stage_ids
        )
        if finished_stage_id is not None:
            required_fanouts[finished_stage_id] = 0

        required_executors = {}
        for stage_id, fanout in required_fanouts.items():
            stage = self.physical_plan.dag.stages[stage_id]
            scale_backend = stage.scale_backend
            runtime_memory = stage.runtime_memory
            key = (scale_backend, runtime_memory)
            if key not in required_executors:
                required_executors[key] = fanout
            else:
                required_executors[key] += fanout

        optimized_executors = self.get_optimized_executors(
            required_executors
        )

        for executor_key, fanout in optimized_executors.items():

            scale_backend, runtime_memory = executor_key

            compatible_executors = self.get_feasible_executors(
                scale_backend,
                runtime_memory,
                lax=True
            )
            current_fanout = sum(
                self.executor_meta[executor_id].workers
                for executor_id in compatible_executors
            )

            fanout_diff = fanout - current_fanout
            if fanout_diff > 0:
                print(
                    "Scaling up %d workers" % (abs(fanout_diff))
                )
                scale_event = ScaleEvent(
                    type=scale_backend,
                    runtime_memory=runtime_memory,
                    num=fanout_diff
                )
                self.rmanager_q.put(scale_event)
            elif fanout_diff < 0:
                print(
                    "Scaling down %d workers" % (abs(fanout_diff))
                )
                compatible_executors = self.get_feasible_executors(
                    scale_backend,
                    runtime_memory,
                    lax=False
                )
                executor_data = [
                    (executor_id, self.executor_meta[executor_id].workers)
                    for executor_id in compatible_executors
                ]
                executor_data.sort(key=lambda x: x[1])
                executors_remove = set()
                sum_exec = 0
                for executor in executor_data[1:]:
                    sum_exec += executor[1]
                    if sum_exec <= abs(fanout_diff):
                        executors_remove.add(executor[0])
                for executor_id in executors_remove:
                    self.executor_meta[executor_id].state = (
                        ExecutorState.WAITING_FOR_FINALIZATION
                    )
                print(
                    "Finalizing executors:",
                    executors_remove,
                    " from scale down"
                )
                self.finalize_executors(executors_remove)
                self.finalized_executors.update(executors_remove)

    def enqueue_task(
        self,
        task_id: int
    ):

        # Case 1: No requirements
        task_backend = self.task_control[task_id].instance.backend
        task_memory = self.task_control[task_id].instance.runtime_memory
        if task_backend is None and task_memory is None:
            self.default_tasks_to_schedule.put(task_id)
            return

        # Case 2: Requirements already in constrained_tasks_to_schedule
        constraint = ExecutionConstraint(
            backend=task_backend,
            runtime_memory=task_memory
        )
        try:
            self.constrained_tasks_to_schedule[constraint].put(task_id)
            return
        except KeyError:
            pass

        # Case 3: Requirements not in constrained_tasks_to_schedule
        self.constrained_tasks_to_schedule[constraint] = queue.Queue()
        self.constrained_tasks_to_schedule[constraint].put(task_id)

    def update_tasks(
        self,
        finished_stage_id: int = None
    ):

        stages = self.physical_plan.next_stage()
        for stage_id, next_stage in stages.items():
            next_stage = self.stages[stage_id]
            for task_info in next_stage.task_infos:
                task_id = task_info.task_id
                self.task_control[task_id] = TaskControl(
                    task_id=task_id,
                    status=TaskState.UNSENT,
                    executor_id=[],
                    worker_id=-1,
                    instance=task_info
                )
                if isinstance(
                    task_info.output_info,
                    CollectInfo
                ):
                    self.data_collects.put(task_info.output_info)
                self.unsent_tasks.add(task_id)
                self.enqueue_task(task_id)
            self.physical_plan.unsent_stages.remove(stage_id)

        # Decide if we need to change executors
        if len(stages) > 0:
            self.scale(
                sent_stage_ids=list(stages.keys()),
                finished_stage_id=finished_stage_id
            )
            print(
                "Launching stages ",
                [stage.stage_id for stage in stages.values()]
            )

        # Here we may decide to launch speculative tasks?

    def finalization_task(
        self,
        num_tasks: int
    ) -> List[services_pb2.TaskInformation]:
        tasks_to_send = [
            task_python_to_grpc(-1)
            for _ in range(num_tasks)
        ]
        return tasks_to_send

    def retry_task(
        self,
        num_tasks: int
    ) -> List[services_pb2.TaskInformation]:
        tasks_to_send = [
            task_python_to_grpc(-2)
            for _ in range(num_tasks)
        ]
        return tasks_to_send

    def SendTasks(
        self,
        request: services_pb2.NumberOfTasks,
        ctx
    ):

        executor_meta = self.executor_meta.get(request.executor_id)
        if (
            executor_meta.state == ExecutorState.WAITING_FOR_FINALIZATION or
            executor_meta.state == ExecutorState.FINISHED
        ):
            print(
                f"Executor {request.executor_id} is not active.",
                "Sending finish signal."
            )
            tasks_to_send = self.finalization_task(request.num_tasks)
            response = services_pb2.ListTaskInformation(
                list_task_info=tasks_to_send
            )
            return response

        tasks_to_send = []
        sending_stage_ids = set()
        for _ in range(request.num_tasks):
            next_task = self.next_task(
                request.num_tasks,
                request.executor_id
            )
            if next_task == -1:
                print(
                    "No tasks for executor",
                    request.executor_id,
                    "sending finish task"
                )
                tasks_to_send.extend(
                    self.finalization_task(request.num_tasks)
                )
                break
            elif next_task == -2:
                tasks_to_send.extend(
                    self.retry_task(request.num_tasks)
                )
                break
            elif next_task is None:
                continue
            else:
                self.pending_tasks.append(next_task.task_id)
                self.task_control[next_task.task_id].status = \
                    TaskState.SUBMITTED
                self.task_control[next_task.task_id].executor_id.append(
                    request.executor_id
                )
                task_info = task_python_to_grpc(next_task)
                tasks_to_send.append(task_info)
                sending_stage_ids.add(task_info.stage_id)

        task_ids = [task.task_id for task in tasks_to_send]
        if len(task_ids) > 0 and task_ids[0] > -1:
            print(
                "Send tasks", task_ids,
                "to executor", request.executor_id
            )

        response = services_pb2.ListTaskInformation(
            list_task_info=tasks_to_send
        )
        return response

    def PartitionSavedExternal(
        self,
        request: services_pb2.PartitionKey,
        ctx
    ):

        key = request.key
        executor_creator_id = request.executor_id

        if self.end:
            return services_pb2.EmptyResponse()

        pkey_request = proto_sched_msg_to_python(request)
        response = self.communicate_with_pmanager(pkey_request)
        if response[0] is not None:
            executor_id, num_read, worker_id, list_worker_id, _ = response
            notified_exec = set()

            for worker_id, _, _, exec_req_id in list_worker_id:
                if executor_creator_id != exec_req_id:

                    if (key, exec_req_id) not in notified_exec:
                        # The executor inside knows which workers need
                        # partitions from this key
                        notified_exec.add((key, exec_req_id))
                        backend = self.executor_meta[exec_req_id].backend
                        if is_container_backend(backend):
                            event = EventStagePartitionSavedInfo(
                                key=key,
                                num_read=num_read,
                                worker_id=worker_id,
                                executor_id=executor_id,
                                storage=IOBackend.EXTERNAL.value
                            )
                            self.executor_meta[exec_req_id].stub.put(
                                event
                            )

                        else:
                            event = services_pb2.PartitionSavedInformation(
                                key=key,
                                num_read=num_read,
                                worker_id=worker_id,
                                executor_id=executor_id,
                                storage=IOBackend.EXTERNAL.value
                            )

                            self.executor_meta[exec_req_id].stub.\
                                ReadPartitionFromCloud(event)

        return services_pb2.EmptyResponse()

    def TaskCompleted(
        self,
        request: services_pb2.TaskCompletedInfo,
        ctx
    ):

        if request.task_id == -1:
            print(
                "Executor",
                request.executor_id,
                "has nothing more to run, finalizing"
            )
            executor_meta = self.executor_meta[request.executor_id]
            return services_pb2.EmptyResponse()

        print(
            "Task completed",
            request.task_id,
            "from executor",
            request.executor_id
        )
        try:
            task_id = request.task_id

            self.task_control[task_id].executor_id = [request.executor_id]
            self.task_control[task_id].status = TaskState.FINISHED
            self.task_control[task_id].worker_id = request.worker_id
            if task_id in self.pending_tasks:
                self.pending_tasks.remove(task_id)
            self.physical_plan.running_tasks -= 1

            stage_id = request.stage_id
            finished_stage_id = self.physical_plan.task_completed(stage_id)
            self.update_tasks(
                finished_stage_id=finished_stage_id
            )

            tcompleted_req = proto_sched_msg_to_python(request)
            response = self.communicate_with_pmanager(tcompleted_req)
            save_partition, partition_saved, update, event_id = response

            for message in save_partition:

                executor_id, key, storage, worker_id = message
                executor_meta = self.executor_meta[request.executor_id]
                backend = executor_meta.backend

                if is_container_backend(backend):
                    event_save = EventStageSavePartitionInfo(
                        key=key,
                        storage=storage,
                        worker_id=worker_id
                    )

                    executor_meta.stub.put(
                        event_save
                    )

                else:
                    event_save = services_pb2.SavePartitionInfo(
                        key=key,
                        storage=storage,
                        worker_id_creator=worker_id
                    )
                    executor_meta.stub.SavePartitionInCloud(event_save)

            for message in partition_saved:

                (
                    executor_request_id,
                    key,
                    num_read,
                    worker_id,
                    executor_id
                ) = message

                executor_meta = self.executor_meta[executor_request_id]
                backend = executor_meta.backend

                try:
                    if is_container_backend(backend):
                        event_read = EventStagePartitionSavedInfo(
                            key=key,
                            num_read=num_read,
                            worker_id=worker_id,
                            executor_id=executor_id,
                            storage=IOBackend.EXTERNAL.value
                        )
                        executor_meta.stub.put(
                            event_read
                        )

                    else:
                        event_read = services_pb2.PartitionSavedInformation(
                            key=key,
                            num_read=num_read,
                            worker_id=worker_id,
                            executor_id=executor_id,
                            storage=IOBackend.EXTERNAL.value
                        )

                        executor_meta.stub.ReadPartitionFromCloud(event_read)

                except grpc.RpcError:
                    # If executor already finished, not necessary to
                    # communicate
                    pass

            for message in update:

                (
                    executor_reader,
                    key,
                    num_read,
                    worker_id,
                    executor_creator
                ) = message

                if (
                    (executor_creator != executor_reader) and
                    (executor_creator not in self.finalized_executors)
                ):
                    executor_meta = self.executor_meta[executor_creator]
                    backend = executor_meta.backend
                    if is_container_backend(backend):
                        event_update_read = EventUpdatePartitionStage(
                            key=key,
                            num_read=num_read,
                            worker_id=worker_id,
                            executor_id=executor_creator
                        )
                        executor_meta.stub.put(
                            event_update_read
                        )
                    else:
                        event_act = EventUpdatePartitionStage(
                            key=key,
                            num_read=num_read,
                            worker_id=worker_id,
                            executor_id=executor_creator
                        )
                        event_update_read = event_upd_p_to_grpc(event_act)
                        executor_meta.stub.UpdatePartitionExecutor(
                            event_update_read
                        )

            if self.check_end():
                executors_to_finalize = []
                self.end = True
                self.pmanager_q.put(-1)
                self.rmanager_q.put("EXIT")
                self.rprovisioner_q.put("EXIT")
                for executor in self.executor_meta.values():
                    if executor.state == ExecutorState.FINISHED:
                        continue
                    executors_to_finalize.append(executor.id)
                    for _ in range(executor.workers):
                        self.default_tasks_to_schedule.put(-1)
                print(
                    "End reached! Finalizing executors:",
                    executors_to_finalize
                )
                self.finalize_executors(executors_to_finalize)

        except Exception as e:
            print(e)

        return services_pb2.EmptyResponse()

    def check_end(self):

        res = (
            len(self.unsent_tasks) == 0 and
            self.physical_plan.empty() and
            len(self.pending_tasks) == 0
        )
        print("%d tasks pending, %d unsent tasks" % (
            len(self.pending_tasks),
            len(self.unsent_tasks)
        ))
        if len(self.pending_tasks) < 5:
            print(
                "Pending tasks:", self.pending_tasks
            )
        return res

    def communicate_with_pmanager(
        self,
        request
    ):
        event_id = self.event_counter.next()
        request.event_id = event_id
        self.pmanager_q.put(request)

        retries = 0
        max_retries = 100
        # Prevent infinite loops in case of issues
        response_received = False
        while not response_received and retries < max_retries:
            with self._response_lock:
                # First, check if the response is already in our
                # unprocessed list
                for i, resp in enumerate(self._unprocessed_responses):
                    resp_id = resp[-1]
                    if resp_id == event_id:
                        # Remove the specific response
                        self._unprocessed_responses.remove(resp)
                        response_received = True
                        break

                if response_received:
                    break

                # If not in buffer, try to get from the queue
                # Using a small timeout to avoid blocking indefinitely and
                # allow for checking the buffer
                try:
                    resp = self.scheduler_pm_q.get(timeout=0.1)
                    resp_id = resp[-1]
                    if resp_id == event_id:
                        response_received = True
                    else:
                        self._unprocessed_responses.append(resp)
                except multiprocessing.queues.Empty:
                    # No message available yet, try again
                    pass

            if not response_received:
                # Small delay to prevent busy-waiting
                time.sleep(0.05)
                retries += 1

        return resp

    def ServerRegisterRequestedPartition(
        self,
        request: services_pb2.RequestPartition,
        ctx
    ):

        rpartition_req = proto_sched_msg_to_python(request)

        response = self.communicate_with_pmanager(rpartition_req)

        save_partition, partition_saved, event_id = response
        for message in save_partition:

            executor_id_creator, key, storage, worker_id_creator = message
            _executor_meta = self.executor_meta[executor_id_creator]
            backend = _executor_meta.backend
            if is_container_backend(backend):
                event_save = EventStageSavePartitionInfo(
                    key=key,
                    storage=storage.value,
                    worker_id=worker_id_creator
                )
                _executor_meta.stub.put(
                    event_save
                )

            else:
                event_save = services_pb2.SavePartitionInfo(
                    key=key,
                    storage=storage.value,
                    worker_id_creator=worker_id_creator
                )
                try:
                    _executor_meta.stub.SavePartitionInCloud(event_save)
                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.UNAVAILABLE:
                        pass

                print(
                    "Notified executor",
                    executor_id_creator,
                    "about requested partition",
                    key
                )

        for message in partition_saved:

            (
                executor_id,
                key,
                num_read,
                worker_id_creator,
                executor_id_creator
            ) = message

            print(
                "Notifying already transmitted partition",
                key,
                "from executor",
                executor_id_creator,
                "to",
                executor_id
            )

            _executor_meta = self.executor_meta[executor_id]

            backend = _executor_meta.backend
            if is_container_backend(backend):
                event_read = EventStagePartitionSavedInfo(
                    key=key,
                    num_read=num_read,
                    worker_id=worker_id_creator,
                    executor_id=executor_id_creator,
                    storage=IOBackend.EXTERNAL.value
                )
                _executor_meta.stub.put(event_read)

            else:
                event_read = services_pb2.PartitionSavedInformation(
                    key=key,
                    num_read=num_read,
                    worker_id=worker_id_creator,
                    executor_id=executor_id_creator,
                    storage=IOBackend.EXTERNAL.value
                )
                _executor_meta.stub.\
                    ReadPartitionFromCloud(event_read)

        return services_pb2.EmptyResponse()

    def ConsultEvents(
        self,
        request: services_pb2.ExecutorInformation,
        ctx
    ):

        executor_id = request.executor_id

        print(
            f"Executor {executor_id} asking for events."
        )

        while True:
            event = self.executor_meta[executor_id].stub.get()

            if isinstance(event, EventStageFinishExecutor):
                send_event = services_pb2.EventWriteOrRead(
                    finish_executor=event.mess
                )
                yield send_event

            elif isinstance(event, EventStageSavePartitionInfo):
                event_save = services_pb2.SavePartitionInfo(
                    key=event.key,
                    storage=event.storage,
                    worker_id_creator=event.worker_id
                    )
                send_event = services_pb2.EventWriteOrRead(
                    save_partition_info=event_save
                )

            elif isinstance(event, EventStagePartitionSavedInfo):
                event_read = services_pb2.PartitionSavedInformation(
                    key=event.key,
                    num_read=event.num_read,
                    worker_id=event.worker_id,
                    executor_id=event.executor_id,
                    storage=event.storage
                )
                send_event = services_pb2.EventWriteOrRead(
                    partition_saved_info=event_read
                )

            elif isinstance(event, EventUpdatePartitionStage):
                event_upd = event_upd_p_to_grpc(event)
                send_event = services_pb2.EventWriteOrRead(
                    update_partition_info=event_upd
                )

            yield send_event

    def Ping(
        self,
        request,
        ctx
    ):
        print(f"Received ping: {request.message}")
        return services_pb2.PingResponse(
            reply=f"Pong: {request.message}",
            timestamp=int(time.time())
        )

    def UpdateFanout(
        self,
        request: services_pb2.Fanout,
        ctx
    ):
        stage_id = request.stage_id
        fanout = request.fanout
        self.physical_plan.dag.stages[stage_id].fanout = fanout
        for parent_stage_id in (
            self.physical_plan.dag.get_immediate_predecessors(stage_id)
        ):
            self.physical_plan.gen_exchange_writes(parent_stage_id)
        return services_pb2.EmptyResponse()


class PartitionManager():

    def __init__(
        self,
        pmanager_q: Queue,
        scheduler_pm_q: Queue
    ):
        self.logger = get_logger("PartitionManager")
        self.pmanager_q = pmanager_q
        self.scheduler_pm_q = scheduler_pm_q
        self.memory_manager: Dict[str, PartitionInfo] = {}
        self.requested_partitions: Dict[str, List[int]] = {}

    def run(
        self
    ):
        while True:
            event = self.pmanager_q.get(block=True)
            if isinstance(event, PartitionKey):
                res = self.partition_saved_external(event)
            elif isinstance(event, TaskCompletedInfo):
                res = self.task_completed(event)
            elif isinstance(event, RequestPartition):
                res = self.register_requested_partition(event)
            elif event == -1:
                self.logger.info(
                    color_string(
                        "Finalizing Partition Manager",
                        [AsciiColors.BOLD, AsciiColors.PEACHY_PASTEL]
                    )
                )
                return 0
            else:
                res = None

            if event != -1:
                res.append(event.event_id)
            self.scheduler_pm_q.put(res)

    def partition_saved_external(
        self,
        partition_key: services_pb2.PartitionKey
    ):

        key = partition_key.key

        if key in self.memory_manager:

            self.memory_manager[key].storage = IOBackend.EXTERNAL
            num_read = self.memory_manager[key].num_read
            worker_id = self.memory_manager[key].worker_id
            executor_id = self.memory_manager[key].executor_id

            if key in self.requested_partitions:
                list_worker_id = self.requested_partitions[key]

                # No more workers have requested this key
                del self.requested_partitions[key]

                return [
                    executor_id,
                    num_read,
                    worker_id,
                    list_worker_id
                ]

        return [None]

    def task_completed(
        self,
        completed_info: services_pb2.TaskCompletedInfo
    ):

        save_partition = []
        partition_saved = []
        update = []

        for event_add_p_grpc in completed_info.list_add_partition_events:

            event = event_add_p_to_python(event_add_p_grpc)

            io_backend = IOBackend(event.storage)
            managed = bool(io_backend == IOBackend.EXTERNAL)
            memory_entry = PartitionInfo(
                io_backend,
                event.num_read,
                event.worker_id,
                event.executor_id,
                managed
            )

            if event.key in self.requested_partitions:
                list_worker_id = self.requested_partitions[event.key]
                workers_to_send = []

                for (
                    w_id,
                    exchange_id,
                    partition_id,
                    executor_request_id
                ) in list_worker_id:
                    if executor_request_id != event.executor_id:
                        workers_to_send.append(
                            (
                                w_id,
                                exchange_id,
                                partition_id,
                                executor_request_id
                            )
                        )

                # Consult before request to write from shmem to s3, that exist
                # at least one worker that will read. If the worker that
                # request the partition is from the same executor as the worker
                # creator, the PartitionManager of the Executor will manage
                # the process.
                storage_backend = memory_entry.storage
                if storage_backend != IOBackend.EXTERNAL and workers_to_send:

                    save_partition.append((
                        completed_info.executor_id,
                        event.key,
                        event.storage,
                        event.worker_id
                    ))

                    # With this we are indicating that the worker who has to
                    # write the partitions to
                    # external memory has already been notified.
                    memory_entry.managed = True

                elif (
                    memory_entry.storage == IOBackend.EXTERNAL and
                    workers_to_send
                ):

                    notified_executors = set()
                    for (
                        w_id,
                        exchange_id,
                        partition_id,
                        executor_request_id
                    ) in workers_to_send:

                        if (
                            event.key,
                            executor_request_id
                        ) not in notified_executors:
                            # The executor inside knows which workers need
                            # partitions from this key
                            notified_executors.add(
                                (event.key, executor_request_id)
                            )

                            partition_saved.append((
                                executor_request_id,
                                event.key,
                                memory_entry.num_read,
                                memory_entry.worker_id,
                                memory_entry.executor_id
                            ))

                            memory_entry.storage = IOBackend.EXTERNAL

                    # No more workers have requested this key
                    del self.requested_partitions[event.key]

            self.memory_manager[event.key] = memory_entry

        for event_upd_p_grpc in completed_info.list_upd_partition_events:

            event = event_upd_p_to_python(event_upd_p_grpc)

            executor_creator = self.memory_manager[event.key].executor_id
            executor_reader = event.executor_id

            if executor_creator != executor_reader:

                update.append((
                    executor_reader,
                    event.key,
                    event.num_read,
                    event.worker_id,
                    executor_creator
                ))

            self.memory_manager[event.key].num_read -= event.num_read
            if self.memory_manager[event.key].num_read <= 0:
                del self.memory_manager[event.key]

        return [
            save_partition,
            partition_saved,
            update
        ]

    def register_requested_partition(
        self,
        request: services_pb2.RequestPartition
    ):

        save_partition = []
        partition_saved = []

        key = request.key
        worker_id = request.worker_id
        exchange_id = request.exchange_id
        partition_id = request.partition_id
        executor_id = request.executor_id

        if key in self.requested_partitions:
            self.requested_partitions[key].append(
                (worker_id, exchange_id, partition_id, executor_id)
            )
        else:
            self.requested_partitions[key] = [
                (worker_id, exchange_id, partition_id, executor_id)
            ]

        if key in self.memory_manager:
            num_read = self.memory_manager[key].num_read
            worker_id_creator = self.memory_manager[key].worker_id
            executor_id_creator = self.memory_manager[key].executor_id
            storage_creator = self.memory_manager[key].storage
            write_to_external_managed = self.memory_manager[key].managed
            self.memory_manager[key].managed = True
            self.memory_manager[key].storage = IOBackend.EXTERNAL
            list_worker_id = self.requested_partitions[key]
        else:
            list_worker_id = None

        if list_worker_id is not None:
            dest_workers = []

            for (
                _worker_id,
                exchange_id,
                partition_id,
                executor_id
            ) in list_worker_id:
                if executor_id != executor_id_creator:
                    dest_workers.append(
                        (_worker_id, exchange_id, partition_id, executor_id)
                    )

            if not write_to_external_managed:

                save_partition.append((
                    executor_id_creator,
                    key,
                    storage_creator,
                    worker_id_creator
                ))

            for (
                _worker_id,
                exchange_id,
                partition_id,
                executor_id
            ) in dest_workers:

                partition_saved.append((
                    executor_id,
                    key,
                    num_read,
                    worker_id_creator,
                    executor_id_creator
                ))

            if key in self.requested_partitions:
                # No more workers have requested this key
                del self.requested_partitions[key]

        return [
            save_partition,
            partition_saved
        ]


class ResourceManager():

    def __init__(
        self,
        execution_config: ExecutorConfig,
        rmanager_q: Queue,
        registered_executors: Queue,
        executor_logs: List
    ):
        self.executor_config = execution_config
        self.rmanager_q = rmanager_q
        self.registered_executors = registered_executors
        self.manager: ManagerType = None
        self.logger = get_logger(__file__.split("/")[-1])

        self.process_executors: List[ExecutorInstance] = []
        self.cf_executors: Dict[str, Dict[int, List[ExecutorInstance]]] = {}
        self.on_premise_executors: \
            List[Tuple[FunctionExecutor, ExecutorInstance]] = []
        self.k8s_executors: Dict[str, Dict[int, List[ExecutorInstance]]] = {}

        self.processes: List[ResourceProxy] = []
        self.cf_futures: List[ResourceProxy] = []
        self.on_premise_futures: \
            List[Tuple[FunctionExecutor, ResponseFuture]] = []
        self.k8s_futures: List[ResourceProxy] = []

        self.valid_processes: List[Process] = []
        self.valid_cf_futures: FuturesList = FuturesList()
        self.valid_k8s_futures: List[ResponseFuture] = []

        self.active_executors: Dict[int, ExecutorMeta] = {}

        self.cloud_function_invoker: FunctionExecutor = None
        # We instantiate one invoker per on premise device
        self.on_premise_invokers: \
            List[Tuple[FunctionExecutor, ExecutorInfo]] = []
        self.k8s_invoker: FunctionExecutor = None

        self.storage = Storage()

        self.executor_logs = executor_logs

    def run(self, job_end_signal: EventType):

        with Manager() as self.manager:

            self.launch_executors()
            while True:
                event = self.rmanager_q.get()
                if event == "EXIT":
                    break
                elif isinstance(event, ScaleEvent):
                    self.launch_executors(
                        backend=event.type,
                        runtime_memory=event.runtime_memory,
                        num_workers=event.num
                    )

            self.update_valid_executors()
            self.wait_executors()
            self.gather_task_logs()

            job_end_signal.set()

        self.logger.info(
            color_string(
                "Finalizing Resource Manager",
                [AsciiColors.BOLD, AsciiColors.PEACHY_PASTEL]
            )
        )
        return 0

    def update_valid_executors(
        self
    ):
        while not self.registered_executors.empty():
            backend, executor_id = self.registered_executors.get()
            if backend == BackendType.CLOUD_FUNCTION:
                for future in self.cf_futures:
                    if future.id == executor_id:
                        self.valid_cf_futures.extend(
                            [future.future]
                        )
            elif backend == BackendType.K8S:
                for future in self.k8s_futures:
                    if future.id == executor_id:
                        self.valid_k8s_futures.append(
                            future.future
                        )
            elif backend == BackendType.MULTIPROCESSING:
                for process in self.processes:
                    if process.id == executor_id:
                        self.valid_processes.append(
                            process.future
                        )

    def prepare_invokers(self):

        executor_infos = list(self.executor_config.executor_infos.values())
        executor_infos = [
            ei for ei in executor_infos
            if ei is not None
        ]

        for executor_info in executor_infos:

            execution_backend = executor_info.execution_backend
            self.prepare_storage(executor_info)

            if execution_backend == BackendType.CLOUD_FUNCTION:
                runtime = executor_info.runtime
                self.cloud_function_invoker = (
                    FunctionExecutor(runtime=runtime)
                )
                self.cf_executors = []

            if execution_backend == BackendType.K8S:
                _lithops_config = executor_info.lithops_config
                runtime = executor_info.runtime
                invoker = FunctionExecutor(
                    runtime=runtime,
                    config=_lithops_config,
                    backend="k8s"
                )
                patch_k8s_invoker(invoker)
                self.k8s_invoker = invoker
                self.k8s_executors = []

            if execution_backend == BackendType.CLOUD_VM:
                # TODO: For now, only cloud_vm only
                # supports initial executor setup
                instances = (
                    self.executor_config.initial_instances[
                        execution_backend
                    ]
                )
                for instance in instances:
                    invoker = FunctionExecutor(
                        runtime=executor_info.runtime,
                        config=self.executor_config.lithops_config,
                        log_level="DEBUG"
                    )
                    self.on_premise_invokers.append(
                        (invoker, instance)
                    )

    def launch_executors(
        self,
        backend: BackendType = None,
        runtime_memory: int = None,
        num_workers: int = None,
        workers_per_executor: int = None
    ):

        print("Launching executors...")

        if num_workers is None:
            self.prepare_invokers()
            print("Prepared invokers.")
            instances = self.executor_config.initial_instances
        else:
            if backend is None:
                backend = self.executor_config.default_autoscaling_backend
            if workers_per_executor is None:
                executor_info = self.executor_config.executor_infos.get(
                    backend
                )
                workers_per_executor = executor_info.default_num_workers
            required = math.ceil(num_workers / workers_per_executor)
            instances = self.executor_config.prepare_executors(
                backend,
                runtime_memory,
                num_workers=workers_per_executor,
                num=required
            )
            instances = {backend: instances}

        for backend, _instances in instances.items():

            if len(_instances) == 0:
                continue

            if backend == BackendType.MULTIPROCESSING:
                if self.manager is None:
                    self.manager = Manager()
                for instance in _instances:
                    instance.exec_info = self.manager.list()
                    self.process_executors.append(instance)
                    process = Process(
                        target=run_executor,
                        args=(instance, )
                    )
                    process.start()
                    self.processes.append(
                        ResourceProxy(
                            instance.id,
                            process
                        )
                    )

            elif backend == BackendType.CLOUD_FUNCTION:
                invoker = self.cloud_function_invoker
                futures = invoker.map(
                    run_executor,
                    _instances,
                    runtime_memory=_instances[0].runtime_memory
                )
                for future, instance in zip(futures, _instances):
                    self.cf_futures.append(
                        ResourceProxy(
                            instance.id,
                            future
                        )
                    )

            elif backend == BackendType.K8S:
                invoker = self.k8s_invoker
                futures = invoker.map(
                    run_executor,
                    _instances,
                    runtime_memory=_instances[0].runtime_memory
                )
                for future, instance in zip(futures, _instances):
                    self.k8s_futures.append(
                        ResourceProxy(
                            instance.id,
                            future
                        )
                    )
            elif backend == BackendType.CLOUD_VM:

                for invoker, executor_instance in self.on_premise_invokers:
                    futures = invoker.map(
                        run_executor,
                        executor_instance
                    )
                    self.on_premise_futures.append((invoker, futures))

    def wait_executors_cf(self):

        # Wait for executors in cloud functions
        if self.cloud_function_invoker is None:
            return
        invoker = self.cloud_function_invoker
        wait(
            self.valid_cf_futures,
            internal_storage=invoker.internal_storage,
            show_progressbar=False
        )

    def wait_executors_k8s(self):

        # Wait for executors in kubernetes
        if self.k8s_invoker is None:
            return
        invoker = self.k8s_invoker
        invoker.wait(
            self.valid_k8s_futures,
            show_progressbar=False
        )

    def wait_executors_cloud_vm(self):

        # Wait for on-premise executors
        for invoker, future in self.on_premise_futures:
            invoker.wait(future)

    def wait_executors_processes(self):

        # Wait for executors in processes
        for process in self.valid_processes:
            process.join()

    def wait_executors(self):

        self.logger.info(
            color_string(
                "Resource Manager waiting for final executors.",
                [AsciiColors.BOLD, AsciiColors.PEACHY_PASTEL]
            )
        )

        self.wait_executors_cf()
        self.wait_executors_processes()
        self.wait_executors_cloud_vm()
        self.wait_executors_k8s()

    def prepare_storage(
        self,
        executor_info: ExecutorInfo
    ):

        config = executor_info.lithops_config
        backend = config.get("lithops").get("storage")
        if backend in config:
            bucket = config[backend].get('storage_bucket')
        else:
            executor_info.lithops_config[backend] = {}
            bucket = None
        executor_info.lithops_config[backend]["storage_bucket"] = \
            bucket or self.storage.bucket

    def gather_task_logs_processes(self):
        task_logs = []
        for executor_info in self.process_executors:
            task_logs.extend(executor_info.exec_info)
        return task_logs

    def gather_task_logs_cloud_functions(self):

        if self.cloud_function_invoker is None:
            return []
        task_logs = []
        invoker = self.cloud_function_invoker
        _task_logs = get_result(
            self.valid_cf_futures,
            internal_storage=invoker.internal_storage,
            show_progressbar=False
        )
        for result in _task_logs:
            task_logs.extend(result)

        return task_logs

    def gather_task_logs_k8s(self):

        if self.k8s_invoker is None:
            return []
        task_logs = []
        invoker = self.k8s_invoker
        _task_logs = invoker.get_result(
            self.valid_k8s_futures,
            show_progressbar=False
        )
        task_logs.extend(_task_logs)

        return task_logs

    def gather_task_logs_containers(self):

        task_logs = []
        for invoker, future in self.on_premise_futures:
            res = invoker.get_result(future)
            if isinstance(res, list):
                task_logs.extend(res)
            else:
                task_logs.extend([res])
        return task_logs

    def gather_task_logs(self):

        executor_logs_proc = self.gather_task_logs_processes()
        executor_logs_cf = self.gather_task_logs_cloud_functions()
        executor_logs_k8s = self.gather_task_logs_k8s()
        executor_logs_cont = self.gather_task_logs_containers()
        logs = (
            executor_logs_proc +
            executor_logs_cf +
            executor_logs_k8s +
            executor_logs_cont
        )
        self.executor_logs.extend(logs)


class Driver():

    def get_tasks(self):
        tasks = {}
        for stage in self.stages.values():
            tasks[stage.stage_id] = stage.task
        return tasks

    def __init__(
        self,
        execution_config: ExecutorConfig,
        physical_plan: PhysicalPlan,
        driver_ip=DEFAULT_DRIVER_LISTEN_IP,
        driver_port=DEFAULT_DRIVER_PORT
    ):

        self.logger = get_logger("StageServer")
        self.logger.info(f"Starting stage server on {driver_ip}:{driver_port}")

        self.execution_config = execution_config
        self.executors_info = execution_config.executor_infos
        self.physical_plan = physical_plan
        self.driver_ip = driver_ip
        self.driver_port = driver_port

        self.pmanager_q: Queue = None
        self.scheduler_pm_q: Queue = None
        self.rmanager_q: Queue = None
        self.scheduler_rm_q: Queue = None
        self.rprovisioner_q: Queue = None
        self.registered_executors: Queue = None

        self.job_end_signal: EventType = None
        self.finalization_signal: EventType = None
        self.manager = Manager()
        self.executor_logs: List = None

    def run(
        self
    ) -> List[CollectInfo]:
        """
        Starts driver components.
        """

        self.job_end_signal = Event()
        self.finalization_signal = Event()
        self.manager = Manager()
        self.executor_logs = self.manager.list()

        self.pmanager_q = Queue()
        self.scheduler_pm_q = Queue()
        self.rmanager_q = Queue()
        self.scheduler_rm_q = Queue()
        self.rprovisioner_q = Queue()
        self.registered_executors = Queue()
        self.data_collects = Queue()

        scheduler = Scheduler(
            self.physical_plan,
            self.rmanager_q,
            self.pmanager_q,
            self.rprovisioner_q,
            self.scheduler_rm_q,
            self.scheduler_pm_q,
            self.registered_executors,
            self.data_collects,
            self.execution_config.get_initial_fanout()
        )

        self.logger.info(
            color_string(
                "Running Scheduler",
                [AsciiColors.BOLD, AsciiColors.PEACHY_PASTEL]
            )
        )
        scheduler_proc = Process(
            target=self.run_scheduler,
            args=(
                scheduler,
                self.job_end_signal,
                self.finalization_signal
            )
        )
        scheduler_proc.start()

        self.logger.info(
            color_string(
                "Running Partition Manager",
                [AsciiColors.BOLD, AsciiColors.PEACHY_PASTEL]
            )
        )
        partition_manager = PartitionManager(
            self.pmanager_q,
            self.scheduler_pm_q
        )
        partition_manager_proc = Process(
            target=partition_manager.run
        )
        partition_manager_proc.start()

        self.logger.info(
            color_string(
                "Running Resource Manager",
                [AsciiColors.BOLD, AsciiColors.PEACHY_PASTEL]
            )
        )
        resource_manager = ResourceManager(
            self.execution_config,
            self.rmanager_q,
            self.registered_executors,
            self.executor_logs
        )
        resource_manager_proc = Process(
            target=resource_manager.run,
            args=[self.job_end_signal]

        )
        resource_manager_proc.start()

        self.finalization_signal.wait()

        if scheduler_proc.is_alive():  # Check if still running
            scheduler_proc.terminate()
            scheduler_proc.join()  # Ensure the process is cleaned up

        data_collects = []
        while not self.data_collects.empty():
            data_collects.append(self.data_collects.get())
        return data_collects

    def run_scheduler(
        self,
        scheduler: Scheduler,
        job_end_signal: EventType,
        finalization_signal: EventType,
    ):
        try:
            server_pool = futures.ThreadPoolExecutor(
                max_workers=MAX_SERVER_THREADS
            )
            server = grpc.server(
                server_pool
            )
            services_pb2_grpc.add_SchedulerServicer_to_server(
                scheduler,
                server
            )
            driver_address = self.driver_ip + ":" + str(self.driver_port)
            server.add_insecure_port(driver_address)
            self.logger.info(f"Driver listening on {driver_address}")
            server.start()

            job_end_signal.wait()

            server.stop(0)

        except Exception as e:
            print(
                f"Error in Scheduler server: {e}"
            )
            raise e

        self.logger.info(
            color_string(
                "Finalized Scheduler",
                [AsciiColors.BOLD, AsciiColors.PEACHY_PASTEL]
            )
        )
        finalization_signal.set()

    def complete_logs(
        self,
        start_time: int,
        end_time: int
    ):

        plan_descriptor = self.physical_plan.to_dict()
        backend_descriptor = self.execution_config.to_dict()
        logs = list(self.executor_logs)
        executor_infos = [
            log["executor_info"]
            for log in logs
        ]
        executor_logs = [
            log["task_exec_info"]
            for log in logs
        ]
        executor_logs_flat = []
        for log in executor_logs:
            if isinstance(log, list):
                executor_logs_flat.extend(log)
            else:
                executor_logs_flat.append(log)
        log = {
            "start_time": start_time,
            "end_time": end_time,
            "plan": plan_descriptor,
            "backend": backend_descriptor,
            "executor_infos": executor_infos,
            "executor_logs": executor_logs_flat
        }
        return log


def run_driver(driver: Driver):

    start_time = time.time()
    data_collects = driver.run()
    end_time = time.time()
    logs = driver.complete_logs(
        start_time,
        end_time
    )
    return logs, data_collects


class MonotonicCounter:
    def __init__(self, start=0):
        self._value = start
        self._lock = threading.Lock()

    def next(self):
        with self._lock:
            val = self._value
            self._value += 1
            return val
