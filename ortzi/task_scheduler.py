
import queue
import threading
import time
from typing import Dict, List

from ortzi.config import (
    MAX_RETRIES_LOW,
    RETRY_WAIT_TIME
)
from ortzi.physical.utils import ThreadSafeCounter
from ortzi.proto.task_info import (
    event_add_p_to_grpc,
    task_grpc_to_python,
    event_upd_p_to_grpc
)
from ortzi.proto.services_pb2_grpc import SchedulerStub
import ortzi.proto.services_pb2 as pb2
from ortzi.io_utils.queues import Queue
from ortzi.io_utils.info import (
    IOBackend,
    PartitionInfo,
    IOInfo
)
from ortzi.event import (
    EventAddPartition,
    EventAddPartitionStage,
    EventEnd,
    EventEndPartitionManager,
    EventTask,
    EventTaskEnd,
    EventUpdatePartitionStage,
    EventVoidTask
)
from ortzi.logging.ui_log import get_logger
from ortzi.task import TaskInfo


class TaskScheduler():
    """
    Handles task scheduling and executions of the tasks inside the workers.

    Attributes:
        worker_queues (Dict[int, Queue]): A dictionary mapping worker IDs to
        their respective queues.
        end_queue (Queue): A queue for signaling task completion.
        partition_manager_q (Queue): A queue for communication with the
        partition manager.
        num_workers (int): The number of workers in the system.
        executor_id: The ID of the executor.
    """

    def __init__(
        self,
        worker_task_qs: Dict[int, Queue],
        task_end_q: Queue,
        partition_manager_q: Queue,
        pmanager_response_q: Queue,
        executor_finish_q: Queue,
        executor_id: int,
        task_exec_info: List[Dict]
    ):
        self.worker_task_qs = worker_task_qs
        self.task_end_q = task_end_q
        self.partition_manager_q = partition_manager_q
        self.pmanager_response_q = pmanager_response_q
        self.num_workers = len(worker_task_qs)
        self.executor_id = executor_id
        self.executor_finish_q = executor_finish_q
        self.task_exec_info = task_exec_info

        self.logger = get_logger("TaskScheduler")

    def updatePartitionManager(
        self,
        task_info: TaskInfo,
        worker_id: int,
        storage_output_backend: str,
        storage_input_keys: List[List[str]],
        storage_output_key: str,
        serialized_data: Dict[int, bytes]
    ):
        """
        Updates the PartitionManager with metadata related to task execution.

        Args:
            task_info (TaskInfo): Information about the task.
            worker_id (int): The ID of the worker executing the task.
            storage_output_backend (str): The storage backend for output of
            the task.
            storage_input_keys: The keys for input partitions used by the task.
            storage_output_key (str): The key for the output partition used by
            the task.
            serialized_data (Dict[int, bytes]): Serialized data for the output
            partition (used by queue memory).

        Returns:
            Tuple[
              List[EventAddPartition],
              List[EventAddPartitionStage],
              List[EventUpdatePartition]
            ]: Lists of events for adding new partitions in the Partition
            Manager of the executor, adding partition new partitions in the
            Partition Manager of the Stage, and updating existing partitions in
            the Partition Manager of the executor and stage.
        """

        add_partition_events = []
        add_partition_events_stage = []
        update_partition_events = []

        # Prepare metadata for the new partition that will be saved
        # in the PartitionManager
        if isinstance(task_info.output_info, PartitionInfo):
            num_read = task_info.output_info.num_read
            if storage_output_backend != IOBackend.QUEUE.value:
                # If the backend are queues, this is performed at put
                # partitions directly.
                add_partition_events.append(
                    EventAddPartition(
                        storage_output_key,
                        num_read,
                        worker_id,
                        self.executor_id,
                        storage_output_backend,
                        serialized_data
                    )
                )
            add_partition_events_stage.append(
                EventAddPartitionStage(
                    storage_output_key,
                    num_read,
                    worker_id,
                    self.executor_id,
                    storage_output_backend
                )
            )

        # Prepare metadata to update the information on read partitions
        if isinstance(task_info.input_info, (PartitionInfo, IOInfo)):
            input_infos = [task_info.input_info]
        else:
            input_infos = task_info.input_info

        for input_i, input_info in enumerate(input_infos):
            if isinstance(input_info, PartitionInfo):
                num_read = input_info.num_read
                for e_k_p in storage_input_keys[input_i]:
                    key = e_k_p[1]
                    update_partition_events.append(
                        EventUpdatePartitionStage(
                            key,
                            num_read,
                            worker_id,
                            self.executor_id
                        )
                    )

        return (
            add_partition_events,
            add_partition_events_stage,
            update_partition_events
        )

    def get_tasks(
        self,
        num_tasks: int,
        stub: SchedulerStub
    ) -> List[TaskInfo]:

        request = pb2.NumberOfTasks(
            num_tasks=num_tasks,
            executor_id=self.executor_id
        )
        for attempt in range(MAX_RETRIES_LOW):
            try:
                while True:
                    tasks_info_grpc = stub.SendTasks(
                        request,
                        timeout=20
                    )
                    if tasks_info_grpc.list_task_info[0].stage_id != -2:
                        return tasks_info_grpc
                    print(
                        f"(Executor {self.executor_id})",
                        "No tasks available, retrying..."
                    )
                    time.sleep(1)
            except Exception as e:
                print(
                    f"Attempt {attempt + 1} failed to get tasks: {e}"
                )
                if attempt == MAX_RETRIES_LOW - 1:
                    raise e
                else:
                    time.sleep(RETRY_WAIT_TIME)
                    continue

    def run(
        self,
        stub: SchedulerStub
    ):
        """
        Method that controls the schedulling and execution of the tasks
        inside the Executors. It also exchanges information to the Stage
        server (ask for tasks, communicate that tasks are completed +
        partitions (updated and added))

        Args:
            stub: Used to communicate with the Stage Server through grpc.
        """

        # Variable that controls how many tasks are not finished yet
        current_queue = 0
        finalization_received = threading.Event()  # Signal for termination
        pending_tasks = ThreadSafeCounter()

        tasks_info_grpc = self.get_tasks(
            self.num_workers,
            stub
        )
        task_infos = []
        for task_info_grpc in tasks_info_grpc.list_task_info:
            # The driver signaled the executor to end
            # (once its tasks are finished)
            if task_info_grpc.stage_id == -1:
                print(
                    f"Executor {self.executor_id}",
                    "received finish signal from driver."
                )
                finalization_received.set()
            else:
                task_info_python = task_grpc_to_python(task_info_grpc)
                task_infos.append(task_info_python)

        # Distribute initial tasks
        for task_info in task_infos:
            task_event = EventTask(task_info)
            self.worker_task_qs[current_queue].put(task_event)
            current_queue = (current_queue + 1) % self.num_workers
            pending_tasks.increment()

        if len(task_infos) < len(self.worker_task_qs):
            for _ in range(len(self.worker_task_qs) - len(task_infos)):
                # Fill the remaining queues with empty tasks
                # to avoid deadlocks
                task_event = EventVoidTask()
                self.worker_task_qs[current_queue].put(task_event)
                current_queue = (current_queue + 1) % self.num_workers

        if finalization_received.is_set():
            for worker_queue in self.worker_task_qs.values():
                worker_queue.put(EventEnd())

        worker_queue = queue.Queue()

        def scheduler():
            nonlocal stub

            try:
                while not finalization_received.is_set():

                    worker_id = worker_queue.get()
                    tasks_info_grpc = self.get_tasks(
                        1,
                        stub
                    )

                    # Handle empty response safely
                    if not tasks_info_grpc.list_task_info:
                        continue

                    task_info_grpc = tasks_info_grpc.list_task_info[0]

                    if task_info_grpc.stage_id == -1:
                        print(
                            f"Executor {self.executor_id}",
                            "received finish signal from driver."
                        )
                        finalization_received.set()
                        return

                    pending_tasks.increment()
                    task_info = task_grpc_to_python(task_info_grpc)
                    task_event = EventTask(task_info)
                    self.worker_task_qs[worker_id].put(task_event)
            except Exception as e:
                print(f"Error in scheduler: {e}")
                raise e

        def monitor():
            nonlocal stub
            while True:
                try:
                    # Avoid blocking indefinitely
                    finished_task_event: EventTaskEnd = \
                        self.task_end_q.get(timeout=0.2)
                    worker_queue.put(finished_task_event.worker_id)
                    if finished_task_event.task_info is not None:
                        pending_tasks.decrement()
                        self.manage_task_completed(
                            finished_task_event,
                            stub
                        )
                except queue.Empty:
                    if (
                        finalization_received.is_set() and
                        pending_tasks._value < 1
                    ):
                        # Exit when no tasks remain & finalization is signaled
                        return 0
                except Exception as e:
                    print(f"Error in monitor: {e}")
                    raise e

        scheduler_thread = threading.Thread(target=scheduler)
        scheduler_thread.start()
        monitor_thread = threading.Thread(target=monitor)
        monitor_thread.start()
        try:
            monitor_thread.join()
        except Exception as e:
            print(f"Error in monitor join: {e}")
            raise e

        # Wait for the scheduler thread to finish
        final_task_completed = pb2.TaskCompletedInfo(
            task_id=-1,
            stage_id=-1,
            executor_id=self.executor_id,
            worker_id=-1,
            list_add_partition_events=[],
            list_upd_partition_events=[]
        )
        try:
            stub.TaskCompleted(final_task_completed)
        except Exception as e:
            print(f"Error sending final task completion: {e}")
            raise e

        # Wait until scheduler notifies Executor Server to close
        # workers, shmem, partition manager and the executor itself.
        self.executor_finish_q.get()
        self.partition_manager_q.put(
            EventEndPartitionManager()
        )
        self.pmanager_response_q.get()

        for w_q in self.worker_task_qs.values():
            w_q.put(EventEnd())

        return self.task_exec_info

    def manage_task_completed(
        self,
        finished_task_event: EventTaskEnd,
        stub: SchedulerStub
    ):

        try:
            (
                add_partition_events,
                add_partition_events_stage,
                update_partition_events
            ) = self.updatePartitionManager(
                    finished_task_event.task_info,
                    finished_task_event.worker_id,
                    finished_task_event.storage_output_backend,
                    finished_task_event.storage_input_keys,
                    finished_task_event.storage_output_key,
                    finished_task_event.serialized_data
                )
            self.task_exec_info.append(finished_task_event.exec_info)

            if add_partition_events:
                for partition_event in add_partition_events:
                    self.partition_manager_q.put(partition_event)

            if update_partition_events:
                for partition_event in update_partition_events:
                    self.partition_manager_q.put(partition_event)

            events_add_part_grpc = []
            for event in add_partition_events_stage:
                events_add_part_grpc.append(event_add_p_to_grpc(event))

            events_update_part_grpc = []
            for event in update_partition_events:
                events_update_part_grpc.append(event_upd_p_to_grpc(event))

            task_completed_info = pb2.TaskCompletedInfo(
                task_id=finished_task_event.task_info.task_id,
                stage_id=finished_task_event.task_info.stage_id,
                executor_id=finished_task_event.executor_id,
                worker_id=finished_task_event.worker_id,
                list_add_partition_events=events_add_part_grpc,
                list_upd_partition_events=events_update_part_grpc
            )

            # The Stage has to control which task has finished
            # and where is situated the new partition saved.
            for attempt in range(MAX_RETRIES_LOW):
                try:
                    stub.TaskCompleted(
                        task_completed_info
                    )
                    break
                except Exception as e:
                    print(
                        f"Attempt {attempt + 1} failed to signal",
                        "task completion: {e}"
                    )
                    if attempt == MAX_RETRIES_LOW - 1:
                        raise e
        except Exception as e:
            print(f"Error signaling task completion: {e}")
            raise e
