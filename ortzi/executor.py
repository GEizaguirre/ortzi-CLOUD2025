from concurrent import futures
import os
import time
import threading
from typing import (
    Dict,
    List
)
from threading import Thread
from multiprocessing import Process
import shutil

import grpc
from lithops.constants import LITHOPS_TEMP_DIR

from ortzi.backends.aws import get_total_vcpus
from ortzi.backends.executor_config import ExecutorInstance
from ortzi.io_utils.queues import (
    Queue,
    CFQueue,
    SMQueue
)
from ortzi.io_utils.info import IOBackend
from ortzi.proto.task_info import partition_msg_to_PartitionInfo
from ortzi.task_scheduler import TaskScheduler
from ortzi.partition_manager import PartitionManager
from ortzi.event import EventAddPartition
from ortzi.event import (
    EventUpdatePartitionStage,
    EventSavePartitionCloud
)
from ortzi.logging.ui_log import get_logger
from ortzi.task import Task
from ortzi.backends.backend import (
    BackendType,
    is_container_backend
)
import ortzi.proto.services_pb2_grpc as services_pb2_grpc
import ortzi.proto.services_pb2 as services_pb2
from ortzi.utils import (
    AsciiColors,
    color_string
)
from ortzi.worker import (
    Worker,
    run_worker
)
from ortzi.config import (
    CONNECTION_RETRY_WAIT_TIME,
    DEFAULT_EXECUTOR_SERVER_IP,
    DEFAULT_RUNTIME,
    MAX_RETRIES_LOW,
    SINGLE_CPU_MEMORY_SHARE_MB
)


def grpc_event_consultor(
    partition_manager_q: Queue,
    externalize_part_qs: Dict[int, Queue],
    executor_finish_q: Queue,
    executor_id: int,
    stub: services_pb2_grpc.SchedulerStub
):
    """
    Thread used by serverless executors to continously consult if the workers
    have to execute any event.
    The events to execute can be of type write/read to/from external memory.

    Args:
        partition_manager_queue: Queue used to notify the partition manager
            new events (EventAddPartition).
        executor_worker_queues: Queues used to notify directly the threads of
            the workers that they have to write some partitions to external
            memory (EventSavePartitionCloud).
        executor_id: The id of the executor.
        stub: Stub used to communicate with the Stage server.
    """

    exec_info = services_pb2.ExecutorInformation(executor_id=executor_id)
    logger = get_logger("executor_consultor_events")

    logger.info("Event consultor running")

    def event_checker(
        _executor_id,
        grpc_call_future
    ):

        try:
            for event_grpc in grpc_call_future:

                if event_grpc.HasField('save_partition_info'):

                    save_part = event_grpc.save_partition_info
                    key = save_part.key
                    storage = save_part.storage
                    worker_id = save_part.worker_id_creator

                    event = EventSavePartitionCloud(
                        key,
                        storage,
                        worker_id
                    )
                    externalize_part_qs[worker_id].put(event)

                if event_grpc.HasField('partition_saved_info'):

                    part_saved = event_grpc.partition_saved_info
                    key = part_saved.key
                    num_read = part_saved.num_read
                    worker_id = part_saved.worker_id
                    executor_id = part_saved.executor_id
                    storage = part_saved.storage
                    event = EventAddPartition(
                        key,
                        num_read,
                        worker_id,
                        executor_id,
                        storage,
                        ""
                    )
                    partition_manager_q.put(event)

                if event_grpc.HasField('update_partition_info'):
                    update_part = event_grpc.update_partition_info
                    key = update_part.key
                    num_read = update_part.num_read
                    executor_id = update_part.executor_id_part
                    worker_id = update_part.worker_id_part
                    event = EventUpdatePartitionStage(
                        key,
                        num_read,
                        worker_id,
                        executor_id
                    )
                    partition_manager_q.put(event)

                if event_grpc.HasField('finish_executor'):

                    print("EVENT CHECKER: Executor finish signal received.")
                    executor_finish_q.put(1)

        except grpc.RpcError as e:
            # Handle gRPC errors, including Cancelled and Unavailable
            if e.code() == grpc.StatusCode.CANCELLED:
                print(
                    "EVENT CHECKER: gRPC call was cancelled.",
                    "Shutting down gracefully."
                )
            elif e.code() == grpc.StatusCode.UNAVAILABLE:
                print(
                    "EVENT CHECKER: gRPC server is unavailable.",
                    f"Details: {e.details()}"
                )
            else:
                print(
                    "EVENT CHECKER: An RPC error occurred:",
                    f"{e.code()}: {e.details()}"
                )

        except Exception as e:
            print(
                f"EVENT CHECKER: An unexpected error occurred: {e}"
            )

        print(f"({_executor_id}) EVENT CHECKER: Thread finished.")

    grpc_future = stub.ConsultEvents(exec_info)
    event_thread = threading.Thread(
        target=event_checker,
        args=(
            executor_id,
            grpc_future,
        )
    )
    event_thread.start()

    return grpc_future, event_thread


class ExecutorServer(services_pb2_grpc.ExecutorServicer):
    """
    Represents a executor grpc server that will be used by the Stage server.
    The Stage server will communicate to the Executor server when the workers
    have to save/read partitions to/from external memory in order to allow
    communication between executors.

    Attributes:
        server_ip (str): The IP address of the executor server.
        server_port (int): The port number on which the executor server
            listens for connections.
        partition_manager_queue (Queue): A queue for communication with the
            memory manager.
        executor_worker_queues (List[Queue]): A list of queues for
            communication with executor workers.
    """

    def __init__(
        self,
        executor_id: int,
        server_ip: str,
        server_port: int,
        partition_manager_q: Queue,
        externalize_part_qs: Dict[int, Queue],
        executor_finish_q: Queue
    ):
        self.executor_id = executor_id
        self.server_ip = server_ip
        self.server_port = server_port
        self.partition_manager_q = partition_manager_q
        self.externalize_part_qs = externalize_part_qs
        self.executor_finish_q = executor_finish_q

        self.logger = get_logger("ExecutorServer")

    def start_server(
        self,
        server_start_ack_q: Queue,
        server_end_q: Queue
    ):
        """
        Starts the executor server.

        Args:
        queue_notify_server_started: A queue to notify when the server has
            started. Since this moment it can be communicated to the Stage
            server that it can connect with this Executor server.
        """
        pool = futures.ThreadPoolExecutor(max_workers=2)
        server = grpc.server(pool)
        services_pb2_grpc.add_ExecutorServicer_to_server(self, server)
        executor_address = '{}:{}'.format(
            DEFAULT_EXECUTOR_SERVER_IP,
            self.server_port
        )
        server.add_insecure_port(executor_address)
        server.start()
        print("Started executor server at {}".format(executor_address))
        server_start_ack_q.put("Executor server started")
        server_end_q.get()

        server.stop(0)
        pool.shutdown(wait=False)

    def SavePartitionInCloud(
        self,
        request: services_pb2.SavePartitionInfo,
        context
    ):
        """
        Saves a partition in cloud storage.

        Args:
            request: The request containing information about the partition
                to be saved.
            context: The context of the GRPC request.

        Returns:
        pb2.SaveCloudRequestDone: A response indicating that the save request
            has been sent. The Stage server will communicate to the Executors
            that requested for the partitions that they can start requestin
            them from external memory (progressive wait).

        """
        event = EventSavePartitionCloud(
            key=request.key,
            storage=request.storage,
            worker_id=request.worker_id_creator
        )
        self.externalize_part_qs[event.worker_id].put(event)

        response = services_pb2.SaveCloudRequestDone(request_send=True)

        return response

    def ReadPartitionFromCloud(
        self,
        request: services_pb2.SavePartitionInfo,
        context
    ):
        """
        Reads a partition from cloud storage.

        Args:
        request: The request containing information about the partition to be
            read.
        context: The context of the GRPC request.

        Returns:
        pb2.EmptyResponse: An empty response indicating the completion of the
            read operation.
        """

        event = EventAddPartition(
            request.key,
            request.num_read,
            request.worker_id,
            request.executor_id,
            request.storage,
            ""
        )
        self.partition_manager_q.put(event)

        # Send a message to the Stage Server indicating that X num_read has
        # been done. Take into account that the reads will be done after
        # reading EventAddPartition and the worker receiving the event.
        # Probably the best moment to send that message to the Stage server
        # it's when the reading is done.

        # When isinstance(task_info.input_info, PartitionInfo) an
        # EventUpdatePartition is created. The number of readings to a
        # partition is specified in the input PartitionInfo of each task.

        return services_pb2.EmptyResponse()

    def UpdatePartitionExecutor(self, request, context):
        """
        This method is called by the Stage driver when another Executor reads
        partitions from a key that are created by this executor. Doing this
        the executor creator can manage when the num_reads is 0 and can delete
        the information to optimize the memory.

        Args:
        request: The request containing information about the partition to be
            read.
        context: The context of the GRPC request.

        Returns:
        pb2.EmptyResponse: An empty response indicating the completion of the
            read operation.
        """

        event = EventUpdatePartitionStage(
            request.key,
            request.num_read,
            request.worker_id_part,
            request.executor_id_part
        )
        self.partition_manager_q.put(event)

        return services_pb2.EmptyResponse()

    def FinishExecutor(
        self,
        request,
        context
    ):
        """
        The stage server, when all the tasks of the stage have been finished,
        will notify the executors that they can finish (close partition
        manager, server...). In the case of having a serverless executor a
        Server-side Streaming RPC is used.

        Args:
        request: The request containing information about the partition to be
            read.
        context: The context of the GRPC request.

        Returns:
        pb2.EmptyResponse: An empty response indicating the completion of the
            read operation.
        """
        print("Executor {} got finish signal".format(self.executor_id))
        self.executor_finish_q.put(1)

        return services_pb2.EmptyResponse()

    def Ping(
        self,
        request,
        context
    ):

        return services_pb2.EmptyResponse()


def run_partition_manager(memory_manager: PartitionManager):
    """
    Initializes the Partition Manager
    """
    memory_manager.run()


def run_scheduler(scheduler: TaskScheduler, stub):

    return scheduler.run(stub)

class Executor:
    """
    Represents an executor responsible for managing workers, task scheduling,
    and communication with the Stage server.

    Attributes:
        executor_id (int): The ID of the executor.
        bucket (str): The name of the storage bucket.
        num_workers (int): The number of workers associated with the executor.
        task_register (Dict[int, Task]): A dictionary mapping task IDs to
            their respective tasks.
        backend_type (BackendType): The type of backend used for execution.
        channel: A gRPC channel for communication with the Stage server.
        stub: A gRPC stub for calling Stage service methods.
        executor_worker_queues (Dict[int, Queue]): A dictionary mapping worker
            IDs to their respective queues for communication with the executor.
        executor_server_ip (str): The IP address of the executor server.
        executor_server_port (int): The port number of the executor server.
    """

    def __init__(
        self,
        executor_id: int,
        bucket: str,
        num_workers: int,
        driver_ip: str,
        driver_port: int,
        config: dict,
        task_register: Dict[int, Task],
        execution_backend: BackendType,
        executor_server_port: int,
        executor_ip: str,
        internal_backend: IOBackend = IOBackend.DISK,
        runtime: str = DEFAULT_RUNTIME,
        runtime_memory: int = SINGLE_CPU_MEMORY_SHARE_MB,
        cpus_per_worker: int = None,
        eager_io: bool = False
    ):

        self.logger = get_logger(f"Executor {executor_id}")
        self.executor_id = executor_id
        self.bucket = bucket
        self.num_workers = num_workers
        self.config = config
        self.task_register = task_register
        self.backend_type = execution_backend
        self.external_backend = IOBackend.EXTERNAL
        driver_address = '{}:{}'.format(
            driver_ip,
            driver_port
        )
        self.driver_address = driver_address
        self.channel = grpc.insecure_channel(target=driver_address)
        self.stub = services_pb2_grpc.SchedulerStub(self.channel)
        self.executor_server_ip = executor_ip
        self.executor_server_port = executor_server_port
        self.internal_backend = internal_backend
        self.runtime = runtime
        self.runtime_memory = runtime_memory
        self.cpus_per_worker = cpus_per_worker
        self.eager_io = eager_io
        self.logger.info(f"Executor {self.executor_id} configured " +
                         "the channel and the stub correctly")

    def register_executor(
        self,
        executor_server_ip: str,
        executor_server_port: int
    ):

        request = services_pb2.ExecutorAddress(
            executor_id=self.executor_id,
            backend_type=self.backend_type.value,
            executor_server_ip=executor_server_ip,
            executor_server_port=executor_server_port,
            workers=self.num_workers,
            runtime_memory=self.runtime_memory
        )

        for attempt in range(MAX_RETRIES_LOW):
            try:
                active_job = self.stub.RegisterExecutorAddress(
                    request,
                    timeout=5
                )
                if active_job.value is False:
                    self.server_end_q.put(1)
                    return False
                else:
                    return True

            except grpc.RpcError:
                if attempt < MAX_RETRIES_LOW - 1:
                    time.sleep(CONNECTION_RETRY_WAIT_TIME)
                    continue
                self.server_end_q.put(1)
                print(
                    "Could not connect to the driver after retries.",
                    "Finishing execution."
                )
                return False

    def run(self):

        is_container = is_container_backend(self.backend_type)
        is_cf = self.backend_type == BackendType.CLOUD_FUNCTION

        self.set_data_structures(is_cf)

        if not is_container:

            executor_server = ExecutorServer(
                self.executor_id,
                self.executor_server_ip,
                self.executor_server_port,
                self.partition_manager_q,
                self.externalize_part_qs,
                self.executor_finish_q
            )
            executor_server_process = Process(
                target=executor_server.start_server,
                args=(
                    self.server_start_ack_q,
                    self.server_end_q,
                )
            )
            executor_server_process.start()

            self.server_start_ack_q.get()

            resp = self.register_executor(
                self.executor_server_ip,
                self.executor_server_port
            )
            if resp is False:
                return []

        else:
            resp = self.register_executor("", -1)
            if resp is False:
                return []

            grpc_future, event_thread = grpc_event_consultor(
                self.partition_manager_q,
                self.externalize_part_qs,
                self.executor_finish_q,
                self.executor_id,
                self.stub
            )

        workers = [
            Worker(
                worker_id=w_i,
                executor_id=self.executor_id,
                bucket=self.bucket,
                config=self.config,
                task_queue=self.worker_task_qs[w_i],
                task_end_q=self.task_end_q,
                task_register=self.task_register,
                execution_backend=self.backend_type,
                external_backend=self.external_backend,
                internal_backend=self.internal_backend,
                worker_recv_part_q=self.worker_recv_part_qs[w_i],
                grouped_partitions_q=self.grouped_parts_qs[w_i],
                ungrouped_partitions_q=self.ungrouped_parts_qs[w_i],
                partition_manager_q=self.partition_manager_q,
                externalize_part_q=self.externalize_part_qs[w_i],
                eager_io=self.eager_io
            )
            for w_i in range(self.num_workers)
        ]

        worker_processes = [
            Process(
                target=run_worker,
                args=(workers[w_i],)
            )
            for w_i in range(self.num_workers)
        ]

        _ = [p.start() for p in worker_processes]

        if self.cpus_per_worker is not None:
            self.allocate_vcpus(worker_processes, self.cpus_per_worker)

        task_exec_info = []
        scheduler = TaskScheduler(
            worker_task_qs=self.worker_task_qs,
            task_end_q=self.task_end_q,
            partition_manager_q=self.partition_manager_q,
            pmanager_response_q=self.pmanager_response_q,
            executor_finish_q=self.executor_finish_q,
            executor_id=self.executor_id,
            task_exec_info=task_exec_info
        )
        scheduler_thread = Thread(
            target=run_scheduler,
            args=(scheduler, self.stub)
        )
        scheduler_thread.start()

        partition_manager = PartitionManager(
            worker_task_qs=self.worker_task_qs,
            partition_manager_q=self.partition_manager_q,
            pmanager_response_q=self.pmanager_response_q,
            worker_recv_part_qs=self.worker_recv_part_qs,
            grouped_partitions_qs=self.grouped_parts_qs,
            ungrouped_partition_qs=self.ungrouped_parts_qs,
            externalize_parts_qs=self.externalize_part_qs,
            stub=self.stub,
            executor_id=self.executor_id,
            config=self.config,
            internal_storage=self.internal_backend
        )

        memory_manager_thread = Thread(
            target=run_partition_manager,
            args=(partition_manager,)
        )
        memory_manager_thread.start()

        _ = [p.join() for p in worker_processes]
        self.logger.info(
            color_string(
                f"({self.executor_id}) Worker processes joint",
                AsciiColors.CYAN
            )
        )

        scheduler_thread.join()
        self.logger.info(
            color_string(
                f"({self.executor_id}) Scheduler process joint",
                AsciiColors.CYAN
            )
        )

        memory_manager_thread.join()
        self.logger.info(
            color_string(
                f"({self.executor_id}) Memory manager process joint",
                AsciiColors.CYAN
            )
        )

        if not is_container:
            self.server_end_q.put(1)
            executor_server_process.join()
        else:
            grpc_future.cancel()
            event_thread.join()

        self.logger.info(
            color_string(
                f"({self.executor_id}) Executor server process joint",
                [AsciiColors.BOLD, AsciiColors.CYAN]
            )
        )

        if (
            self.internal_backend == IOBackend.DISK and
            self.backend_type == BackendType.CLOUD_VM
        ):
            int_path = os.path.join(LITHOPS_TEMP_DIR, self.bucket)
            shutil.rmtree(int_path)
            self.logger.info(f"Removed all files in {int_path}")

        if not is_container_backend(self.backend_type):
            if executor_server_process.is_alive():
                executor_server_process.terminate()
                executor_server_process.join()

        for worker_process in worker_processes:
            if worker_process.is_alive():
                worker_process.terminate()
                worker_process.join()

        self.channel.close()

        return task_exec_info

    def set_cpu_affinity(self, process: Process, cpus: List[int]):
        """Sets CPU affinity for a process."""
        try:
            process.cpu_affinity(cpus)
        except AttributeError:
            os.sched_setaffinity(process.pid, set(cpus))

    def allocate_vcpus(self, processes: List[Process], vcpus_per_process: int):
        """
        Assigns vCPUs to processes in order.

        :param processes: List of process IDs or names.
        :param vcpus_per_process: Number of vCPUs required per process.
        """
        total_vcpus = get_total_vcpus()
        available_vcpus = total_vcpus
        start_cpu = 0

        for process in processes:
            if available_vcpus >= vcpus_per_process:
                assigned_cpus = list(
                    range(start_cpu, start_cpu + vcpus_per_process)
                )
                self.set_cpu_affinity(process, assigned_cpus)
                start_cpu += vcpus_per_process
                available_vcpus -= vcpus_per_process

    def set_data_structures(self, is_cf: bool):

        if is_cf:
            CustomQueue = CFQueue
            # CustomQueue = SMQueue
        else:
            CustomQueue = SMQueue

        # For scheduling tasks (things that workers have to proactively do)
        self.worker_task_qs = {
            w_i: CustomQueue() for w_i in range(self.num_workers)
        }
        # For executor->worker communication
        self.externalize_part_qs = {
            w_i: CustomQueue() for w_i in range(self.num_workers)
        }

        # Sending partitions from partition manager to workers.
        # Is it necessary to have so many queues?
        self.worker_recv_part_qs = {
            w_i: CustomQueue() for w_i in range(self.num_workers)
        }
        # Only for queues backend
        self.grouped_parts_qs = {
            w_i: CustomQueue() for w_i in range(self.num_workers)
        }
        self.ungrouped_parts_qs = {
            w_i: CustomQueue() for w_i in range(self.num_workers)
        }

        # For the executor, to signal its finish
        # Executor Server will communicate to the TaskScheduller when
        # close the workers, PartitionManager and finally close the Executor.
        self.executor_finish_q = CustomQueue()

        # For worker->partition_manager communication
        self.partition_manager_q = CustomQueue()
        self.pmanager_response_q = CustomQueue()
        # To signal the ending of workers
        self.task_end_q = CustomQueue()

        if not is_cf:
            self.server_start_ack_q = CustomQueue()
        self.server_end_q = CustomQueue()


def run_executor(executor_instance: ExecutorInstance):

    # Executor-specific information
    executor_info = executor_instance.executor_info
    executor_id = executor_instance.id
    task_register = executor_instance.task_register
    executor_server_port = executor_instance.port
    executor_ip = executor_instance.executor_ip
    runtime_memory = executor_instance.runtime_memory
    num_workers = executor_instance.num_workers
    cpus_per_worker = executor_instance.cpus_per_worker

    # Backend setup information
    bucket = executor_info.bucket
    config = executor_info.lithops_config
    execution_backend = executor_info.execution_backend
    driver_ip = executor_info.driver_ip
    driver_port = executor_info.driver_port
    internal_backend = executor_info.internal_backend
    runtime = executor_info.runtime
    eager_io = executor_info.eager_io

    executor_start_time = time.time()
    executor = Executor(
        executor_id,
        bucket,
        num_workers,
        driver_ip,
        driver_port,
        config,
        task_register,
        execution_backend,
        executor_server_port,
        executor_ip,
        internal_backend=internal_backend,
        runtime=runtime,
        runtime_memory=runtime_memory,
        cpus_per_worker=cpus_per_worker,
        eager_io=eager_io
    )

    logger = get_logger()
    logger.info(f"Starting executor {executor_id}")

    task_exec_info = executor.run()
    executor_end_time = time.time()

    executor_info = {
        "executor_id": executor_id,
        "execution_backend": execution_backend.value,
        "internal_io_backend": internal_backend.value,
        "execution_start": executor_start_time,
        "execution_end": executor_end_time,
        "num_workers": num_workers,
        "runtime_memory": runtime_memory,
        "cpus_per_worker": cpus_per_worker,
        "runtime": runtime
    }
    logs = [{
        "executor_info": executor_info,
        "task_exec_info": task_exec_info
    }]

    if is_container_backend(execution_backend):
        return logs
    else:
        executor_instance.exec_info.extend(logs)
