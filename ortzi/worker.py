from threading import Thread
from typing import Dict
import time

from lithops.storage import Storage

from ortzi.io_utils.queues import Queue
from ortzi.io_utils.info import (
    IOBackend,
    iobackend_to_lithops
)
from ortzi.io_utils.io_handler import IOHandler
from ortzi.logging.ui_log import get_logger
from ortzi.utils import (
    AsciiColors,
    print_color
)
from ortzi.task import Task
from ortzi.event import (
    EventTaskEnd,
    EventType
)
from ortzi.backends.backend import BackendType
from ortzi.event import EventPartitionSavedInCloud


class Worker:
    """
    Represents a worker that executes tasks.

    Attributes:
        - worker_id (int): The ID of the worker.
        - bucket (str):
        - task_queue (Queue): Queue for receiving events.
        - task_end_queue (Queue): Queue for sending to the
            executor that the task has been executed.
        - task_register (Dict[int, Task]): Dictionary containing registered
            tasks.
        - executor_id (int): ID of the executor.
        - backend_type: BackendType of the executor that uses this worker.
        - worker_recv_part_q (Queue): Queue to communicate
            partition manager with IO_handler.get_partitions.
        - grouped_partitions_q (Queue): Queue to
            communicate partition manager with
            IO.queue_client.get_partitions_grouped().
        - memory_worker_queue (Queue): Queue to communicate
            partition manager with IO.queue_client.get_partitions().
        - executor_worker_queue (Queue): Queue to communicate
            the Executor with the worker thread directly.
    """

    def __init__(
        self,
        worker_id: int,
        executor_id: int,
        bucket: str,
        config: dict,
        task_queue: Queue,
        task_end_q: Queue,
        task_register: Dict[int, Task],
        execution_backend: BackendType,
        external_backend: IOBackend,
        internal_backend: IOBackend,
        worker_recv_part_q: Queue,
        grouped_partitions_q: Queue,
        ungrouped_partitions_q: Queue,
        partition_manager_q: Queue,
        externalize_part_q: Queue,
        eager_io: bool = True
    ):

        self.worker_id = worker_id
        self.executor_id = executor_id
        self.logger = get_logger(f"({self.executor_id})"
                                 f"Worker {self.worker_id}")

        external_storage = Storage(
            config=config,
            backend=iobackend_to_lithops(config, external_backend)
        )
        disk_storage = Storage(
            config=config,
            backend="localhost"
        )

        self.io_handler = IOHandler(
            bucket=bucket,
            external_storage=external_storage,
            disk_storage=disk_storage,
            worker_id=worker_id,
            executor_id=executor_id,
            backend_type=execution_backend,
            worker_recv_part_q=worker_recv_part_q,
            grouped_partitions_q=grouped_partitions_q,
            ungrouped_partitions_q=ungrouped_partitions_q,
            partition_manager_q=partition_manager_q,
            io_backend=internal_backend,
            eager_io=eager_io
        )
        self.task_queue = task_queue
        self.task_end_q = task_end_q
        self.task_register = task_register
        self.partition_manager_q = partition_manager_q
        self.externalize_part_q = externalize_part_q
        self.execution_backend = execution_backend

    def write_partitions_to_cloud(self):
        """
        Thread that is in charge of receiving events to write partitions to
        external memory. That's done in order to make available the
        partitions for other executors.
        """

        while True:
            event = self.externalize_part_q.get()

            if event is None:
                break

            # Process event
            try:
                poss_new_key = self.io_handler.write_from_localhost_to_cloud(
                    event.key
                )
                ev = EventPartitionSavedInCloud(key=poss_new_key)
                self.partition_manager_q.put(ev)

            except Exception as e:
                self.logger.info("WRITE_PARTITIONS Partitions couldn't be"
                                 f"written to external memory (error: {e})")

    def run(self):
        """
        Runs the worker. Is in charge of receiving and managing the events.
        """

        try:

            thread_writer = Thread(target=self.write_partitions_to_cloud)
            thread_writer.start()

            while True:

                event = self.task_queue.get()

                if event.event_type == EventType.VOID:
                    time.sleep(event.sleep)
                    self.task_end_q.put(
                        EventTaskEnd(
                            None,
                            self.worker_id,
                            self.executor_id,
                            None,
                            None,
                            None,
                            None,
                            None
                        ),
                        block=False
                    )

                if event.event_type == EventType.END:
                    break

                if event.event_type == EventType.TASK:

                    # Get corresponding task and run it
                    task = self.task_register[event.task_info.stage_id]
                    try:
                        storage_output_backend, storage_input_keys, \
                            storage_output_key, serialized_data_output, \
                            exec_info = task.run(
                                self.worker_id,
                                self.executor_id,
                                event.task_info,
                                self.io_handler,
                                self.execution_backend
                            )
                    except Exception as e:
                        print(
                            "Error executing task",
                            f"{event.task_info.task_id}"
                        )
                        print(e)
                        raise e

                    # Signal Task ends
                    self.task_end_q.put(
                        EventTaskEnd(
                            event.task_info,
                            self.worker_id,
                            self.executor_id,
                            storage_output_backend,
                            storage_input_keys,
                            storage_output_key,
                            serialized_data_output,
                            exec_info
                        ),
                        block=False
                    )

                if event.event_type == EventType.MEMORY:
                    if (event.unlink_all):
                        self.io_handler.unlink_all_shared_memories()
                    else:
                        self.io_handler.unlink_shared_memory(event.shmem_key)

                if event.event_type == EventType.DELETE_KEY_S3:
                    self.io_handler.delete_key_from_s3(event.key)

                if event.event_type == EventType.DELETE_KEY_DISK:
                    self.io_handler.delete_key_from_disk(event.key)

            # Stop task externalizer thread
            self.externalize_part_q.put(None)

            print_color(
                f"Finishing worker {self.worker_id} ({self.executor_id})",
                AsciiColors.GREEN
            )

            thread_writer.join()

            self.io_handler.unlink_all_shared_memories()

        except Exception as e:
            print_color(
                f"Error in worker {self.worker_id} ({self.executor_id}): {e}",
                AsciiColors.RED
            )
            raise e


def run_worker(worker: Worker):
    """
    Function to run a worker.

    Args:
        worker (Worker): The worker instance to run.
    """
    worker.run()
