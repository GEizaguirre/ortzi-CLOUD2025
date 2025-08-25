from dataclasses import dataclass
from typing import (
    Dict,
    List
)

from ortzi.io_utils.info import IOBackend
from ortzi.io_utils.queues import Queue
from ortzi.event import (
    EventAddPartition,
    EventDeleteKeyDisk,
    EventDeleteKeyS3,
    EventEndPartitionManager,
    EventMemory,
    EventPartitionInQueues,
    EventPartitionNotInQueues,
    EventPartitionReady,
    EventPartitionSavedInCloud,
    EventRequestPartition,
    EventSavePartitionCloud,
    EventUpdatePartitionStage
)
from ortzi.logging.ui_log import get_logger
import ortzi.proto.services_pb2 as pb2
from ortzi.proto.services_pb2_grpc import SchedulerStub


@dataclass
class PartitionEntry:
    storage: IOBackend
    # Times this partition is going to be read
    num_read: int
    worker_id: int
    executor_id: int
    # If the storage is a queue this will contain
    # the serialized data. If not, will contain ""
    data: bytes
    externalized: bool = False


class PartitionManager():
    """
    Manages partitions and their related operations within the executor.

    Attributes:
        worker_task_qs (Dict[int, Queue]): A dictionary mapping worker IDs to
            their respective queues.
        partition_info (Dict[str, PartitionEntry]): A dictionary containing
            information about each partition, including the number of reads,
            worker ID, storage type, and data.
        partition_manager_q (Queue): A queue for communication with the
            this partition manager.
        worker_recv_part_qs (Dict[int, Queue]): Queues to communicate
            partition manager with IO_handler.get_partitions.
        grouped_partitions_qs (Dict[int, Queue]): Queues to
            communicate partition manager with
            IO.queue_client.get_partitions_grouped().
        ungrouped_partitions_qs (Dict[int, Queue]): Queue to communicate
            partition manager with IO.queue_client.get_partitions().
        requested_partitions_sm (Dict): A dictionary to store requested
            partitions.
        requested_partitions (Dict): A dictionary to store requested
            partitions.
        stub: Stub for communication with Stage server.
        executor_id: ID of the executor.
    """

    def __init__(
        self,
        worker_task_qs: Dict[int, Queue],
        partition_manager_q: Queue,
        pmanager_response_q: Queue,
        worker_recv_part_qs: Dict[int, Queue],
        grouped_partitions_qs: Dict[int, Queue],
        ungrouped_partition_qs: Dict[int, Queue],
        externalize_parts_qs: Dict[int, Queue],
        stub: SchedulerStub,
        executor_id: int,
        config: dict,
        internal_storage: IOBackend,
        delete_memories: bool = True
    ):
        self.logger = get_logger(f"PartitionManager{executor_id}")
        self.worker_task_qs = worker_task_qs
        # Contains for each partition the obj_ref (where
        # is saved), num of lectures to be done and the id
        # of the worker which created the shmem
        self.partition_info: Dict[str, PartitionEntry] = {}
        self.partition_manager_q = partition_manager_q
        self.pmanager_response_q = pmanager_response_q
        self.worker_recv_part_qs = worker_recv_part_qs
        self.grouped_partitions_qs = grouped_partitions_qs
        self.ungrouped_partition_qs = ungrouped_partition_qs
        self.requested_partitions_sm = {}
        self.externalize_parts_qs = externalize_parts_qs
        self.requested_partitions: Dict[str, List] = {}
        self.stub = stub
        self.executor_id = executor_id

        self.delete_memories = delete_memories
        self.external_is_disk = config["lithops"]["storage"] == "localhost"
        self.internal_is_disk = (
            internal_storage == IOBackend.DISK or
            (
                internal_storage == IOBackend.EXTERNAL
                and config["lithops"]["storage"] == "localhost"
            )
        )
        self.disk_partitions = 0

        self.final_partitions = set()
        self.finalization_protocol = False
        self.finalize = False

    def add_partition(
        self,
        key: str,
        num_read: int,
        worker_id: int,
        storage: str,
        executor_id: int,
        serialized_data: Dict[int, bytes]
    ):
        """
        Adds a partition to the partition manager.

        Args:
        key (str): The key of the partition.
        num_read (int): The number of reads required for the partition.
        worker_id (int): The ID of the worker creating the partition.
        storage (str): The storage type of the partition.
        serialized_data (Dict[int, pyarrow.RecordBatch]):
            Serialized data of the partition (Needed when the partitions are
            saved with queues).
        """

        io_backend = IOBackend(storage)

        if key not in self.partition_info:
            self.partition_info[key] = PartitionEntry(
                io_backend,
                num_read,
                worker_id,
                executor_id,
                serialized_data
            )

        if key in self.requested_partitions:
            list_worker_id = self.requested_partitions[key]
            for w_id, exchange_id, partition_id, executor_id in list_worker_id:
                event = EventPartitionReady(
                    storage,
                    exchange_id,
                    key,
                    partition_id,
                    self.partition_info[key].data
                )
                self.worker_recv_part_qs[w_id].put(event)

            del self.requested_partitions[key]

            # That implies that the key can be read in this executor because
            # the information is stored in the same machine as this executor
            # (localhost - shmem or disk) or the key is stored in external
            # memory. If that's the situation we have to update the number of
            # reads done to this key in the Partition Manager of the executor
            # that created the k
            # if io_backend == IOBackend.DISK or (io_backend == IOBackend.EXTERNAL and self.external_is_disk):
            #     self.disk_partitions += 1
            #     if self.disk_partitions >= MAX_PARTITIONS_FFLUSH:
            #         os.sync()
            #         self.disk_partitions = 0

    def update_partition(
        self,
        key: str,
        num_read_done: int
    ):
        """
        Updates the status of a partition. If all reads from a partition are
        done, and this partition is saved in shmem -> unlink shmem to free up
        space.

        Args:
            key (str): The key of the partition.
            num_read_done (int): The number of reads completed for the
                partition.
            worker_id (int): The ID of the worker updating the partition.
        """

        if key in self.partition_info:
            self.partition_info[key].num_read -= num_read_done
            creator_worker_id = self.partition_info[key].worker_id

            if self.partition_info[key].num_read <= 0:
                if self.delete_memories:
                    storage_backend = self.partition_info[key].storage
                    if storage_backend == IOBackend.MEMORY:
                        self.worker_task_qs[creator_worker_id].put(
                            EventMemory(shmem_key=key, unlink_all=False)
                        )

                    elif storage_backend == IOBackend.DISK:
                        self.worker_task_qs[creator_worker_id].put(
                            EventDeleteKeyDisk(key=key)
                        )

                    elif storage_backend == IOBackend.EXTERNAL:
                        self.worker_task_qs[creator_worker_id].put(
                            EventDeleteKeyS3(key=key)
                        )

                    del self.partition_info[key]

    def partition_saved_in_cloud(
        self,
        key: str
    ):
        """
        Notify the stage server that now the partitions are available in the
        external memory. The stage server can notify the executors (that
        requested the partitions) that they can read them from external
        memory.

        Args:
            key (str): The key of the partition.
        """
        if key in self.partition_info.keys():
            self.partition_info[key].externalized = True
        request = pb2.PartitionKey(
            key=key,
            executor_id=self.executor_id
        )
        _ = self.stub.PartitionSavedExternal(request)

        # Final externalization of partitions
        if self.finalization_protocol:
            if key in self.final_partitions:
                self.final_partitions.remove(key)
                if len(self.final_partitions) == 0:
                    self.finalize = True

    def consult_partition_in_queues(
        self,
        key,
        worker_id,
        exchange_id,
        partition_id
    ):
        """
        Consults the availability of a partition in queues. Method used by the
        workers trying to read the partitions from queues.

        Args:
            key: The key of the partition.
            worker_id: The ID of the worker requesting the partition.
            exchange_id: The exchange ID of the partition.
            partition_id: The ID of the partition.
        """

        partition_sent = False

        if key in self.partition_info:
            if self.partition_info[key].storage == IOBackend.QUEUE:
                event = EventPartitionReady(
                    self.partition_info[key].storage.value,
                    exchange_id,
                    key,
                    partition_id,
                    self.partition_info[key].data
                )
                if exchange_id == -1 and partition_id == -1:
                    self.grouped_partitions_qs[worker_id].put(event)
                else:
                    self.ungrouped_partition_qs[worker_id].put(event)

                partition_sent = True

        if not partition_sent:
            event = EventPartitionNotInQueues(
                exchange_id,
                key,
                partition_id
            )
            if exchange_id == -1 and partition_id == -1:
                self.grouped_partitions_qs[worker_id].put(event)
            else:
                self.ungrouped_partition_qs[worker_id].put(event)

    def register_requested_partition(
        self,
        key,
        worker_id,
        exchange_id,
        partition_id
    ):
        """
        Registers a requested partition. If the partition is not actually in
        local memory, notify the Stage server to register it also (another
        Executor can have it in its local memory).

        Args:
            key: The key of the partition.
            worker_id: The ID of the worker requesting the partition.
            exchange_id: The exchange ID of the partition.
            partition_id: The ID of the partition.
        """

        if key in self.requested_partitions:
            self.requested_partitions[key].append(
                (worker_id, exchange_id, partition_id, self.executor_id)
            )
        else:
            self.requested_partitions[key] = [
                (worker_id, exchange_id, partition_id, self.executor_id)
            ]

        if key in self.partition_info:
            list_worker_id = self.requested_partitions[key]
            for w_id, exch_id, p_id, exec_id in list_worker_id:
                event = EventPartitionReady(
                    self.partition_info[key].storage.value,
                    exch_id,
                    key,
                    p_id,
                    self.partition_info[key].data
                )
                self.worker_recv_part_qs[w_id].put(event)

            # SUPOSANT QUE NO ES DEMANARA MES
            del self.requested_partitions[key]

        else:
            request = pb2.RequestPartition(
                key=key,
                worker_id=worker_id,
                exchange_id=exchange_id,
                partition_id=partition_id,
                executor_id=self.executor_id
            )
            _ = self.stub.ServerRegisterRequestedPartition(request)

    def run(self):
        """
        Thread that takes care of receiving the events that the
        Partition Manager has to control.
        """

        while True:
            if self.finalize:
                self.pmanager_response_q.put(1)
                break
            event = self.partition_manager_q.get()
            if isinstance(event, EventAddPartition):
                self.add_partition(
                    event.key,
                    event.num_read,
                    event.worker_id,
                    event.storage,
                    event.executor_id,
                    event.serialized_data
                )
            elif isinstance(event, EventUpdatePartitionStage):
                self.update_partition(
                    event.key,
                    event.num_read
                )
            elif isinstance(event, EventRequestPartition):
                self.register_requested_partition(
                    event.key,
                    event.worker_id,
                    event.exchange_id,
                    event.partition_id
                )
            elif isinstance(event, EventPartitionInQueues):
                self.consult_partition_in_queues(
                    event.key,
                    event.worker_id,
                    event.exchange_id,
                    event.partition_id
                )
            elif isinstance(event, EventPartitionSavedInCloud):
                self.partition_saved_in_cloud(event.key)
            elif isinstance(event, EventEndPartitionManager):
                # Final externalization of partitions
                for key in self.partition_info.keys():
                    partition_info = self.partition_info[key]
                    if partition_info.executor_id != self.executor_id:
                        continue
                    if not partition_info.externalized:
                        worker_id = partition_info.worker_id
                        self.externalize_parts_qs[worker_id].put(
                            EventSavePartitionCloud(
                                key,
                                IOBackend.EXTERNAL,
                                worker_id
                            )
                        )
                        partition_info.externalized = True
                        self.final_partitions.add(key)
                if len(self.final_partitions) == 0:
                    self.finalize = True
                else:
                    self.finalization_protocol = True
