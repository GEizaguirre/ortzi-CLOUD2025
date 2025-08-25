import asyncio
from typing import (
    List,
    Union,
    Tuple
)
import time
import random

import numpy as np
from lithops.storage import Storage

from ortzi.io_utils.queues import (
    Queue,
    get_async
)
from ortzi.io_utils.io_clients import (
    IOExternal,
    IOMemory,
    IOQueues,
    IODisk,
    IOResult
)
from ortzi.io_utils.info import (
    IOBackend,
    IOInfo,
    PartitionInfo,
    AuxiliaryInfo,
    AUXILIARY_PREFIX
)
from ortzi.io_utils.serialize import (
    serialize_partitions,
    serialize_partitions_memory,
    deserialize
)
from ortzi.struct.dataframe import (
    _DataFrame,
    concat_progressive
)
from ortzi.config import (
    META_SUFFIX,
    NUM_PREFIXES,
    config
)
from ortzi.backends.backend import BackendType
from ortzi.event import EventRequestPartition
from ortzi.logging.ui_log import get_logger

MET_SUFIX = "meta"


class IOHandler:
    """
    Handles I/O operations for different storage backends.

    Attributes:
        - bucket: Bucket name for storage.
        - external_storage: Storage backend object for external memory.
        - disk_storage: Storage backend object for disk memory.
        - worker_id: ID of the worker.
        - backend_type: Type of backend.
        - memory_worker_comm: Queue to communicate partition manager with
            IO_handler.get_partitions.
        - memory_worker_queue_grouped: Queue to communicate partition manager
            with IO.queue_client.get_partitions_grouped().
        - memory_worker_queue: Queue to communicate partition manager with
            IO.queue_client.get_partitions().

    """

    prefixes: List[str]
    exchange_id: str

    def __init__(
        self,
        bucket: str,
        external_storage: Storage,
        disk_storage: Storage,
        worker_id: int,
        executor_id: int,
        backend_type: BackendType,
        worker_recv_part_q: Queue,
        grouped_partitions_q: Queue,
        ungrouped_partitions_q: Queue,
        partition_manager_q: Queue,
        io_backend: IOBackend = IOBackend.DISK,
        eager_io: bool = True
    ):

        self.worker_id = worker_id
        self.backend_type = backend_type
        self.worker_recv_part_q = worker_recv_part_q
        self.grouped_partitions_q = grouped_partitions_q
        self.ungrouped_partitions_q = ungrouped_partitions_q
        self.partition_manager_q = partition_manager_q
        self.eager_io = eager_io

        self.memory_client = IOMemory()
        self.queue_client = IOQueues(
            worker_id=worker_id,
            executor_id=executor_id,
            grouped_partitions_q=grouped_partitions_q,
            ungrouped_partitions_q=ungrouped_partitions_q,
            partition_manager_q=partition_manager_q
        )
        self.disk_client = IODisk(
            bucket,
            disk_storage
        )
        self.external_client = IOExternal(
            bucket,
            external_storage
        )

        self.shmem_opened = []
        self.prefixes = config.get("prefixes")
        self.io_backend = io_backend
        self.executor_id = executor_id

        self.logger = get_logger(f"IOHandler-Worker{self.worker_id}")

    def gen_key(
        self,
        partition_info: PartitionInfo
    ) -> List[Tuple[int, str, int]]:
        """
        Generates keys for data partitions that have to be saved.

        Args:
        - partition_info: Information about the partitions.

        Returns:
        - List of tuples containing exchange ID, key, and partition ID.
        """

        exchange_key_partition = []

        exchange_id = partition_info.exchange_id
        source_tasks = partition_info.source_tasks
        parts_per_file = partition_info.num_partitions // source_tasks

        for pid in partition_info.partition_ids:
            file_id = pid // parts_per_file
            prefix_id = file_id % NUM_PREFIXES
            key = f"{self.prefixes[prefix_id]}_{exchange_id}_{file_id}"
            exchange_key_partition.append((exchange_id, key, pid))

        return exchange_key_partition

    def get_auxiliary(
        self,
        auxiliary_info: AuxiliaryInfo
    ):
        auxiliary_key = f"{AUXILIARY_PREFIX}_{auxiliary_info.exchange_id}"
        auxiliary_data = self.external_client.read_aux(auxiliary_key)
        return auxiliary_data

    def get_partitions(
        self,
        task_id: int,
        partition_info: Union[
            PartitionInfo,
            List[PartitionInfo],
            AuxiliaryInfo
        ],
    ) -> IOResult:

        """
        Tries to retrieve the partitions from different storage backends
        in this order: shared memory, queues, disk, and external memory.
        If the partitions are not in any storage, request them to
        the Partition Manager of the executor.

        Args:
        - partition_info: Information about the partitions.

        Returns:
        - Tuple containing dataframes, shared memory data, exchange IDs,
          dataframe sizes, and exchange key partitions.
        """

        # Split the partitions from memory and from external storage
        if isinstance(partition_info, (PartitionInfo, IOInfo)):
            partition_info = [partition_info]

        partition_sizes = []
        deserialization_times = []
        read_times = []
        sm_data_opened = []

        memory_results = []
        queue_results = []
        disk_results = []
        external_results = []

        exchange_key_partition = [
            ekp
            for pi in partition_info
            for ekp in self.gen_key(pi)
        ]

        print(
            f"Task ID {task_id} - ",
            f"Getting {len(exchange_key_partition)} partitions"
        )

        try:
            # We first check what we have in memory
            (
                memory_results,
                sm_data_opened,
                keys_not_found,
                _partition_sizes,
                _deserialization_times,
                _read_times
            ) = asyncio.run(
                self.memory_client.get_partitions(exchange_key_partition)
            )

            partition_sizes.extend(_partition_sizes)
            deserialization_times.extend(_deserialization_times)
            read_times.extend(_read_times)

            # Then we access other optional storage backends
            if self.io_backend == IOBackend.QUEUE:
                (
                    queue_results,
                    keys_not_found,
                    _partition_sizes,
                    _deserialization_times,
                    _read_times
                ) = asyncio.run(
                    self.queue_client.get_partitions(keys_not_found)
                )
            elif self.io_backend == IOBackend.DISK:
                (
                    disk_results,
                    keys_not_found,
                    _partition_sizes,
                    _deserialization_times,
                    _read_times
                ) = asyncio.run(
                    self.disk_client.get_partitions(
                        keys_not_found,
                        fast=self.eager_io
                    )
                )
        except Exception as e:
            print(
                f"Task ID {task_id} - ",
                f"Error retrieving partitions: {e}"
            )
            raise e

        print(
            f"Task ID {task_id} - ",
            f"{len(keys_not_found)} Partitions not found locally"
        )

        partition_sizes.extend(_partition_sizes)
        deserialization_times.extend(_deserialization_times)
        read_times.extend(_read_times)

        # Finally we check if the requested partitions have been made public,
        # bypassing the PartitionManager
        if self.io_backend == IOBackend.EXTERNAL or len(keys_not_found) > 0:

            print(
                f"Task ID {task_id} - ",
                f"Requesting {len(keys_not_found)} partitions from external storage"
            )
            (
                external_results,
                keys_not_found,
                _partition_sizes,
                _deserialization_times,
                _read_times
            ) = asyncio.run(
                self.external_client.get_partitions(
                    keys_not_found,
                    fast=self.eager_io
                )
            )
            partition_sizes.extend(_partition_sizes)
            deserialization_times.extend(_deserialization_times)
            read_times.extend(_read_times)

        if len(keys_not_found) > 0:

            i = len(keys_not_found)
            # We request to the PartitionManager of the executor the missing
            # partitions.
            # The PartitionManager will request them to the Driver.
            for exchange_id, key_partition, partition_id in keys_not_found:
                event = EventRequestPartition(
                    key_partition,
                    self.worker_id,
                    exchange_id,
                    partition_id
                )
                self.partition_manager_q.put(event)
                i -= 1

            random_number = random.randint(1, 2000)
            keys_not_found_copy = keys_not_found.copy()

            # Fetch missing partitions asynchronously
            async def fetch_partition(p_i):

                nonlocal memory_results
                nonlocal random_number
                nonlocal keys_not_found_copy

                event = await get_async(self.worker_recv_part_q)
                print(
                    f"Task ID {task_id} fetching partition: ",
                    f"{p_i} from {event.storage_backend}"
                )

                keys_not_found_copy.remove(
                    (event.exchange_id, event.key, event.partition_id)
                )

                if event.storage_backend == IOBackend.MEMORY.value:
                    (
                        mem_res,
                        shmem_op,
                        keys_not_f,
                        _partition_sizes,
                        _deserialization_times,
                        _read_times
                    ) = await self.memory_client.get_partitions(
                        [(
                            event.exchange_id,
                            event.key,
                            event.partition_id
                        )]
                    )

                    partition_sizes.extend(_partition_sizes)
                    deserialization_times.extend(_deserialization_times)
                    read_times.extend(_read_times)

                    memory_results.extend(mem_res)
                    sm_data_opened.extend(shmem_op)

                elif event.storage_backend == IOBackend.QUEUE.value:
                    # If partition is ready and is saved in Queue, the PM
                    # sends the data direcly.
                    serialized_data = event.serialized_data
                    partition_id = event.partition_id

                    serialized_partition_data = serialized_data[partition_id]
                    partition_size = len(serialized_partition_data)
                    btime = time.time()
                    p_data = deserialize(serialized_partition_data)
                    atime = time.time()

                    partition_sizes.append(partition_size)
                    deserialization_times.append(atime - btime)
                    read_times.append(0.0)
                    queue_results.append(p_data)

                elif event.storage_backend == IOBackend.DISK.value:
                    (
                        _disk_results,
                        keys_not_f,
                        _partition_sizes,
                        _deserialization_times,
                        _read_times,
                    ) = await self.disk_client.get_partitions(
                        [(
                            event.exchange_id,
                            event.key,
                            event.partition_id
                        )]
                    )

                    disk_results.extend(_disk_results)
                    partition_sizes.extend(_partition_sizes)
                    deserialization_times.extend(_deserialization_times)
                    read_times.extend(_read_times)

                elif event.storage_backend == IOBackend.EXTERNAL.value:
                    (
                        _external_results,
                        keys_not_f,
                        _partition_sizes,
                        _deserialization_times,
                        _read_times,
                    ) = await self.external_client.get_partitions(
                        [(
                            event.exchange_id,
                            event.key,
                            event.partition_id
                        )],
                        fast=False
                    )
                    external_results.extend(_external_results)
                    partition_sizes.extend(_partition_sizes)
                    deserialization_times.extend(_deserialization_times)
                    read_times.extend(_read_times)

                print(
                    f"Task ID {task_id} correctly fetch ",
                    f"partition: {event.partition_id}",
                    f"from {event.storage_backend}"
                )

                return p_i

            async def launch_fetch_coroutines():

                tasks = [
                    asyncio.create_task(fetch_partition(i))
                    for i in range(len(keys_not_found))
                ]
                await asyncio.gather(*tasks)

            asyncio.run(launch_fetch_coroutines())

        # Aggregate results
        dataframes = (
            memory_results +
            queue_results +
            disk_results +
            external_results
        )

        del memory_results

        result = IOResult(
            data=concat_progressive(dataframes),
            partition_sizes=np.array(
                partition_sizes,
                dtype=np.int64
            ),
            ser_times=np.array(
                deserialization_times,
                dtype=np.float32
            ),
            io_times=np.array(
                read_times,
                dtype=np.float32
            ),
            shmems=sm_data_opened,
            keys=[ekp[1] for ekp in exchange_key_partition]
        )

        return result

    def write_from_localhost_to_cloud(
        self,
        key: str
    ) -> str:
        """
        Writes data from localhost (shmem, queues or disk) to cloud storage.

        Args:
        - key: Key for the data.
        - storage: Storage backend type.
        """
        consolidated_serialized_partitions = b""

        if self.io_backend == IOBackend.MEMORY:
            consolidated_serialized_partitions, metadata = \
                self.memory_client.get_partitions_grouped(key)
        elif self.io_backend == IOBackend.QUEUE:
            consolidated_serialized_partitions, metadata, key = \
                self.queue_client.get_partitions_grouped(key)
        elif self.io_backend == IOBackend.DISK:
            consolidated_serialized_partitions, metadata = \
                self.disk_client.get_partitions_grouped(key)
        elif self.io_backend == IOBackend.EXTERNAL:
            return key

        try:
            self.external_client.put_partition(
                partitions=consolidated_serialized_partitions,
                key=key,
                metadata=metadata
            )
        except Exception as e:
            self.logger.info(f"write_from_localhost_to_cloud (error: {e})")

        return key

    def put_partitions_shmem(
        self,
        data: _DataFrame,
        partitions: np.ndarray,
        partition_info: PartitionInfo
    ) -> IOResult:

        """
        Writes partitions to shared memory.

        Args:
        - data: Dataframe to be written.
        - partitions: Array of partitions.
        - partition_info: Information about the partitions.

        Returns:
        - Tuple containing key, write size, storage type, and metadata.
        """

        source_tasks = partition_info.source_tasks
        num_destinations = partition_info.num_partitions//source_tasks
        destination_partitions = [
            pid + (num_destinations * partition_info.partition_ids[0])
            for pid in range(num_destinations)
        ]

        serialized_partitions, partition_sizes, serialization_times = \
            serialize_partitions_memory(
                num_destinations,
                data,
                partitions,
                destination_partitions
            )

        btime = time.time()
        sm_data, sm_meta, key, write_size = self.memory_client.put_partition(
            serialized_partitions,
            partition_info
        )
        atime = time.time()
        write_time = atime - btime

        self.shmem_opened.append(sm_data)
        print(f"({self.worker_id}) Added shared memory {sm_data.name}")
        self.shmem_opened.append(sm_meta)

        result = IOResult(
            data="",
            partition_sizes=np.array(
                partition_sizes,
                dtype=np.int64
            ),
            ser_times=np.array(
                serialization_times,
                dtype=np.int64
            ),
            io_times=np.array(
                [write_time],
                dtype=np.float32
            ),
            keys=[key]
        )

        return result

    def put_partitions_queue(
        self,
        data: _DataFrame,
        partitions: np.ndarray,
        partition_info: PartitionInfo
    ) -> IOResult:
        """
        Writes partitions to queue.

        Args:
        - data: Dataframe to be written.
        - partitions: Array of partitions.
        - partition_info: Information about the partitions.

        Returns:
        - Tuple containing key, write size, storage type, and
          serialized partitions.
        """

        source_tasks = partition_info.source_tasks
        num_destinations = partition_info.num_partitions // source_tasks
        destination_partitions = [
            pid + (num_destinations * partition_info.partition_ids[0])
            for pid in range(num_destinations)
        ]

        serialized_partitions, partition_sizes, serialization_times = \
            serialize_partitions(
                num_destinations,
                data,
                partitions,
                destination_partitions
            )

        btime = time.time()
        key = self.queue_client.put_partition(
            serialized_partitions,
            partition_info
        )
        atime = time.time()
        write_time = atime - btime

        result = IOResult(
            data=serialized_partitions,
            partition_sizes=np.array(
                partition_sizes,
                dtype=np.int64
            ),
            ser_times=np.array(
                serialization_times,
                dtype=np.int64
            ),
            io_times=np.array(
                [write_time],
                dtype=np.float32
            ),
            keys=[key]
        )

        return result

    def put_partitions_disk(
        self,
        data: _DataFrame,
        partitions: np.ndarray,
        partition_info: PartitionInfo
    ) -> IOResult:
        """
        Writes partitions to external storage.

        Args:
        - data: Dataframe to be written.
        - partitions: Array of partitions.
        - partition_info: Information about the partitions.

        Returns:
        - Tuple containing key, write size, storage type, and metadata.
        """
        source_tasks = partition_info.source_tasks
        num_destinations = partition_info.num_partitions//source_tasks
        destination_partitions = [
            pid + (num_destinations * partition_info.partition_ids[0])
            for pid in range(num_destinations)
        ]

        (
            serialized_partitions,
            partition_sizes,
            serialization_times
        ) = serialize_partitions(
                num_destinations,
                data,
                partitions,
                destination_partitions
            )

        time_write, key, write_size = self.disk_client.put_partition(
            serialized_partitions,
            partition_info
        )

        result = IOResult(
            data="",
            partition_sizes=np.array(
                partition_sizes,
                dtype=np.int64
            ),
            ser_times=np.array(
                serialization_times,
                dtype=np.int64
            ),
            io_times=np.array(
                [time_write],
                dtype=np.float32
            ),
            keys=[key]
        )

        return result

    def write_object(
        self,
        data: object,
        bucket: str,
        key: str
    ) -> IOResult:
        return self.external_client.write(
            data,
            bucket,
            key
        )

    def write_auxiliary(
        self,
        data: object,
        auxiliary_info: AuxiliaryInfo
    ) -> IOResult:
        io_result = self.external_client.write_aux(
            data,
            auxiliary_info
        )
        return io_result

    def put_partitions_external(
        self,
        data: _DataFrame,
        partitions: np.ndarray,
        partition_info: PartitionInfo
    ) -> IOResult:
        """
        Writes partitions to external storage.

        Args:
        - data: Dataframe to be written.
        - partitions: Array of partitions.
        - partition_info: Information about the partitions.

        Returns:
        - Tuple containing key, write size, storage type, and metadata.
        """
        source_tasks = partition_info.source_tasks
        num_destinations = partition_info.num_partitions//source_tasks
        destination_partitions = [
            pid + (num_destinations * partition_info.partition_ids[0])
            for pid in range(num_destinations)
        ]

        serialized_partitions, partition_sizes, serialization_times = \
            serialize_partitions(
                num_destinations,
                data,
                partitions,
                destination_partitions
            )

        time_write, key, write_size = self.external_client.put_partition(
            serialized_partitions,
            partition_info
        )

        result = IOResult(
            data="",
            partition_sizes=np.array(
                partition_sizes,
                dtype=np.int64
            ),
            ser_times=np.array(
                serialization_times,
                dtype=np.float64
            ),
            io_times=np.array(
                [time_write],
                dtype=np.float32
            ),
            keys=[key]
        )

        return result

    def put_partitions(
        self,
        data: _DataFrame,
        partitions: np.ndarray,
        partition_info: PartitionInfo
    ) -> IOResult:

        try:
            if self.io_backend == IOBackend.MEMORY:
                return self.put_partitions_shmem(
                    data,
                    partitions,
                    partition_info
                )
            elif self.io_backend == IOBackend.QUEUE:
                return self.put_partitions_queue(
                    data,
                    partitions,
                    partition_info
                )
            elif self.io_backend == IOBackend.DISK:
                return self.put_partitions_disk(
                    data,
                    partitions,
                    partition_info
                )
            elif self.io_backend == IOBackend.EXTERNAL:
                return self.put_partitions_external(
                    data,
                    partitions,
                    partition_info
                )

        except Exception as e:
            print(f"The partitions can't be saved in storage: {e}")
            raise e

    def scan_data(
        self,
        io_info: IOInfo
    ) -> IOResult:
        """
        Scans data from external storage.

        Args:
        - io_info: Information about the I/O operation.

        Returns:
        - DataFrame containing the scanned data.
        """
        try:
            return self.external_client.scan_data(io_info)
        except Exception as e:
            print("Error scanning partition %d"%(io_info.partition_id))
            raise e

    def write_dataframe(
        self,
        data: _DataFrame,
        io_info: IOInfo
    ) -> IOResult:
        """
        Writes data to external storage.

        Args:
        - data: Dataframe to be written.
        - io_info: Information about the I/O operation.
        """
        return self.external_client.write_dataframe(
            data,
            io_info
        )

    def delete_key_from_s3(self, key):
        self.external_client.delete_object(key)
        self.external_client.delete_object(key+META_SUFFIX)

    def delete_key_from_disk(self, key):
        self.disk_client.delete_object(key)
        self.disk_client.delete_object(key+META_SUFFIX)

    def unlink_shared_memory(self, shmem_name):
        """
        Unlinks shared memory created by the worker.

        Args:
        - shmem_name: Name of the shared memory.
        """
        shmem_names = []

        if self.shmem_opened:
            for shmem in self.shmem_opened:
                shmem_names.append(shmem.name)

            id = shmem_names.index(shmem_name)
            self.shmem_opened[id].unlink()
            self.shmem_opened.pop(id)
            shmem_names.pop(id)

            id_meta = shmem_names.index(shmem_name+META_SUFFIX)
            self.shmem_opened[id_meta].unlink()
            self.shmem_opened.pop(id_meta)

    def unlink_all_shared_memories(self):
        """
        Unlinks all shared memories created by the worker.
        """
        for sm in self.shmem_opened:
            sm.unlink()
        self.shmem_opened = []
