import asyncio
import os
import shutil
from typing import (
    Tuple,
    Union,
    List
)
import time

import aioboto3
from aiofile import async_open
import cloudpickle as pickle
from lithops.storage import Storage
from lithops.constants import LITHOPS_TEMP_DIR

from ortzi.config import (
    MAX_RETRIES_LOW,
    MAX_RETRIES_POLL,
    RETRY_WAIT_TIME
)
from ortzi.io_utils.info import IOMargins
from ortzi.io_utils.serialize import deserialize


def get_data_size(
    storage: Storage,
    bucket: str,
    path: str,
    retries: int = MAX_RETRIES_LOW,
    partition_id: int = None
) -> int:
    """
    Get the size of data in bytes.

    Args:
    - storage (Storage): Object for accessing the storage service.
    - bucket (str): Name of the storage bucket.
    - path (str): Path to the data in the bucket.

    Returns:
    - int: Size of the data in bytes.
    """
    attempt = 0
    while attempt < retries:
        try:
            return int(storage.head_object(bucket, path)['content-length'])
        except Exception:
            attempt += 1
            if attempt >= retries:
                raise
            time.sleep(0.5 * attempt)
            print(
                "Retrying to get data size from storage...",
                f"Partition {partition_id} Attempt {attempt}"
            )


def scan_partition(
    storage: Storage,
    bucket: str,
    key: str,
    partition_id: int,
    num_partitions: int,
    bound_extraction_margin: int = IOMargins.CSV.value,
    eol_char: bytes = b"\n",
    retries: int = MAX_RETRIES_LOW
) -> Tuple[memoryview, int]:

    """
    Scan a partition to determine the bytes of the partition and the range of
    bytes to read.

    Args:
    - storage (Storage): Object for accessing the storage service.
    - bucket (str): Name of the storage bucket.
    - key (str): Key identifying the data in the bucket.
    - partition_id (int): ID of the partition to scan.
    - num_partitions (int): Total number of partitions.

    Returns:
    - Bytes of the partition and the size read.
    """

    data_size = get_data_size(
        storage,
        bucket,
        key,
        retries=retries
    )
    partition_size = data_size // num_partitions

    lower_bound = partition_size * partition_id
    if partition_id == (num_partitions-1):
        upper_bound = data_size
        upper_bound_read = upper_bound
    else:
        upper_bound = partition_size * (partition_id + 1)
        upper_bound_read = upper_bound + bound_extraction_margin

    btime = time.time()
    attempt = 0
    while True:
        try:
            read_part = storage.get_object(
                bucket,
                key,
                extra_get_args={"Range": ''.join([
                        'bytes=', str(lower_bound), '-',
                        str(upper_bound_read)
                    ])
                }
            )
            break
        except Exception as e:
            attempt += 1
            if attempt >= retries:
                print(e)
                raise e
            time.sleep(RETRY_WAIT_TIME * attempt)
            print(
                "Retrying to scan from storage...",
                f"Partition {partition_id} Attempt {attempt}"
            )
    print("Read partition %d from storage"%(partition_id))
    partition = adjust_bounds(
        read_part,
        initial=(partition_id == 0),
        final=partition_id == (num_partitions-1),
        upper_start=upper_bound - lower_bound,
        delimiter=eol_char
    )
    atime = time.time()
    print("Partition %d length: %d"%(partition_id, len(partition)))

    read_time = atime - btime

    return partition, read_time


def adjust_bounds(
    part: Union[str, bytes],
    initial: bool,
    final: bool,
    upper_start: int,
    delimiter: str = b"\n"
) -> memoryview:
    """
    Adjusts the bounds of a partition to align with record boundaries.

    Args:
    - part (Union[str, bytes]): Partition data.
    - initial (bool)
    - final (bool)
    - upper_start (int)
    - delimiter (str)

    Returns:
    - memoryview: Adjusted partition data.
    """
    total_size = len(part)

    lower_bound = 0
    if not initial:
        while lower_bound < total_size:
            lower_bound += 1
            if part[lower_bound:lower_bound + 1] == delimiter:
                lower_bound += 1
                break

    upper_bound = upper_start

    if not final:
        while upper_bound < total_size:
            upper_bound += 1
            if part[upper_bound:upper_bound + 1] == delimiter:
                upper_bound += 1
                break  

    bm = memoryview(part)

    return bm[lower_bound:upper_bound]


def get_bytes_to_read(
    w: int,
    metadata: dict
) -> Tuple[int, int]:
    """
    Get the range of bytes to read for a partition.

    Args:
    - w (int): Partition ID.
    - metadata (dict): Metadata containing byte ranges for partitions.

    Returns:
    - Tuple[int, int]: Start and end bytes to read.
    """

    return metadata[w][0], metadata[w][1] - 1


async def reader_async_s3(
    s3,
    bucket,
    exchange_id,
    key,
    partition_id,
    fast
):
    """
    Asynchronous reader function for reading partition data from S3.

    Args:
    - s3: S3 client.
    - bucket (str): Name of the storage bucket.
    - exchange_id (int): ID of the data exchange.
    - key (str): Key identifying the data in the bucket.
    - partition_id (int): ID of the partition to read.
    - fast (bool): Indicates If a fast reading has to be done (few retries).

    Returns:
    - Tuple: Tuple containing exchange ID, key, partition data, and timings.
    """

    if fast:
        retries = 1
    else:
        retries = MAX_RETRIES_POLL

    interval = RETRY_WAIT_TIME
    data = None

    i = 0

    while True:
        try:
            await s3.head_object(Bucket=bucket, Key=key+"_meta")
            break

        except Exception:
            i += 1
            if i >= retries:
                print(f"Could not read metadata for partition {partition_id}")
                raise IOError()
            time.sleep(interval)
            interval = i * interval

    then_meta = time.time()
    response = await s3.get_object(Bucket=bucket, Key=key + "_meta")
    metadata = await response['Body'].read()
    after_meta = time.time()

    unserialized_metadata = pickle.loads(metadata)

    read_start, read_end = get_bytes_to_read(
        partition_id,
        unserialized_metadata
    )
    partition_size = read_end - read_start

    then = time.time()
    response = await s3.get_object(
        Bucket=bucket,
        Key=key,
        Range=f"bytes={read_start}-{read_end}"
    )
    data = await response['Body'].read()
    after = time.time()

    bdeser = time.time()
    data = deserialize(data)
    adeser = time.time()

    return (
        exchange_id,
        key,
        partition_id,
        data,
        after_meta - then_meta,
        partition_size,
        adeser - bdeser,
        after - then
    )


def read_file_local(filename: str):
    with open(filename, "rb") as f:
        return f.read()


async def read_file_local_async(
    filename: str,
    read_start: int = None,
    read_end: int = None
):
    async with async_open(filename, 'rb') as afp:
        if read_start is not None:
            afp.seek(read_start)

        if read_end is not None and read_start is not None:
            return await afp.read(read_end - read_start + 1)
        else:
            return await afp.read()


async def reader_local(
    bucket,
    exchange_id,
    key,
    partition_id,
    fast: bool = False
):
    """
    Local reader function for reading partition data from disk.

    Args:
    - client (Storage): Object for accessing the storage service (disk).
    - bucket (str): Name of the storage bucket.
    - exchange_id (int): ID of the data exchange.
    - key (str): Key identifying the data in the bucket.
    - partition_id (int): ID of the partition to read.
    - fast (bool): Indicates If a fast reading has to be done (few retries).

    Returns:
    - Tuple: Tuple containing exchange ID, key, partition data, and timings.
    """
    if fast:
        retries = 1
    else:
        retries = 5
    interval = 0.5

    i = 0

    metadata_key = key+"_meta"

    metadata_path = os.path.join(LITHOPS_TEMP_DIR, bucket, metadata_key)
    file_path = os.path.join(LITHOPS_TEMP_DIR, bucket, key)

    while True:
        try:
            for _ in range(10):
                # The partition can take time to be flushed on disk
                then_meta = time.time()
                metadata_serialized = await read_file_local_async(
                    metadata_path
                )
                after_meta = time.time()
                metadata = pickle.loads(metadata_serialized)
                if len(metadata_serialized) == 0:
                    break
                time.sleep(0.05)
            break

        except Exception:

            i += 1
            if i >= retries:
                return exchange_id, key, partition_id
            else:
                time.sleep(interval)
                interval += 0.5

    read_start, read_end = get_bytes_to_read(partition_id, metadata)
    partition_size = read_end - read_start

    partition_serialized = b""
    for _ in range(10):
        # The partition can take time to be flushed on disk
        then = time.time()
        partition_serialized = await read_file_local_async(
            file_path,
            int(read_start),
            int(read_end)
        )
        after = time.time()
        if len(partition_serialized) == (read_end-read_start+1):
            break
        time.sleep(0.05)

    bdeser = time.time()
    data = deserialize(partition_serialized)
    adeser = time.time()

    return (
        exchange_id,
        key,
        partition_id,
        data,
        after_meta - then_meta,
        partition_size,
        adeser - bdeser,
        after - then
    )


def reader_local_grouped(
    bucket,
    key
) -> Tuple[bytes, bytes]:
    try:

        metadata_path = os.path.join(LITHOPS_TEMP_DIR, bucket, key+"_meta")
        file_path = os.path.join(LITHOPS_TEMP_DIR, bucket, key)

        metadata_serialized = read_file_local(metadata_path)
        partitions_data = read_file_local(file_path)

        return partitions_data, metadata_serialized

    except Exception as e:
        print(f"Disk storage (reader_local_grouped) -> {e}")
        return None, None


async def _external_read_exchange_async(
    exchange_key_partition: List[Tuple[int, str, int]],
    bucket: str,
    storage: Storage,
    fast: bool
):
    """
    Asynchronous function for reading partition data from external storage.

    Args:
    - exchange_key_partition (List[Tuple[int, str, int]]): List of tuples
        containing exchange ID, key, and partition ID.
    - bucket (str): Name of the storage bucket.
    - storage (Storage): Object for accessing the storage service.
    - fast (bool): Indicates If a fast reading has to be done (few retries).

    Returns:
    - Tuple: Tuple containing partition data and keys not found.
    """

    storage_config = storage.config[storage.config["backend"]]

    session = aioboto3.Session()
    async with session.client(
        "s3",
        region_name=storage_config.get("region", None),
        endpoint_url=storage_config.get("endpoint", None),
        aws_access_key_id=storage_config["access_key_id"],
        aws_secret_access_key=storage_config["secret_access_key"],
        aws_session_token=storage_config.get("session_token", None)
    ) as s3:
        tasks = [
            reader_async_s3(
                s3,
                bucket,
                e_k_p[0],
                e_k_p[1],
                e_k_p[2],
                fast
            )
            for e_k_p in exchange_key_partition
        ]
        results = await asyncio.gather(*tasks)

    # Gather results by exchange id
    keys_not_found = []
    dataframes = []
    partition_sizes = []
    deserialization_times = []
    read_times = []

    for r in results:
        if len(r) > 3:
            dataframes.append(r[3])
            partition_sizes.append(r[5])
            read_times.append(r[4])
            deserialization_times.append(r[6])
        else:
            keys_not_found.append((r[0], r[1], r[2]))

    return (
        dataframes,
        keys_not_found,
        partition_sizes,
        deserialization_times,
        read_times
    )


async def _disk_read_exchange(
    exchange_key_partition: List[Tuple[int, str, int]],
    bucket: str,
    fast: bool
):
    dataframes = []
    partition_sizes = []
    deserialization_times = []
    read_times = []
    keys_not_found = []

    async def reader_local_wrapper(e_k_p):
        res = await reader_local(
            bucket,
            e_k_p[0],
            e_k_p[1],
            e_k_p[2],
            fast
        )
        return res

    async def launch_coroutines():
        tasks = [
            asyncio.create_task(reader_local_wrapper(ekp))
            for ekp in exchange_key_partition
        ]
        res = await asyncio.gather(*tasks)
        return res

    results = await launch_coroutines()

    for r in results:
        if len(r) > 3:
            dataframes.append(r[3])
            partition_sizes.append(r[5])
            read_times.append(r[4])
            deserialization_times.append(r[6])
        else:
            keys_not_found.append((r[0], r[1], r[2]))

    return (
        dataframes,
        keys_not_found,
        partition_sizes,
        deserialization_times,
        read_times
    )


def _disk_read_exchange_async_grouped(
    key: str,
    bucket: str,
) -> Tuple[bytes, bytes]:
    """
    Asynchronous function for reading partition data from local disk (Gets a
        dict of partition_id: data).

    Args:
    - exchange_key_partition (List[Tuple[int, str, int]]): List of tuples
        containing exchange ID, key, and partition ID.
    - bucket (str): Name of the storage bucket.
    - storage (Storage): Object for accessing the storage service.

    Returns:
    - Tuple: Dict containing partition_id and its data.
    """
    consolidated_partition_data, metadata = reader_local_grouped(
        bucket,
        key
    )

    return consolidated_partition_data, metadata


async def read_exchanges_external(
    exchange_key_partition: List[Tuple[int, str, int]],
    bucket: str,
    storage: Storage,
    fast: bool = False
):
    """
    Read partition data from external storage asynchronously.

    Args:
    - exchange_key_partition (List[Tuple[int, str, int]]): List of tuples
        containing exchange ID, key, and partition ID.
    - bucket (str): Name of the storage bucket.
    - storage (Storage): Object for accessing the storage service.
    - fast (bool): Indicates If a fast reading has to be done (few retries).

    Returns:
    - Tuple: Tuple containing partition data and keys not found.
    """
    if storage.config["backend"] == "localhost":
        return await _disk_read_exchange(
            exchange_key_partition,
            bucket,
            fast
        )

    else:
        return await _external_read_exchange_async(
            exchange_key_partition,
            bucket,
            storage,
            fast
        )


async def read_exchanges_disk(
    exchange_key_partition: List[Tuple[int, str, int]],
    bucket: str,
    fast: bool = False
):
    """
    Read partition data from local disk asynchronously.

    Args:
    - exchange_key_partition (List[Tuple[int, str, int]]): List of tuples
        containing exchange ID, key, and partition ID.
    - bucket (str): Name of the storage bucket.
    - storage (Storage): Object for accessing the storage service.
    - fast (bool): Indicates If a fast reading has to be done (few retries).

    Returns:
    - Tuple: Tuple containing partition data and keys not found.
    """

    return await _disk_read_exchange(exchange_key_partition, bucket, fast)


def read_exchanges_grouped_disk(
    key: str,
    bucket: str
) -> Tuple[bytes, bytes]:
    """
    Read partition data from local disk asynchronously.

    Args:
    - exchange_key_partition (List[Tuple[int, str, int]]): List of tuples
        containing exchange ID, key, and partition ID.
    - bucket (str): Name of the storage bucket.
    - storage (Storage): Object for accessing the storage service.

    Returns:
    - Tuple: Dict containing partition_id and its data.
    """

    return _disk_read_exchange_async_grouped(key, bucket)


def timed_put_local(
    bucket: str,
    key: str,
    body: bytes
):
    filename = os.path.join(LITHOPS_TEMP_DIR, bucket, key)
    then = time.time()
    with open(filename, "wb") as f:
        f.write(body)
    elapsed = time.time() - then
    return elapsed


def timed_put_external(
    storage: Storage,
    bucket: str,
    key: str,
    body: bytes,
    retries: int = MAX_RETRIES_LOW
):
    """
    Put object into storage and measure the time taken (used to save info to
        disk or external memory).

    Args:
    - storage (Storage): Object for accessing the storage service.
    - bucket (str): Name of the storage bucket.
    - key (str): Key identifying the object in the bucket.
    - body (bytes): Object to be stored.

    Returns:
    - float: Time taken to put the object into storage.
    """

    then = time.time()
    attempt = 0
    while attempt < retries:
        try:
            if storage.backend != "localhost":
                storage.put_object(bucket, key, body)
            else:
                put_object_patch(bucket, key, body)
            break
        except Exception:
            attempt += 1
            if attempt >= retries:
                raise
            time.sleep(RETRY_WAIT_TIME * attempt)
    elapsed = time.time() - then
    return elapsed


def put_object_patch(bucket_name, key, data):
    """
    Put an object in localhost filesystem.
    Override the object if the key already exists.
    :param key: key of the object.
    :param data: data of the object
    :type data: str/bytes
    :return: None
    """
    data_type = type(data)
    file_path = os.path.join(LITHOPS_TEMP_DIR, bucket_name, key)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    if data_type == bytes:
        with open(file_path, "wb") as f:
            f.write(data)
            f.flush()
    elif hasattr(data, 'read'):
        with open(file_path, "wb") as f:
            shutil.copyfileobj(data, f, 1024 * 1024)
    else:
        with open(file_path, "w") as f:
            f.write(data)
