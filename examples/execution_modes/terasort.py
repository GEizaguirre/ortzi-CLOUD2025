# Update PATH with yasp library's installation directory
import os
import argparse

from lithops.constants import LITHOPS_TEMP_DIR
from lithops.storage import Storage
import polars as pl

from ortzi.backends.backend import BackendType
from ortzi.io_utils.info import IOBackend
from ortzi.backends.executor_config import get_initial_execution_config
from ortzi import DataFrame
from ortzi.io_utils.serialize import deserialize
from ortzi.physical.provisioning.resource_provisioner import ProvisionerMode
from ortzi.utils import setup_debugging

from config import *


# Functional variables
BUCKET = "benchmark-objects"
FNAME = "terasort-50m"
TEST_FNAME = "terasort-240m"
OUT_DIR = "sort_out"


def remove_output(
    bucket: str,
    key: str
):
    """
    Remove the output directory from the specified bucket.
    """
    storage = Storage()
    obj_metas = storage.list_objects(
        bucket=bucket,
        prefix=key
    )

    for obj_meta in obj_metas:
        storage.delete_object(
            bucket=bucket,
            key=obj_meta['Key']
        )

    aux_obj_metas = storage.list_objects(
        bucket=AUX_BUCKET
    )

    for aux_obj_meta in aux_obj_metas:
        storage.delete_object(
            bucket=AUX_BUCKET,
            key=aux_obj_meta['Key']
        )


def check_output(
    bucket: str,
    key: str
) -> bool:

    storage = Storage()

    obj_metas = storage.list_objects(
        bucket=bucket,
        prefix=key
    )
    keys = [obj_meta['Key'] for obj_meta in obj_metas]
    keys.sort(key=lambda x: int(x.split("/")[-1]))

    DF = []
    for key in keys:
        obj = storage.get_object(
            bucket=bucket,
            key=key
        )
        df = deserialize(obj)
        DF.append(df)

    DF = pl.concat(DF)
    is_monotonic = DF["0"].is_sorted()

    print(f"Output rows: {DF.shape[0]}")
    assert is_monotonic, "Output is not sorted correctly!"
    print("Output is sorted correctly!")


def prepare_storage():

    if os.path.exists(f"data/{TEST_FNAME}"):
        lithops_temp_dir = LITHOPS_TEMP_DIR
        if not os.path.exists(lithops_temp_dir):
            os.makedirs(lithops_temp_dir, exist_ok=True)

        bucket_path = os.path.join(lithops_temp_dir, BUCKET)
        if not os.path.exists(bucket_path):
            os.makedirs(bucket_path, exist_ok=True)

        src_path = f"data/{TEST_FNAME}"
        dst_path = os.path.join(bucket_path, TEST_FNAME)
        with open(src_path, 'rb') as src:
            with open(dst_path, 'wb') as dst:
                dst.write(src.read())


def run(
    fname: str,
    provisioner_mode: ProvisionerMode,
    stage0_fanout: int,
    stage1_fanout: int,
    execution_backend: BackendType,
    internal_storage: IOBackend,
    num_executors: int,
    num_workers: int,
    runtime: str,
    instance_id: str,
    instance_ip: str,
    autoscale: bool = True,
    log_dir: str = None
):

    print("Running job with the following parameters:")
    print(f"  provisioner_mode: {provisioner_mode}")
    print(f"  fanout_mult1: {stage0_fanout}")
    print(f"  fanout_mult2: {stage1_fanout}")
    print(f"  execution_backend: {execution_backend}")
    print(f"  internal_storage: {internal_storage}")
    print(f"  num_executors: {num_executors}")
    print(f"  num_workers: {num_workers}")
    print(f"  autoscale: {autoscale}")

    execution_config = get_initial_execution_config(
        bucket=AUX_BUCKET,
        driver_ip=DRIVER_IP,
        ssh_key_name=SSH_KEY_NAME,
        execution_backend=execution_backend,
        internal_storage=internal_storage,
        runtime=runtime,
        instance_id=instance_id,
        instance_ip=instance_ip,
        num_executors=num_executors,
        default_num_workers=num_workers,
        log_dir=log_dir
    )

    if execution_backend == BackendType.CLOUD_VM:
        scale_backend = BackendType.CLOUD_FUNCTION
    else:
        scale_backend = execution_backend

    fname = os.path.basename(fname)

    df: DataFrame = DataFrame.from_teragen(
        bucket=BUCKET,
        key=fname,
        fanout=stage0_fanout,
        autoscale=autoscale
    )

    df.set_scaleup(scale_backend)

    df.sort(
        by="0",
        fanout=stage1_fanout,
        autoscale=autoscale
    )

    df.write(
        BUCKET,
        OUT_DIR,
        execution_config,
        synchronous=True,
        provisioner_mode=provisioner_mode,
        debug=True,
        pipeline_name="terasort"
    )


def run_job(
    fname: str,
    provisioner_mode: ProvisionerMode,
    stage0_fanout: int,
    stage1_fanout: int,
    executors: int,
    workers: int,
    execution_backend: BackendType,
    internal_storage: IOBackend,
    autoscale: bool,
    log_dir: str = None
):

    if execution_backend == BackendType.CLOUD_FUNCTION:
        num_executors = executors
        num_workers = 1
        instance_id = None
        instance_ip = None
    elif (
        execution_backend == BackendType.CLOUD_VM or
        execution_backend == BackendType.K8S
    ):
        num_executors = 1
        num_workers = workers
        instance_id = EXECUTOR_INSTANCE_ID
        instance_ip = None
    else:
        num_executors = executors
        num_workers = workers
        instance_id = None
        instance_ip = "127.0.0.1"

    run(
        fname,
        provisioner_mode,
        stage0_fanout,
        stage1_fanout,
        execution_backend,
        internal_storage,
        num_executors,
        num_workers,
        RUNTIMES[execution_backend],
        instance_id,
        instance_ip,
        autoscale,
        log_dir
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run terasort job with specified parameters.")
    parser.add_argument("--fanout1", type=int, required=False, default=None, help="Fanout value for first stage.")
    parser.add_argument("--fanout2", type=int, required=False, default=None, help="Fanout value for second stage.")
    parser.add_argument("--backend", type=str, required=True, choices=["CLOUD_FUNCTION", "CONTAINER", "MULTIPROCESSING", "K8S"], help="Backend type for execution.")
    parser.add_argument("--provisioner-mode", type=str, required=True, choices=["PROACTIVE"], help="Resource provisioning mode.")
    parser.add_argument("--storage", type=str, required=True, choices=["EXTERNAL", "DISK", "MEMORY"], help="Internal storage backend.")
    parser.add_argument("--executors", type=int, required=False, default=0, help="Number of executors to use.")
    parser.add_argument("--workers", type=int, required=False, default=1, help="Number of workers per executor.")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debugging mode (for gRPC).")
    parser.add_argument("-s", "--autoscale", action="store_true", help="Autoscale resources.")
    parser.add_argument("-t", "--test", action="store_true", help="Run in test mode.")
    parser.add_argument("--validate", action="store_true", help="Validate result.")
    parser.add_argument("--log-dir", type=str, required=False, default=None, help="Directory to save logs.")

    args = parser.parse_args()

    backend_type = BackendType[args.backend]
    internal_storage = IOBackend[args.storage]
    provisioner_mode = ProvisionerMode[args.provisioner_mode]

    if internal_storage == IOBackend.DISK:
        remove_disk_intermediates()

    if args.debug:
        setup_debugging()

    if args.test:
        prepare_storage()
        fname = TEST_FNAME
    else:
        fname = FNAME

    if args.validate:
        remove_output(
            bucket=BUCKET,
            key=OUT_DIR
        )

    autoscale = args.autoscale

    run_job(
        fname,
        provisioner_mode,
        args.fanout1,
        args.fanout2,
        args.executors,
        args.workers,
        backend_type,
        internal_storage,
        autoscale,
        args.log_dir
    )

    if args.validate:
        check_output(
            bucket=BUCKET,
            key=OUT_DIR
        )
