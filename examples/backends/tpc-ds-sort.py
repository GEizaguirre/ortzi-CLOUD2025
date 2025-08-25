import os
import argparse
import sys
import shutil

from lithops.constants import LITHOPS_TEMP_DIR

import ortzi
from ortzi.backends.backend import BackendType
from ortzi.io_utils.info import IOBackend
from ortzi.backends.executor_config import get_initial_execution_config
from ortzi import DataFrame
from ortzi.physical.provisioning.resource_provisioner import ProvisionerMode
from ortzi.utils import setup_debugging

sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/../.."))
from examples.tpcds_schemas import web_returns_schema
from config import *

# Functional variables
BUCKET = "benchmark-objects"
AUX_BUCKET = "aux-bucket"
FNAME = "tpcds/1/web_returns.dat"
OUT_DIR = "sort_out"
SF = "1"


def prepare_data_localhost():
    # Define directory paths
    base_dir = os.path.join(LITHOPS_TEMP_DIR)
    bucket_dir = os.path.join(base_dir, BUCKET)
    tpcds_dir = os.path.join(bucket_dir, "tpcds")
    sf_dir = os.path.join(tpcds_dir, SF)

    # Ensure each directory exists before creating it
    for directory in [base_dir, bucket_dir, tpcds_dir, sf_dir]:
        if not os.path.exists(directory):
            os.mkdir(directory)

    data_dir = os.path.join(
        os.path.dirname(os.path.dirname(ortzi.__file__)), "data"
    )
    # Define source and destination paths
    source_dir = f"{data_dir}/tpcds/{SF}"
    dest_dir = sf_dir


    # Copy files if they exist
    src_path = os.path.join(source_dir, FNAME)
    dest_path = os.path.join(dest_dir, FNAME)

    if os.path.exists(src_path):
        shutil.copy(src_path, dest_path)
    else:
        print(f"Warning: {src_path} does not exist and was not copied.")

    # Remove all data in LITHOPS_TEMP_DIR/AUX_BUCKET
    aux_bucket_dir = os.path.join(base_dir, AUX_BUCKET)
    if os.path.exists(aux_bucket_dir):
        shutil.rmtree(aux_bucket_dir)
        print(f"Removed all data in {aux_bucket_dir}")


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
    print(f"  provisioner mode: {provisioner_mode}")
    print(f"  execution_backend: {execution_backend}")
    print(f"  internal_storage: {internal_storage}")
    print(f"  num_executors: {num_executors}")
    print(f"  num_workers: {num_workers}")

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
        log_dir=log_dir,
        eager_io=False
    )

    if execution_backend == BackendType.CLOUD_VM:
        scale_backend = BackendType.CLOUD_FUNCTION
    else:
        scale_backend = execution_backend

    df: DataFrame = DataFrame.from_csv(
        bucket=BUCKET,
        key=fname,
        fanout=stage0_fanout,
        has_header=False,
        separator="|",
        new_columns=web_returns_schema.keys(),
        parse_args={
            "dtypes": web_returns_schema
        },
        autoscale=autoscale,
    )

    df.set_scaleup(scale_backend)

    df.sort(
        by="wr_item_sk",
        fanout=stage1_fanout,
        autoscale=autoscale
    )

    df.write(
        BUCKET,
        OUT_DIR,
        execution_config,
        synchronous=True,
        provisioner_mode=provisioner_mode,
        debug=True
    )


def run_job(
    fname: str,
    provisioner_mode: ProvisionerMode,
    stage0_fanout: int,
    stage1_fanout: int,
    num_executors: int,
    num_workers: int,
    execution_backend: BackendType,
    internal_storage: IOBackend,
    autoscale: bool,
    log_dir: str
):

    if execution_backend == BackendType.CLOUD_FUNCTION:
        instance_id = None
        instance_ip = None
    if execution_backend == BackendType.CLOUD_VM:
        instance_id = EXECUTOR_INSTANCE_ID
        instance_ip = None
    else:
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
    parser.add_argument("--backend", type=str, required=True, choices=["CLOUD_FUNCTION", "CONTAINER", "MULTIPROCESSING","K8S"], help="Backend type for execution.")
    parser.add_argument("--provisioner-mode", type=str, required=True, choices=["PROACTIVE"], help="Resource provisioning mode.")
    parser.add_argument("--storage", type=str, required=True, choices=["EXTERNAL", "DISK", "MEMORY"], help="Internal storage backend.")
    parser.add_argument("--executors", type=int, required=False, default=0, help="Number of executors to use.")
    parser.add_argument("--workers", type=int, required=False, default=1, help="Number of workers per executor.")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debugging mode (for gRPC).")
    parser.add_argument("-s", "--autoscale", action="store_true", help="Autoscale resources.")
    parser.add_argument("-t", "--test", action="store_true", help="Run in test mode.")
    parser.add_argument("--log-dir", type=str, required=False, default=None, help="Directory to save logs.")

    args = parser.parse_args()

    backend_type = BackendType[args.backend]
    internal_storage = IOBackend[args.storage]
    provisioner_mode = ProvisionerMode[args.provisioner_mode]

    if args.debug:
        setup_debugging()

    if args.test:
        prepare_data_localhost()
    
    autoscale = args.autoscale

    run_job(
        FNAME,
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
