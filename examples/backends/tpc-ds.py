# Update PATH with yasp library's installation directory
import os
import shutil
import argparse
from typing import Dict
import sys

from lithops.constants import LITHOPS_TEMP_DIR

import ortzi
from ortzi.backends.backend import BackendType
from ortzi.io_utils.info import IOBackend
from ortzi.backends.executor_config import get_initial_execution_config
from ortzi import (
    DataFrame,
    Agg,
    Ops,
    Transform,
    Condition,
    Merge,
    Cast,
    MergeSource,
    MergePattern,
    Reduce
)
from ortzi.physical.provisioning.resource_provisioner import ProvisionerMode
from ortzi.utils import is_equal_with_resolution, setup_debugging

sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/../.."))
from examples.tpcds_schemas import (
    web_sales_schema,
    web_returns_schema,
    web_site_schema,
    date_dim_schema,
    customer_address_schema
)
from config import *


SF = "1"
FANOUTS = {
    # This could be N
    "stage0": None,
    # This must be 1
    "stage1": 1,
    # This could be N
    "stage2": None,
    # This could be N
    "stage3": None,
    # This could be N
    "stage4": None,
    # This could be N
    "stage5": None,
    # This must be 1
    "stage6": 1
}

# Functional variables
BUCKET = "benchmark-objects"
AUX_BUCKET = "aux-bucket"
expected_result = {
    'unique_order_number_count': 36,
    'total_cost': 40565.10999999999,
    'total_profit': -6348.799999999999
}


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

    # List of files to copy
    files = [
        "web_sales.dat",
        "web_returns.dat",
        "date_dim.dat",
        "customer_address.dat",
        "web_site.dat"
    ]

    # Copy files if they exist
    for file in files:
        src_path = os.path.join(source_dir, file)
        dest_path = os.path.join(dest_dir, file)

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
    fanouts: Dict[str, int],
    provisioner_mode: ProvisionerMode,
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

    def flag_func(x):
        return x > 1

    def condition_func_web_sales(x, y):
        return (x == 1) | ((x == 0) & (y == 1))

    def condition_func_date_dim(x):
        return (
            (x >= Cast.to_datetime(1999, 4, 1)) &
            (x <= Cast.to_datetime(1999, 6, 1))
        )

    def condition_customer_address(x):
        return x == "IA"

    def condition_web_site(x):
        return x == "pri"

    ws_order_numbers: DataFrame = DataFrame.from_csv(
        bucket=BUCKET,
        key=f"tpcds/{SF}/web_sales.dat",
        fanout=fanouts["stage0"],
        has_header=False,
        separator="|",
        new_columns=web_sales_schema.keys(),
        parse_args={
            "dtypes": web_sales_schema
        },
        autoscale=autoscale
    ).select(
        ['ws_order_number', 'ws_warehouse_sk']
    ).groupby(
        "ws_order_number",
        [
            Agg.n_unique("ws_warehouse_sk", "unique_count"),
            Agg.first("ws_warehouse_sk", "unique_value")
        ],
        transforms=Ops.flag(
            flag_func,
            "unique_count",
            "unique_count_flag",
            bool_type=False
        ),
        aggs2=[],
        fanout=1,
        autoscale=autoscale
    ).map(
        Transform.max("unique_count_flag", "ws_order_number", "max_flag"),
    ).map(
        Transform.max("unique_value", "ws_order_number", "unique_vals"),
    ).filter(
        Condition.condition(
            ["max_flag", "unique_vals"], condition_func_web_sales
        )
    ).select(
        "ws_order_number"
    )

    ws_order_numbers.set_scaleup(scale_backend)

    web_sales: DataFrame = DataFrame.from_csv(
        bucket=BUCKET,
        key=f"tpcds/{SF}/web_sales.dat",
        fanout=fanouts["stage2"],
        has_header=False,
        separator="|",
        new_columns=web_sales_schema.keys(),
        parse_args={
            "dtypes": web_sales_schema
        },
        autoscale=autoscale
    ).select(
        list(web_sales_schema.keys())
    ).select(
        [
            'ws_order_number',
            'ws_ext_ship_cost',
            'ws_net_profit',
            'ws_ship_date_sk',
            'ws_ship_addr_sk',
            'ws_web_site_sk',
            'ws_warehouse_sk'
        ]
    )

    wr_order_numbers: DataFrame = DataFrame.from_csv(
        bucket=BUCKET,
        key=f"tpcds/{SF}/web_returns.dat",
        fanout=fanouts["stage3"],
        has_header=False,
        separator="|",
        new_columns=web_returns_schema.keys(),
        parse_args={
            "dtypes": web_returns_schema
        },
        autoscale=autoscale
    ).select(
        ["wr_order_number"]
    )

    web_sales2: DataFrame = DataFrame.from_csv(
        bucket=BUCKET,
        key=f"tpcds/{SF}/date_dim.dat",
        fanout=fanouts["stage4"],
        has_header=False,
        separator="|",
        new_columns=date_dim_schema.keys(),
        parse_args={
            "dtypes": date_dim_schema
        },
        partition=False,
        autoscale=autoscale
    ).select(
        ["d_date", "d_date_sk"]
    ).filter(
        Condition.condition(["d_date"], condition_func_date_dim)
    ).select(
        ["d_date_sk"]
    ).merge(
        {
            0: MergeSource(web_sales, "ws_ship_date_sk", MergePattern.PARTIAL),
            1: MergeSource(wr_order_numbers, "wr_order_number", MergePattern.ALL),
            2: MergeSource(ws_order_numbers, "ws_order_number", MergePattern.ALL)
        },
        [
            Merge.isin(2, "ws_order_number", "ws_order_number", 0),
            Merge.isin(1, "ws_order_number", "wr_order_number", 0),
        ],
        autoscale=autoscale
    ).switch(
        0
    ).merge(
        merge_ops=Merge.join(0, "ws_ship_date_sk", "d_date_sk")
    ).purgue(
        [0, 1, 2]
    )

    customer_address: DataFrame = DataFrame.from_csv(
        bucket=BUCKET,
        key=f"tpcds/{SF}/customer_address.dat",
        fanout=fanouts["stage5"],
        has_header=False,
        separator="|",
        new_columns=customer_address_schema.keys(),
        parse_args={
            "dtypes": customer_address_schema
        },
        autoscale=autoscale
    ).filter(
        Condition.condition(
            ["ca_state"], condition_customer_address
        )
    ).select(
        ["ca_address_sk"]
    )

    result = DataFrame.from_csv(
        bucket=BUCKET,
        key=f"tpcds/{SF}/web_site.dat",
        fanout=fanouts["stage6"],
        has_header=False,
        separator="|",
        new_columns=web_site_schema.keys(),
        parse_args={
            "dtypes": web_site_schema
        },
        autoscale=autoscale
    ).filter(
        Condition.condition(
            ["web_company_name"], condition_web_site
        )
    ).select(
        ["web_site_sk"]
    ).merge(
        {
            0: MergeSource(web_sales2, "ws_ship_addr_sk", MergePattern.PARTIAL),
            1: MergeSource(customer_address, "ca_address_sk", MergePattern.ALL)
        },
        [
            Merge.join(1, "ws_ship_addr_sk", "ca_address_sk", 0)
        ],
        autoscale=autoscale
    ).purgue(
        [1]
    ).switch(
        0
    ).drop(
        ["ws_ship_addr_sk"]
    ).merge(
        merge_ops=Merge.join(0, "ws_web_site_sk", "web_site_sk")
    ).select(
        ["ws_order_number", "ws_ext_ship_cost", "ws_net_profit"]
    ).gather().reduce(
        [
            Reduce.n_unique("ws_order_number", "unique_order_number_count"),
            Reduce.sum("ws_ext_ship_cost", "ship_cost_sum"),
            Reduce.sum("ws_net_profit", "net_profit_sum")
        ]
    ).collect(
        execution_config,
        provisioner_mode=provisioner_mode,
        debug=True
    )

    print(result)

def run_job(
    fanouts: dict,
    provisioner_mode: ProvisionerMode,
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
    elif (
        execution_backend == BackendType.CLOUD_VM or
        execution_backend == BackendType.K8S
    ):
        instance_id = EXECUTOR_INSTANCE_ID
        instance_ip = None
    else:
        instance_id = None
        instance_ip = "127.0.0.1"

    run(
        fanouts,
        provisioner_mode,
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
    parser.add_argument("--backend", type=str, required=True, choices=["CLOUD_FUNCTION", "CONTAINER", "MULTIPROCESSING", "K8S"], help="Backend type for execution.")
    parser.add_argument("--provisioner-mode", type=str, required=True, choices=["PROACTIVE"], help="Resource provisioning mode.")
    parser.add_argument("--storage", type=str, required=True, choices=["EXTERNAL", "DISK", "MEMORY"], help="Internal storage backend.")
    parser.add_argument("--executors", type=int, required=False, default=0, help="Number of executors to use.")
    parser.add_argument("--workers", type=int, required=False, default=1, help="Number of workers per executor.")
    parser.add_argument("-t", "--test", action="store_true", help="Run in test mode.")
    parser.add_argument("-s", "--autoscale", action="store_true", help="Autoscale resources.")
    parser.add_argument("-d", "--debug", action="store_true", help="Debug logs.")
    parser.add_argument("--log-dir", type=str, required=False, default=None, help="Directory to save logs.")

    args = parser.parse_args()

    backend_type = BackendType[args.backend]
    internal_storage = IOBackend[args.storage]
    provisioner_mode = ProvisionerMode[args.provisioner_mode]

    if args.test:
        prepare_data_localhost()

    if args.debug:
        setup_debugging()

    autoscale = args.autoscale

    run_job(
        FANOUTS,
        provisioner_mode,
        args.executors,
        args.workers,
        backend_type,
        internal_storage,
        autoscale,
        args.log_dir
    )
