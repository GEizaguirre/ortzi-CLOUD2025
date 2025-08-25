# Update PATH with yasp library's installation directory
import os
import argparse
import sys

import numpy as np
import pandas as pd

from ortzi.api.dataframe import Agg, Merge, MergePattern, MergeSource
from ortzi.backends.aws import get_private_ip
from ortzi.backends.backend import BackendType
from ortzi.io_utils.info import IOBackend
from ortzi.backends.executor_config import get_initial_execution_config
from ortzi import (
    DataFrame
)
from ortzi.physical.provisioning.resource_provisioner import ProvisionerMode
from ortzi.utils import setup_debugging

sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/../../.."))
from config import *


BASE_NUM_FUNC = 5


def run(
    log_dir: str = None,
    backend: BackendType = BackendType.CLOUD_FUNCTION
):

    execution_config = get_initial_execution_config(
        driver_ip=get_private_ip(),
        bucket=AUX_BUCKET,
        execution_backend=backend,
        internal_storage=IOBackend.EXTERNAL,
        runtime=RUNTIMES[backend],
        num_executors=0,
        default_num_workers=1,
        log_dir=log_dir,
        eager_io=False
    )

    def map_func():
        df = pd.DataFrame({
            '0': np.random.randint(0, 101, size=10000),
            '1': np.random.uniform(0, 1, size=10000)
        })
        return df

    df1 = DataFrame.void().map(
        map_func,
        fanout=BASE_NUM_FUNC * 2,
        autoscale=True,
        runtime_memory=1024
    )

    df2 = DataFrame.void().map(
        map_func,
        fanout=BASE_NUM_FUNC * 3,
        autoscale=True,
        runtime_memory=512
    )

    result = df1.merge(
        {
            0: MergeSource(df2, "0", MergePattern.PARTIAL),
        },
        [
            Merge.join(0, "0", "0")
        ]
    ).purgue(
        [0]
    ).groupby(
        "0",
        [
            Agg.sum("1", "sum_1"),
        ],
        fanout=BASE_NUM_FUNC * 1,
        runtime_memory=2048
    ).collect(
        execution_config,
        provisioner_mode=ProvisionerMode.PROACTIVE,
        debug=False
    )

    print("Result:")
    print(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run with specified parameters."
    )
    parser.add_argument(
        "--backend",
        type=str,
        required=True,
        choices=["CLOUD_FUNCTION", "CONTAINER", "MULTIPROCESSING", "K8S"],
        help="Backend type for execution."
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debugging mode."
    )
    parser.add_argument(
        "--log-dir",
        type=str,
        required=False,
        default=None,
        help="Directory to save logs."
    )

    args = parser.parse_args()

    if args.debug:
        print("Setting debug")
        setup_debugging()

    backend_type = BackendType[args.backend]

    run(
        log_dir=args.log_dir,
        backend=backend_type
    )
