import time
import subprocess
import shlex
import os
from typing import (
    Dict,
    List
)

import numpy as np
import psutil
import json
from lithops.storage import Storage

from ortzi.backends.executor_config import ExecutorConfig
from ortzi.backends.aws import cmd_remove_s3
from ortzi.backends.minio import cmd_remove_minio
from ortzi.io_utils.info import CollectInfo
from ortzi.io_utils.serialize import deserialize
from ortzi.physical.physical import PhysicalPlan
from ortzi.driver import (
    Driver,
    run_driver
)
from ortzi.logging.ui_log import get_logger
from ortzi.config import (
    LAST_LOGS_VARNAME,
    config
)
from ortzi.utils import set_config_value


class Client:

    def __init__(
        self,
        execution_config: ExecutorConfig,
        enhanced_logging: bool = False,
        storage: Storage = None,
        clean: bool = True,
        log_file: str = None
    ):
        self.execution_config = execution_config
        self.enhanced_logging = enhanced_logging
        self.driver: Driver = None
        self.clean = clean
        self.log_file = log_file
        if storage is None:
            self.storage = Storage()
        else:
            self.storage = storage

    def run(
        self,
        physical_plan: PhysicalPlan,
    ):

        config.set(enhanced_logging=self.enhanced_logging)
        logger = get_logger("client")

        self.driver = Driver(
            self.execution_config,
            physical_plan
        )

        try:
            exec_logs, data_collects = run_driver(self.driver)
            self.save_logs(exec_logs)

        except KeyboardInterrupt:
            logger.info("Keyboard interrumpt, killing child processes")
            parent_pid = os.getpid()
            parent = psutil.Process(parent_pid)
            # or parent.children() for recursive=False
            for child in parent.children(recursive=True):
                try:
                    child.kill()
                except psutil.NoSuchProcess:
                    pass

        print("Collecting data...")
        collect_data = self.collect(data_collects)
        if self.clean:
            print("Cleaning intermediates...")
            self.clean_intermediates()

        return collect_data

    def collect(
        self,
        collect_data: List[CollectInfo]
    ):

        data = [
            deserialize(
                self.storage.get_object(
                    desc.bucket, f"{desc.key}/{desc.partition_id}"
                )
            )
            for desc in collect_data
        ]

        if data and len(data) == 1:
            return data[0]

        return data

    def save_logs(
        self,
        logs: Dict
    ):

        log_file = f"ortzi_log_{time.strftime('%Y%m%d_%H%M%S')}.json"
        log_path = os.path.join(
            self.execution_config.log_dir,
            log_file
        )
        os.makedirs(
            os.path.dirname(log_path),
            exist_ok=True
        )

        print(f"Saving logs in {log_path}")
        logs_jsonizable = convert_numpy_arrays_to_lists(logs)
        with open(log_path, "w") as f:
            json.dump(
                logs_jsonizable,
                f,
                indent=2
            )
        set_config_value(
            LAST_LOGS_VARNAME,
            log_path
        )

    def clean_intermediates(self):

        bucket_name = self.execution_config.bucket
        external_backend = self.execution_config.get_external_storage_backend()

        if not bucket_name:
            print("Error: S3 bucket name is not defined in execution_config.")
            return

        print(external_backend)

        if external_backend == "aws_s3":
            command = cmd_remove_s3(bucket_name)
        elif external_backend == "minio":
            command = cmd_remove_minio(bucket_name)

        print(f"Attempting to empty bucket: {bucket_name}")
        print(f"Executing command: {command}")

        subcommands = command.split("&&")
        subcommands = [sub.strip() for sub in subcommands if sub.strip()]
        for subcommand in subcommands:
            print(f"Subcommand: {subcommand}")
            try:
                result = subprocess.run(
                    shlex.split(subcommand),
                    capture_output=True,
                    text=True,
                    check=True  # Raise an exception for non-zero exit codes
                )
                print("Bucket emptied successfully!")
                if result.stderr:
                    print("STDERR:\n", result.stderr)
            except subprocess.CalledProcessError as e:
                print(f"Error emptying bucket: {e}")
                print("STDOUT:\n", e.stdout)
                print("STDERR:\n", e.stderr)
            except Exception as e:
                print(f"An unexpected error occurred: {e}")


def convert_numpy_arrays_to_lists(data):

    if isinstance(data, dict):
        return {k: convert_numpy_arrays_to_lists(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_numpy_arrays_to_lists(item) for item in data]
    elif isinstance(data, np.ndarray):
        return data.tolist()
    return data
