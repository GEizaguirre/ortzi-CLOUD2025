import json
import io
import os
from typing import List

from ortzi.config import MAX_PARALLELISM
from ortzi.backends.aws import (
    AWS_INSTANCES_COST,
    AWS_LAMBDA_COST_MB,
    AWS_S3_GET_COST,
    AWS_S3_PUT_COST
)


def load_data(data: str | io.IOBase | dict) -> dict:
    if isinstance(data, str):
        with open(data, 'r') as file:
            d = json.load(file)
    elif hasattr(data, 'read'):
        d = json.load(data)
    elif not isinstance(data, dict):
        raise ValueError("Input must be a filename, "
                         "file object, or dictionary")
    else:
        d = data
    return d


def get_elapsed_time(
    data: str | io.IOBase | dict,
    stage_0: int,
    stage_0_key: str,
    stage_1: int,
    stage_1_key: str
):
    d = load_data(data)

    start_times = []
    end_times = []
    for tasks in d["executor_logs"]:
        if not isinstance(tasks, list):
            _tasks = [tasks]
        else:
            _tasks = tasks
        for task in _tasks:
            stage_id = task["stage_id"]
            if stage_id == stage_0:
                start_times.append(task[stage_0_key])
            elif stage_id == stage_1:
                end_times.append(task[stage_1_key])
    start_time = min(start_times)
    end_time = max(end_times)

    return end_time - start_time


def get_exchange_time(
    data: str | io.IOBase | dict,
    stage_0: int,
    stage_1: int
):

    return get_elapsed_time(
        data,
        stage_0,
        "output_start_time",
        stage_1,
        "input_end_time"
    )


def get_execution_time_stages(
    data: str | io.IOBase | dict,
    stage_0: int,
    stage_1: int
):
    return get_elapsed_time(
        data,
        stage_0,
        "start_time",
        stage_1,
        "end_time"
    )


def get_total_time(data: str | io.IOBase | dict):
    d = load_data(data)
    start_time = d["start_time"]
    end_time = d["end_time"]
    return end_time - start_time


def get_execution_time(
    data: str | io.IOBase | dict,
    end_stage_id: int
):
    d = load_data(data)

    end_times = []
    for tasks in d["executor_logs"]:
        if not isinstance(tasks, list):
            _tasks = [tasks]
        else:
            _tasks = tasks
        for task in _tasks:
            stage_id = task["stage_id"]
            if stage_id == end_stage_id:
                end_times.append(task["end_time"])
    start_time = d["start_time"]
    end_time = max(end_times)

    return end_time - start_time


COLOR_PALETTE = [
    "#ea5545", "#f46a9b", "#ef9b20",
    "#edbf33", "#ede15b", "#bdcf32",
    "#87bc45", "#27aeef", "#b33dc6"
]


def get_task(
    data: str | io.IOBase | dict,
    stage_id: int,
    task_number: int
):

    d = load_data(data)
    tasks = d["executor_logs"]
    for t in tasks:
        base_id = stage_id * MAX_PARALLELISM
        if t["stage_id"] == stage_id:
            _task_number = t["task_id"] - base_id
            if _task_number == task_number:
                return t

    raise Exception("Task not found")


def get_computation_time(
    task_log: dict
):
    return (
        task_log["partition_end_time"] - task_log["manipulate_start_time"]
    )


def get_read_time(
    task_log: dict
):
    return (
        task_log["input_end_time"] - task_log["input_start_time"]
    )


def get_write_time(
    task_log: dict
):
    return (
        task_log["output_end_time"] - task_log["output_start_time"]
    )


def load_data_dir(
    dirname: str
) -> list[dict]:
    data = []
    for f in os.listdir(dirname):
        if not f.endswith(".json"):
            continue
        path = os.path.join(dirname, f)
        data.append(load_data(path))
    return data


def get_execution_cost(
    logs: dict,
    driver_instance_type: str = "t2.xlarge",
    final_stage: int = 1,
    runtime_cost: float = AWS_LAMBDA_COST_MB,
    read_request_cost: float = AWS_S3_GET_COST,
    write_request_cost: float = AWS_S3_PUT_COST,
    runtime_memory_mb: int = 1769
):
    total_task_runtime = 0
    functions_cost = 0
    read_requests = 0
    write_requests = 0

    for tasks in logs["executor_logs"]:
        for task in tasks:
            start_time = task["start_time"]
            end_time = task["end_time"]
            task_runtime = end_time - start_time
            total_task_runtime += task_runtime
            functions_cost += (
                task_runtime * runtime_cost * runtime_memory_mb
            )
            read_requests += len(task["read_sizes"])
            write_requests += len(task["write_times"])

    if final_stage is None:
        total_execution_time = get_elapsed_time(logs)
    else:
        total_execution_time = get_execution_time(
            logs,
            final_stage
        )
    driver_cost = (
        AWS_INSTANCES_COST[driver_instance_type] * total_execution_time
        / 3600
    )

    read_cost = read_requests * read_request_cost
    write_cost = write_requests * write_request_cost
    return driver_cost + functions_cost + read_cost + write_cost


def get_stage_ids(data: dict):

    stage_ids = set()
    plan = data.get("plan", {})
    for d in plan:
        stage_ids.add(d.get("stage_id"))

    return sorted(stage_ids)


def get_stage_plan(
    data: dict,
    stage_id: int
) -> dict:
    plan = data.get("plan", {})
    for d in plan:
        if d.get("stage_id") == stage_id:
            return d
    return {}


def get_predecessors(
    data: dict,
    stage_ids: List[int]
) -> dict[int, list[int]]:

    source_exchange_ids = {
        sid: [] for sid in get_stage_ids(data)
    }
    sink_exchange_ids = {
        sid: None for sid in get_stage_ids(data)
    }
    get_predecessors = {
        sid: [] for sid in get_stage_ids(data)
    }
    for sid in stage_ids:
        stage_plan = get_stage_plan(data, sid)
        for i in stage_plan.get("input", {}).values():
            source_exchanges = i.get("source_exchange_ids", [])
            source_exchange_ids[sid].extend(source_exchanges)
        output = stage_plan.get("output", {})
        sink_exchange = output.get("exchange_id", None)
        sink_exchange_ids[sid] = sink_exchange

    for sid in stage_ids:
        source_exchanges = source_exchange_ids[sid]
        for source_exchange in source_exchanges:
            for other_sid in stage_ids:
                if other_sid == sid:
                    continue
                if sink_exchange_ids[other_sid] == source_exchange:
                    get_predecessors[sid].append(other_sid)

    return get_predecessors


def get_read_sizes(
    data: dict,
    stage_ids: list[int] | None = None
) -> dict[int, int]:

    read_sizes = {
        sid: 0 for sid in get_stage_ids(data)
    }

    for tasks in data.get("executor_logs", []):
        for task in tasks:
            stage_id = task["stage_id"]
            if stage_ids is not None and stage_id not in stage_ids:
                continue
            rs = task.get("read_sizes", [])
            read_sizes[stage_id] += sum(rs)

    return read_sizes


def get_write_sizes(
    data: dict,
    stage_ids: list[int] | None = None
) -> dict[int, int]:

    write_sizes = {
        sid: 0 for sid in get_stage_ids(data)
    }

    for tasks in data.get("executor_logs", []):
        for task in tasks:
            stage_id = task["stage_id"]
            if stage_ids is not None and stage_id not in stage_ids:
                continue
            ws = task.get("write_sizes", [])
            write_sizes[stage_id] += sum(ws)

    return write_sizes


def get_data_size_per_stage(
    data: dict,
    stage_ids: list[int] | None = None
) -> dict[int, int]:

    stage_ids = get_stage_ids(data)
    stage_predecessors = get_predecessors(
        data,
        stage_ids
    )
    write_sizes = get_write_sizes(
        data,
        stage_ids=stage_ids
    )
    stage_sizes = {}
    for sid in stage_ids:
        stage_sizes[sid] = 0
        predecessors = stage_predecessors.get(sid, [])
        for p in predecessors:
            stage_sizes[sid] += write_sizes.get(p, 0)

    return stage_sizes


def get_executor_summary(data: dict) -> dict:

    executor_infos = data.get("executor_infos", [])
    executor_summary = {}
    for executor in executor_infos:
        backend = executor.get("execution_backend", None)
        memory = executor.get("runtime_memory", 0)
        workers = executor.get("num_workers", 0)
        internal_io = executor.get("internal_io_backend", None)

        key = (backend, memory, workers, internal_io)
        if key not in executor_summary:
            executor_summary[key] = 0
        executor_summary[key] += 1

    return executor_summary


def get_tasks_per_stage(data: dict) -> dict:

    stage_ids = get_stage_ids(data)
    tasks = data["executor_logs"]
    stage_tasks = {
        sid: 0 for sid in stage_ids
    }
    for task in tasks:
        stage_id = task["stage_id"]
        stage_tasks[stage_id] += 1

    return stage_tasks
