import io
from typing import List
import matplotlib.pyplot as plt

from ortzi.logging.utils import (
    COLOR_PALETTE,
    load_data
)

plt.switch_backend("Qt5Agg")


def plot_gantt(
    data: str | io.IOBase | dict,
    stage_ids: List[int]
):
    d = load_data(data)
    tasks = {}
    for task in d["tasks"]:
        stage_id = task["stage_id"]
        if stage_id in stage_ids:
            start_time = task["start_time"]
            read_time = task["input_end_time"]
            transform_time = task["manipulate_end_time"]
            partition_time = task["partition_end_time"]
            end_time = task["end_time"]
            task_id = task["task_id"]
            tasks[task_id] = [
                start_time,
                read_time,
                transform_time,
                partition_time,
                end_time
            ]
    min_time = min([times[0] for times in tasks.values()])
    for task_id, times in tasks.items():
        for i in range(len(times)):
            times[i] -= min_time

    fig, ax = plt.subplots(figsize=(10, 6))

    for i, (task_id, times) in enumerate(sorted(tasks.items())):
        yarg = (i - .25, 0.5)
        start_time = times[0]
        read_time = times[1]
        transform_time = times[2]
        partition_time = times[3]
        end_time = times[4]
        ax.broken_barh(
            [
                (start_time, read_time - start_time),
                (read_time, transform_time - read_time),
                (transform_time, partition_time-transform_time),
                (partition_time, end_time-partition_time)

            ],
            yarg,
            facecolors=COLOR_PALETTE[:5]
        )

    ax.set_yticks(range(len(tasks)))
    ax.set_yticklabels(sorted(tasks.keys()))
    ax.set_xlabel('Time  (s)')
    ax.set_ylabel('Task ID')
    # ax.set_title('Gantt Chart of Tasks')
    ax.legend()
    ax.legend(
        handles=[
            plt.Line2D([0], [0], color=COLOR_PALETTE[0], lw=4, label='Read Time'),
            plt.Line2D([0], [0], color=COLOR_PALETTE[1], lw=4, label='Manipulate Time'),
            plt.Line2D([0], [0], color=COLOR_PALETTE[2], lw=4, label='Partition Time'),
            plt.Line2D([0], [0], color=COLOR_PALETTE[3], lw=4, label='Write Time')
        ],
        loc='upper right'
    )

    plt.show()
