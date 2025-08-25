import click

from ortzi.config import LAST_LOGS_VARNAME
from ortzi.logging.utils import get_executor_summary, get_tasks_per_stage, load_data
from ortzi.utils import get_config_value


@click.group()
def cli():
    pass


@cli.command()
@click.argument('log_file', required=False, type=click.Path(exists=True))
def summarize_logs(log_file):

    if not log_file:
        log_file = get_config_value(
            LAST_LOGS_VARNAME
        )
        if log_file is None:
            raise click.UsageError(
                "No log file available. (there may be no successful runs)"
            )

    data = load_data(log_file)
    executor_summary = get_executor_summary(data)
    stage_tasks = get_tasks_per_stage(data)

    print("Executor Summary:")
    for key, count in executor_summary.items():
        backend, memory, workers, io_backend = key
        print(f"  Backend: {backend}, Memory: {memory}MB, Workers: {workers}, IO Backend: {io_backend} -> Count: {count}")

    print("\nTasks Per Stage:")
    for stage_id, num_tasks in stage_tasks.items():
        print(f"  Stage {stage_id}: {num_tasks} task(s)")