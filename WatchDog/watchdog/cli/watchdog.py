import click
import asyncio
from watchdog.watcher import run_process

@click.command()
@click.argument('command', nargs=-1)
def cli(command):
    """
        [ WD ] : WatchDog
    """
    click.echo(f"[ WD ]: Executing the following command: {' '.join(command)}")
    command_exit_code = asyncio.run(run_process(command))
    return command_exit_code
