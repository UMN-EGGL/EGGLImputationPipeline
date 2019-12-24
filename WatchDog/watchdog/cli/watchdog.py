import sys
import click
import asyncio
from watchdog.watcher import watch_beagle

@click.command()
@click.option('--gt')
@click.option('--out')
@click.option('--window')
@click.option('--overlap')
@click.option('--nthreads')
def cli(gt,out,window,overlap,nthreads):
    """
        [ WD ] : WatchDog
    """
    command_exit_code = asyncio.run(
        watch_beagle(
            gt, 
            out,
            impute='true',
            window=window,
            overlap=overlap,
            nthreads=nthreads,
        )
    )
    sys.exit(command_exit_code)
