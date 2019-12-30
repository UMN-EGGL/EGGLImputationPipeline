import sys
import click
import asyncio
from watchdog.watcher import watch_beagle

@click.command()
@click.option('--gt')
@click.option('--out-prefix')
@click.option('--window', default=0.05)
@click.option('--overlap', default=0.005)
@click.option('--nthreads', default=4)
@click.option('--timeout',default=10)
@click.option('--debug/--no-debug', default=False)
@click.option('--heap-size', default='50g')
def cli(
        gt,out_prefix,window,overlap,
        nthreads,timeout,heap_size,debug
    ):
    """
        [ WD ] : WatchDog
    """
    if debug:
        from IPython.core import ultratb
        sys.excepthook = ultratb.FormattedTB(
            mode='Verbose', color_scheme='Linux', call_pdb=1
        )

    command_exit_code = asyncio.run(
        watch_beagle(
            gt, 
            out_prefix,
            window=window,
            overlap=overlap,
            nthreads=nthreads,
            timeout=timeout,
            heap_size=heap_size
        )
    )
    sys.exit(command_exit_code)
