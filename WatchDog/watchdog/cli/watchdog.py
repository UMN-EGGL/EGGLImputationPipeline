import sys
import click
import asyncio
import shutil
from watchdog.watcher import watcher

@click.command()
@click.option('--gt',type=click.Path(exists=True))
@click.option('--out-prefix')
@click.option('--ref', default=None)
@click.option('--window', default=0.05)
@click.option('--overlap', default=0.005)
@click.option('--nthreads', default=4)
@click.option('--timeout',default=30)
@click.option('--debug/--no-debug', default=False)
@click.option('--heap-size', default='50g')
def cli(
        gt,out_prefix,ref,window,overlap,
        nthreads,timeout,heap_size,debug
    ):
    """
        [ WD ] : WatchDog
    """
    # Check for bcftools
    if shutil.which('bcftools') is None:
        click.print("bcftools needs to be installed to use this program")
        sys.exit(1)

    if debug:
        from IPython.core import ultratb
        sys.excepthook = ultratb.FormattedTB(
            mode='Verbose', color_scheme='Linux', call_pdb=1
        )
    w = watcher(
        gt, 
        out_prefix,
        ref=ref,
        window_size=window,
        overlap=overlap,
        nthreads=nthreads,
        timeout=timeout,
        heap_size=heap_size
    )
    command_exit_code = asyncio.run(w.run())
    sys.exit(command_exit_code)
