# -*- coding: utf-8 -*-

"""Console script for tile_processor."""
import sys
from os import path
from datetime import datetime
import logging
import yaml

import click

from tile_processor import processor
from tile_processor.worker import WorkerFactory, ThreedfierWorker, LoD10Worker
from tile_processor import recorder

# The config dictionary contains all the values required to initialize each of the services.
config = {
    'cfg_3dfier': 'config for 3dfier',
    'cfg_lod10': 'config for the LoD1.0 reconstruction'
}
exe_factory = WorkerFactory()


def configure_logging(verbosity):
    """Configures the general logging in the application"""
    log_level = max(10, 30 - 10 * verbosity)
    logging.basicConfig(
        stream=sys.stderr,
        level=log_level,
        format='%(levelname)s:%(name)s:%(funcName)s\t%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


@click.group()
@click.option(
    '--verbose', '-v',
    count=True,
    help="Increase verbosity. You can increment the level by chaining the argument, eg. -vvv")
@click.option(
    '--quiet', '-q',
    count=True,
    help="Decrease verbosity.")
@click.option(
    '--monitor', '-m',
    type = int,
    default = None,
    help='Monitor the resource usage of the processes that work on the tiles. The log is saved as TSV '
         'into tile-resource-usage_<date>.tsv. The monitoring interval is passed in seconds.'
)
@click.pass_context
def main(ctx, verbose, quiet, monitor):
    """Console script for tile_processor."""
    ctx.ensure_object(dict)
    ctx.obj['monitor_log'] = None
    ctx.obj['monitor_interval'] = 5
    if monitor:
        monitor_log, logname = recorder.configure_ressource_logging()
        ctx.obj['monitor_log'] = monitor_log
        ctx.obj['monitor_interval'] = monitor
        ctx.obj['monitor_logname'] = logname
    verbosity = verbose - quiet
    configure_logging(verbosity)
    ctx.obj['log'] = logging.getLogger(__name__)
    return 0


@click.command()
@click.argument('configuration', type=click.File('r'))
@click.option('--threads', type=int, default=3,
              help='Max. number of 3dfier instances to start, each on a separate thread')
@click.pass_context
def run_3dfier(ctx, configuration, threads):
    """Run 3dfier on a batch of tiles in parallel.

    The 'configuration' argument is the path to the YAML configuration file that controls the batch processing.
    """
    tiles = ['tile_1', 'tile_2', 'tile_3', 'tile_4', 'tile_5']
    exe_factory.register_worker('3dfier', ThreedfierWorker)
    threedfier = exe_factory.create('3dfier')
    threadprocessor = processor.factory.create('threadprocessor')
    log = ctx.obj['log']
    log.debug(f"threads: {threads}")
    result = threadprocessor.process(threads=threads,
                                     monitor_log=ctx.obj['monitor_log'],
                                     monitor_interval=ctx.obj['monitor_interval'],
                                     worker=threedfier.execute,
                                     tiles=tiles,
                                     **config)
    click.echo(list(result))
    return 0


@click.command()
def run_lod10():
    """Run the LoD1.0 building reconstruction"""
    exe_factory.register_worker('lod10', LoD10Worker)
    lod10 = exe_factory.create('lod10')
    multiprocessor = processor.factory.create('multiprocessor')
    multiprocessor.process()
    lod10.execute(**config)
    return 0


@click.command()
@click.argument('logfile', type=click.File('r'))
def plot_monitor_log(logfile):
    """Plot the data that the resource monitor recorded.

    The 'logfile' is the TSV-file that is output by the monitoring
    """
    log_df = recorder.parse_log(logfile)
    mem_file = 'rss.pdf'
    cpu_file = 'cpu.pdf'
    click.echo(f"Writing memory usage graph to {mem_file}")
    recorder.save_mem_plot(log_df, mem_file)
    click.echo(f"Writing CPU time graph to {cpu_file}")
    recorder.save_cpu_log(log_df, cpu_file)
    return 0


main.add_command(run_3dfier)
main.add_command(run_lod10)
main.add_command(plot_monitor_log)

if __name__ == "__main__":
    sys.exit(main(obj={}))  # pragma: no cover
