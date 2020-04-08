# -*- coding: utf-8 -*-

"""Console script for tile_processor."""
import logging
import sys

import click

from tile_processor import recorder, controller, worker


@click.group()
@click.option(
    '--verbose', '-v',
    count=True,
    help="Increase verbosity. You can increment the level by chaining the "
         "argument, eg. -vvv")
@click.option(
    '--quiet', '-q',
    count=True,
    help="Decrease verbosity.")
@click.option(
    '--monitor', '-m',
    type = int,
    default = None,
    help="Monitor the resource usage of the processes that work on the tiles. "
         "The log is saved as TSV into tile-resource-usage_<date>.tsv. "
         "The monitoring interval is passed in seconds."
)
@click.pass_context
def main(ctx, verbose, quiet, monitor):
    """Process data sets in tiles."""
    ctx.ensure_object(dict)
    ctx.obj['monitor_log'] = None
    ctx.obj['monitor_interval'] = 5
    if monitor:
        monitor_log = recorder.configure_ressource_logging()
        ctx.obj['monitor_log'] = monitor_log
        ctx.obj['monitor_interval'] = monitor
    verbosity = verbose - quiet
    recorder.configure_logging(verbosity)
    # For logging from the click commands
    ctx.obj['log'] = logging.getLogger(__name__)
    return 0


@click.command('run')
@click.argument('controller_key',
                type=click.Choice(controller.factory._controllers,
                                  case_sensitive=False))
@click.argument('worker_key',
                type=click.Choice(worker.factory._executors,
                                  case_sensitive=False))
@click.argument('configuration', type=click.File('r'))
@click.argument('tiles', type=str, nargs=-1)
@click.option('--threads', type=int, default=3,
              help="Max. number of worker instances to start, "
                   "each on a separate thread")
@click.pass_context
def run_cmd(ctx, controller_key, worker_key, configuration, tiles, threads):
    """Run a process on multiple threads."""
    logger = ctx.obj['log']
    logger.debug(f"Controller key: {controller_key}")
    logger.debug(f"Worker key: {worker_key}")
    ctrl = controller.factory.create(controller_key,
        configuration=configuration,
        threads=threads,
        monitor_log=ctx.obj['monitor_log'],
        monitor_interval=ctx.obj['monitor_interval']
    )
    ctrl.configure(
        tiles=list(tiles),
        processor_key='threadprocessor',
        worker_key=worker_key
    )
    ctrl.run()
    return 0


@click.command()
@click.argument('name', type=str)
@click.argument('path', type=click.Path(exists=True))
def register_schema(name, path):
    """Registers a schema for parsing configuration files.

    Having a schema is especially useful in case of complex configuration files,
    so the configuration is validated before starting the processing. The
    schema (as well as the configuration) must be YAML.
    """
    schema = controller.ConfigurationSchema()
    schema.register(name, path)
    return 0


@click.command()
def list_schemas():
    """Lists the registered configuration schemas."""
    schema = controller.ConfigurationSchema()
    click.echo("Registered schemas:")
    click.echo(schema.db)


@click.command()
@click.argument('name', type=str)
def remove_schema(name):
    """Removes a configuration schema from the database"""
    schema = controller.ConfigurationSchema()
    schema.remove(name)
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
    click.echo(f"Writing memory usage graph to '{mem_file}'")
    recorder.save_mem_plot(log_df, mem_file)
    click.echo(f"Writing CPU time graph to '{cpu_file}'")
    recorder.save_cpu_log(log_df, cpu_file)
    return 0


main.add_command(run_cmd)
main.add_command(register_schema)
main.add_command(list_schemas)
main.add_command(remove_schema)
main.add_command(plot_monitor_log)

if __name__ == "__main__":
    sys.exit(main(obj={}))  # pragma: no cover
