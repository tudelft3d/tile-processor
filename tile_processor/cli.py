# -*- coding: utf-8 -*-

"""Console script for tile_processor."""
import sys
import logging

import click

from tile_processor import recorder
from tile_processor import controller


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
    """Console script for tile_processor."""
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


@click.command()
@click.option('--threads', type=int, default=3,
              help="Max. number of worker instances to start, "
                   "each on a separate thread")
@click.pass_context
def run_template(ctx, threads):
    """Run a template process on a batch of tiles in parallel.

    This command (incl. function calls within) is meant as template for
    implementing your own command. It will run several TemplateWorker-s, that
    execute the `./src/simlate_memory_use.sh` script, which allocates a constant
    amount of RAM (~600Mb) and 'holds' it for 10s, simulating an executable that
    consumes a larger amount of memory.

    The 'configuration' argument is the path to the YAML configuration file that
    controls the batch processing.
    """
    # This is how can you log from the cli commands in case you dont want to
    # use click.echo()
    log = ctx.obj['log']
    log.debug(f"Threads: {threads}")
    # Dummy data
    tiles = ['tile_1', 'tile_2', 'tile_3', 'tile_4', 'tile_5']
    configuration = {
        'cfg_3dfier': "config for 3dfier",
        'cfg_lod10': "config for the LoD1.0 reconstruction"
    }
    #
    template_controller = controller.factory.create('template')
    template_controller.configure(
        threads=threads,
        monitor_log=ctx.obj['monitor_log'],
        monitor_interval=ctx.obj['monitor_interval'],
        tiles=tiles,
        processor_key='threadprocessor',
        configuration=configuration
    )
    template_controller.run()
    return 0


@click.command()
@click.argument('configuration', type=click.File('r'))
@click.option('--threads', type=int, default=3,
              help="Max. number of worker instances to start, "
                   "each on a separate thread")
@click.pass_context
def run_3dfier(ctx, configuration, threads):
    """Run 3dfier"""
    threedfier_controller = controller.factory.create('threedfier',
        configuration=configuration,
        threads=threads,
        monitor_log=ctx.obj['monitor_log'],
        monitor_interval=ctx.obj['monitor_interval'],
    )
    threedfier_controller.configure(processor_key='threadprocessor')
    threedfier_controller.run()


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


main.add_command(run_template)
main.add_command(run_3dfier)
main.add_command(register_schema)
main.add_command(list_schemas)
main.add_command(remove_schema)
main.add_command(plot_monitor_log)

if __name__ == "__main__":
    sys.exit(main(obj={}))  # pragma: no cover
