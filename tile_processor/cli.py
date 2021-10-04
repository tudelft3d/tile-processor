# -*- coding: utf-8 -*-

"""Console script for tile_processor."""
import logging
import sys
from time import time

import click

from tile_processor import recorder, controller, worker


@click.group()
@click.option(
    "--loglevel",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    default="INFO",
    help="Set the logging level.",
)
@click.option(
    "--monitor",
    "-m",
    type=int,
    default=None,
    help="Monitor the resource usage of the processes that work on the tiles. "
    "The log is saved as TSV into tile-resource-usage_<date>.tsv. "
    "The monitoring interval is passed in seconds.",
)
@click.pass_context
def main(ctx, loglevel, monitor):
    """Process data sets in tiles."""
    ctx.ensure_object(dict)
    ctx.obj["monitor_log"] = None
    ctx.obj["monitor_interval"] = 5
    if monitor:
        monitor_log = recorder.configure_ressource_logging()
        ctx.obj["monitor_log"] = monitor_log
        ctx.obj["monitor_interval"] = monitor
    recorder.configure_logging(loglevel)
    # For logging from the click commands
    ctx.obj["log"] = logging.getLogger(__name__)
    return 0


@click.command("run")
@click.argument(
    "controller_key",
    type=click.Choice(controller.factory._controllers, case_sensitive=False),
)
@click.argument(
    "worker_key",
    type=click.Choice(worker.factory._executors, case_sensitive=False),
)
@click.argument("configuration", type=click.File("r"))
@click.argument("tiles", type=str, nargs=-1)
@click.option(
    "--threads",
    type=int,
    default=3,
    help="Max. number of worker instances to start, "
    "each on a separate thread",
)
@click.option(
    "--restart",
    type=int,
    default=0,
    help="Nr. of times to restart the failed tiles. Default 0, means no restart.",
)
@click.pass_context
def run_cmd(
    ctx, controller_key, worker_key, configuration, tiles, threads, restart
):
    """Run a process on multiple threads."""
    logger = ctx.obj["log"]
    logger.debug(f"Controller key: {controller_key}")
    logger.debug(f"Worker key: {worker_key}")
    start = time()
    ctrl = controller.factory.create(
        controller_key,
        configuration=configuration,
        threads=threads,
        monitor_log=ctx.obj["monitor_log"],
        monitor_interval=ctx.obj["monitor_interval"],
    )
    ctrl.configure(
        tiles=list(tiles),
        processor_key="threadprocessor",
        worker_key=worker_key,
    )
    ctrl.run(restart=restart)
    finish = time()
    logger.info(f"Tile-processor completed in {(finish-start)/60} minutes")
    return 0


@click.command("export_tile_inputs")
@click.argument(
    "controller_key",
    type=click.Choice(controller.factory._controllers, case_sensitive=False),
)
@click.argument("configuration", type=click.File("r"))
@click.argument("tiles", type=str, nargs=-1)
@click.argument(
    "out_dir",
    type=click.Path(
        resolve_path=True, writable=True, exists=True, file_okay=False
    ),
)
@click.pass_context
def export_tile_inputs_cmd(ctx, controller_key, configuration, tiles, out_dir):
    """Export the input footprints and point cloud for a list of tiles.

    CONFIGURATION is the yaml configuration file that is used for the 'run' command.

    TILES is a list of tile IDs to export. Or the keyword 'all' to export all tiles.

    OUT_DIR is the path the the output directory.
    """
    worker_key = "TileExporter"
    logger = ctx.obj["log"]
    logger.debug(f"Controller key: {controller_key}")
    logger.debug(f"Worker key: {worker_key}")
    start = time()
    ctrl = controller.factory.create(
        controller_key,
        configuration=configuration,
        threads=1,
        monitor_log=ctx.obj["monitor_log"],
        monitor_interval=ctx.obj["monitor_interval"],
    )
    ctrl.configure(
        tiles=list(tiles),
        processor_key="threadprocessor",
        worker_key=worker_key,
    )
    ctrl.cfg["config"]["out_dir"] = out_dir
    ctrl.run()
    finish = time()
    logger.info(f"Tile-processor completed in {(finish-start)/60} minutes")
    return 0


@click.command()
@click.argument("name", type=str)
@click.argument("path", type=click.Path(exists=True))
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
@click.argument("name", type=str)
def remove_schema(name):
    """Removes a configuration schema from the database"""
    schema = controller.ConfigurationSchema()
    schema.remove(name)
    return 0


@click.command()
@click.argument("logfile", type=click.File("r"))
def plot_monitor_log(logfile):
    """Plot the data that the resource monitor recorded.

    The 'logfile' is the TSV-file that is output by the monitoring
    """
    log_df = recorder.parse_log(logfile)
    mem_file = "rss.pdf"
    cpu_file = "cpu.pdf"
    click.echo(f"Writing memory usage graph to '{mem_file}'")
    recorder.save_mem_plot(log_df, mem_file)
    click.echo(f"Writing CPU time graph to '{cpu_file}'")
    recorder.save_cpu_log(log_df, cpu_file)
    return 0


main.add_command(run_cmd)
main.add_command(export_tile_inputs_cmd)
main.add_command(register_schema)
main.add_command(list_schemas)
main.add_command(remove_schema)
main.add_command(plot_monitor_log)

if __name__ == "__main__":
    sys.exit(main(obj={}))  # pragma: no cover
