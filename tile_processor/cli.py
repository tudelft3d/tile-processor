# -*- coding: utf-8 -*-

"""Console script for tile_processor."""
import sys
import click

from tile_processor import processor
from tile_processor.worker import WorkerFactory, ThreedfierWorker, LoD10Worker

# The config dictionary contains all the values required to initialize each of the services.
config = {
    'cfg_3dfier': 'config for 3dfier',
    'cfg_lod10': 'config for the LoD1.0 reconstruction'
}
exe_factory = WorkerFactory()


@click.group()
def main(args=None):
    """Console script for tile_processor."""
    return 0


@click.command()
@click.argument('configuration', type=click.File('r'))
def run_3dfier(configuration):
    """Run 3dfier on a batch of tiles in parallel

    The 'configuration' argument is the path to the YAML configuration file that controls the batch processing.
    """
    tiles = ['tile_1', 'tile_2', 'tile_3', 'tile_4', 'tile_5']
    exe_factory.register_worker('3dfier', ThreedfierWorker)
    threedfier = exe_factory.create('3dfier')
    threadprocessor = processor.factory.create('threadprocessor')
    result = threadprocessor.process(threads=3, worker=threedfier.execute, tiles=tiles, **config)
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


main.add_command(run_3dfier)
main.add_command(run_lod10)

if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
