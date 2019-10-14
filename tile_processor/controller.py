# -*- coding: utf-8 -*-

"""Control the execution of several Processors. In some cases it is necessary
to run several Processors in a loop. When there is a large number of tiles and
a few subsets of these tiles need to be processed with different settings,
you might want to maximize the resource use of the Processors, so one can
finish as quickly as possible. Then probably won't be enough resource left
to run a second Processor in parallel. This is where a Controller comes in,
which can control the execution of the Processors."""

import json
import logging
import os
from shutil import copyfile, rmtree
from typing import TextIO, List

import pykwalify.core
import pykwalify.errors
import yaml
from click import echo, secho, exceptions

from tile_processor import processor, worker, tileconfig, db, output

log = logging.getLogger(__name__)
logging.getLogger("pykwalify").setLevel(logging.WARNING)


class ConfigurationSchema:
    """Schema for validating a configuration file.

    For registering and removing configuration schemas,
    see the `register-schema` and `remove-schema` commands.
    """

    def __init__(self, name=None):
        self.name = name
        self.dir = os.path.join(os.path.dirname(__file__), 'schemas')
        self.db_path = os.path.join(os.path.dirname(__file__), 'schemas',
                                    'schemas.json')
        self.db = self.fetch()
        self.schema = self.fetch(self.name) if self.name else None

    def fetch(self, name=None):
        """Load the schema database (schema.json) or a specific schema if
        name is provided."""
        if name is None:
            with open(self.db_path, 'r') as fp:
                return json.load(fp)
        else:
            with open(self.db_path, 'r') as fp:
                s = json.load(fp)
                try:
                    src = os.path.join(self.dir, s[name])
                except KeyError:
                    raise exceptions.NoSuchOption(
                        name,
                        message=f"The configuration schema '{name}' is not "
                                f"registered. Register it with the "
                                f"'register-schema' command."
                    )
            try:
                with open(src, 'r') as cfgp:
                    return yaml.load(cfgp, Loader=yaml.FullLoader)
            except FileNotFoundError:
                raise exceptions.ClickException(
                    message=f"The configuration schema '{name}' is registered, "
                            f"but the file {src} is not found.")

    def register(self, name, path):
        """Register a configuration schema in the schema database
        (schema.json)."""
        fname = os.path.basename(path)
        try:
            dst = os.path.join(self.dir, fname)
            copyfile(path, dst)
        except Exception as e:
            log.exception(e)
            raise
        self.db[name] = fname
        try:
            with open(self.db_path, 'w') as fp:
                json.dump(self.db, fp)
        except Exception as e:
            log.exception(e)
            raise
        echo(f"Registered the configuraton schema '{fname}' as '{name}'")

    def remove(self, name=None):
        """Remove a schema from the database."""
        if name is None:
            name = self.name
        try:
            fname = self.db[name]
            del self.db[name]
            with open(self.db_path, 'w') as fp:
                json.dump(self.db, fp)
        except KeyError:
            secho(
                f"Schema '{name}' not in the database, not removing anything",
                fg='yellow')
            return
        try:
            p = os.path.join(self.dir, fname)
            os.remove(p)
            echo(f"Removed the configuration schema '{name}'")
        except FileNotFoundError:
            secho(
                f"Schema file '{fname}' is not in {self.dir}, "
                f"not removing anything",
                fg='yellow')
            return

    def validate_configuration(self, config):
        """Validates a configuration file against the schema.

        Validation is done with `pykwalify
        <https://pykwalify.readthedocs.io/en/master/>`_.
        """
        if config is None:
            log.warning(f"config is None")
            return None
        cfg = yaml.load(config, Loader=yaml.FullLoader)
        if self.schema:
            try:
                c = pykwalify.core.Core(source_data=cfg,
                                        schema_data=self.schema)
                # return the validated configuration
                return c.validate(raise_exception=True)
            except pykwalify.errors.PyKwalifyException:
                log.exception("Configuration file is not valid")
                raise
        else:
            log.warning("There is no registered schema, skipping validation")
            return cfg


class ControllerFactory:
    """Registers and instantiates a Controller that launches the Processors."""

    def __init__(self):
        self._controllers = {}

    def register_controller(self, key, controller):
        """Register an controller for use.

        :param key: Name of the controller
        :param controller: Can be a function, a class, or an object that
            implements .__call__()
        """
        self._controllers[key] = controller

    def create(self, key, **kwargs):
        """Instantiate a Processor"""
        controller = self._controllers.get(key)
        if not controller:
            raise ValueError(key)
        return controller(**kwargs)


class ExampleController:
    """Controller for tiles that are stored in PostgreSQL."""

    def __init__(self,
                 configuration: TextIO,
                 threads: int,
                 monitor_log: logging.Logger,
                 monitor_interval: int):
        self.schema = ConfigurationSchema('example')
        self.cfg = self.parse_configuration(
            configuration, threads, monitor_log, monitor_interval
        )
        self.processors = {}

    def parse_configuration(self,
                            config: TextIO,
                            threads: int,
                            monitor_log: logging.Logger,
                            monitor_interval: int) -> dict:
        """Parse, validate and prepare the configuration file.

        :param monitor_log:
        :param monitor_interval:
        :param config: A text stream, containing the configuration
        :param threads: Number of threads
        :return: Configuration
        """
        cfg = {}
        try:
            cfg_stream = self.schema.validate_configuration(config)
            log.info(f"Configuration file {config.name} is valid")
        except Exception as e:
            log.exception(e)
            raise
        cfg['threads'] = int(threads)
        cfg['monitor_log'] = monitor_log
        cfg['monitor_interval'] = monitor_interval
        cfg['config'] = cfg_stream
        return cfg

    def configure(self, tiles, processor_key: str, worker_key: str):
        """Configure the controller."""
        worker_init = worker.factory.create(worker_key)
        self.cfg['worker'] = worker_init.execute

        if worker_key == 'Example':
            tilescfg = tileconfig.Tiles()
            tilescfg.configure(tiles=tiles)
            out_dir = output.DirOutput(self.cfg['config']['output']['dir'])
            # Set up logic
            parts = {
                'part_A': tilescfg,
                'part_B': tilescfg
            }
        else:
            # For the ExampleDb worker
            tilescfg = tileconfig.DbTiles(
                conn=db.Db(**self.cfg['config']['database']),
                index_schema=db.Schema(self.cfg['config']['features_index']),
                feature_schema=db.Schema(self.cfg['config']['features'])
            )
            tilescfg.configure(tiles=tiles)
            out_dir = output.DirOutput(self.cfg['config']['output']['dir'])
            # Set up logic
            parts = {
                'part_A': tilescfg,
            }

        for part, _tilescfg in parts.items():
            _tilescfg.output = output.DirOutput(out_dir.add(part))
            proc = processor.factory.create(
                processor_key, name=part, tiles=_tilescfg)
            self.processors[proc] = part
        log.info(f"Configured {self.__class__.__name__}")

    def run(self) -> dict:
        """Run the Controller

        :return: `(processor.name : [tile ID])`
            Returns the tile IDs per Processor that failed even after
            restarts
        """
        log.info(f"Running {self.__class__.__name__}")
        results = {}
        for proc in self.processors:
            proc.configure(**self.cfg)
            res = proc.process()
            results[proc.name] = res
        log.info(f"Done {self.__class__.__name__}. Failed: {results}")
        return results


class AHNController:
    """Controller for AHN."""

    def __init__(self,
                 configuration: TextIO,
                 threads: int,
                 monitor_log: logging.Logger,
                 monitor_interval: int):
        self.schema = ConfigurationSchema('threedfier')
        self.cfg = self.parse_configuration(
            configuration, threads, monitor_log, monitor_interval
        )
        self.processors = {}

    def parse_configuration(self,
                            config: TextIO,
                            threads: int,
                            monitor_log: logging.Logger,
                            monitor_interval: int) -> dict:
        """Parse, validate and prepare the configuration file.

        :param monitor_log:
        :param monitor_interval:
        :param config: A text stream, containing the configuration
        :param threads: Number of threads
        :return: Configuration
        """
        cfg = {}
        try:
            cfg_stream = self.schema.validate_configuration(config)
            log.info(f"Configuration file {config.name} is valid")
        except Exception as e:
            log.exception(e)
            raise

        cfg['config'] = cfg_stream
        directory_mapping = {}
        for mapping in cfg_stream['elevation']['directories']:
            dir, properties = mapping.popitem()
            if not os.path.isabs(dir):
                raise ValueError(f"Path {dir} is not absolute in "
                                 f"elevation:directories")
            directory_mapping[dir] = properties
        cfg['config']['directory_mapping'] = directory_mapping

        cfg['threads'] = int(threads)
        cfg['monitor_log'] = monitor_log
        cfg['monitor_interval'] = monitor_interval

        return cfg

    def configure(self, tiles, processor_key: str, worker_key: str):
        """Configure the control logic."""
        worker_init = worker.factory.create(worker_key)
        self.cfg['worker'] = worker_init.execute

        # Configure the tiles
        # NOTE BD: Using the elevation_index as feature tile index works for
        # now, because the elevation tiles equal the feature tiles. However,
        # if that is not the case, then I'll need to partition the AHN tiles
        # as I do with the feature tiles below, and use the AHN tile partitions
        # to partition the feature tiles too.
        # FIXME: need a way to separately pass the feature_index, elevation_index and all the features/elevation to the tiles
        ahn_2 = tileconfig.DbTilesAHN(
            conn=db.Db(**self.cfg['config']['database']),
            index_schema=db.Schema(self.cfg['config']['elevation_index']),
            feature_schema=db.Schema(self.cfg['config']['features'])
        )
        ahn_2.configure(tiles=tiles, version=2,
                        directory_mapping=self.cfg['config']['directory_mapping'])

        ahn_3 = tileconfig.DbTilesAHN(
            conn=db.Db(**self.cfg['config']['database']),
            index_schema=db.Schema(self.cfg['config']['elevation_index']),
            feature_schema=db.Schema(self.cfg['config']['features'])
        )
        ahn_3.configure(tiles=tiles, version=3,
                        directory_mapping=self.cfg['config']['directory_mapping'])

        ahn_border = tileconfig.DbTilesAHN(
            conn=db.Db(**self.cfg['config']['database']),
            index_schema=db.Schema(self.cfg['config']['elevation_index']),
            feature_schema=db.Schema(self.cfg['config']['features'])
        )
        ahn_border.configure(tiles=tiles, on_border=True,
                             directory_mapping=self.cfg['config']['directory_mapping'])

        out_dir = output.DirOutput(self.cfg['config']['output']['dir'])
        # Set up logic
        parts = {
            'AHN2': ahn_2,
            'AHN3': ahn_3,
            'AHN_border': ahn_border
        }
        for part, ahntiles in parts.items():
            ahntiles.output = output.DirOutput(out_dir.add(part))
            proc = processor.factory.create(
                processor_key, name=part, tiles=ahntiles)
            self.processors[proc] = part
        log.info(f"Configured {self.__class__.__name__}")

    def run(self) -> dict:
        """Run the processors"""
        log.info(f"Running {self.__class__.__name__}")
        results = {}
        for proc in self.processors:
            proc.configure(**self.cfg)
            res = proc.process()
            results[proc.name] = res
        log.info(f"Done {self.__class__.__name__}. Failed: {results}")
        return results


class AHNTINController(AHNController):

    def configure(self, tiles, processor_key: str, worker_key: str):
        """Configure the control logic."""
        worker_init = worker.factory.create(worker_key)
        self.cfg['worker'] = worker_init.execute

        # Configure the tiles
        # NOTE BD: Ignore AHN versions for now, just testing

        ahn_3 = tileconfig.DbTilesAHN(
            conn=db.Db(**self.cfg['config']['database']),
            index_schema=db.Schema(self.cfg['config']['elevation_index']),
            feature_schema=db.Schema(self.cfg['config']['features'])
        )
        ahn_3.configure(tiles=tiles,
                        directory_mapping=self.cfg['config']['directory_mapping'])

        out_dir = output.DirOutput(self.cfg['config']['output']['dir'])
        # Set up logic
        parts = {
            'AHN3': ahn_3,
        }
        for part, ahntiles in parts.items():
            ahntiles.output = output.DirOutput(out_dir.add(part))
            proc = processor.factory.create(
                processor_key, name=part, tiles=ahntiles)
            self.processors[proc] = part
        log.info(f"Configured {self.__class__.__name__}")

def add_abspath(dirs: List):
    """Recursively append the absolute path to the paths in a nested list

    If not a list, returns the string with absolute path.
    """
    if isinstance(dirs, list):
        for i, elem in enumerate(dirs):
            if isinstance(elem, str):
                dirs[i] = os.path.abspath(elem)
            else:
                dirs[i] = add_abspath(elem)
        return dirs
    else:
        return os.path.abspath(dirs)


factory = ControllerFactory()
factory.register_controller('Example', ExampleController)
# factory.register_controller('ExampleDb', ExampleDbController)
factory.register_controller('AHN', AHNController)
factory.register_controller('AHNtin', AHNTINController)
