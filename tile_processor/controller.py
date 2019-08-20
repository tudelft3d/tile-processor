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

from tile_processor import processor, worker
from tile_processor import tileconfig
from tile_processor import db

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


class TemplateController:
    """A sample implementation of a Controller."""

    def __init__(self):
        self.cfg = {}
        self.processors = {}

    def configure(self,
                  threads,
                  monitor_log,
                  monitor_interval,
                  tiles,
                  processor_key,
                  configuration):
        """Configure the controller.

        :param threads:
        :param monitor_log:
        :param monitor_interval:
        :param tiles:
        :param processor_key:
        :param configuration:
        """
        template_worker = worker.factory.create('template')
        self.cfg = {
            'threads': threads,
            'monitor_log': monitor_log,
            'monitor_interval': monitor_interval,
            'worker': template_worker.execute,
            'tiles': tiles,
            'config': configuration
        }
        for part in ['part_A', 'part_B']:
            self.processors[
                processor.factory.create(processor_key, name=part)] = part
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


class TemplateDbController:
    """Controller for tiles that are stored in PostgreSQL."""

    def __init__(self,
                 configuration: TextIO,
                 threads: int,
                 monitor_log: logging.Logger,
                 monitor_interval: int):
        self.schema = ConfigurationSchema('templatedb')
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

    def configure(self,
                  tiles,
                  processor_key):
        """Configure the controller."""
        template_worker = worker.factory.create('templatedb')
        dbtiles = tileconfig.DbTiles(
            conn=db.Db(**self.cfg['config']['database']),
            index_schema=db.Schema(self.cfg['config']['features_index']),
            feature_schema=db.Schema(self.cfg['config']['features'])
        )
        dbtiles.configure(tiles=tiles)
        self.cfg['tiles'] = dbtiles
        self.cfg['worker'] = template_worker.execute
        for part in ['part_A']:
            self.processors[
                processor.factory.create(processor_key, name=part)] = part
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


class ThreedfierController:
    """Controller for 3dfier."""

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
        # cfg['config']['in'] = config.name
        # rootdir = os.path.dirname(config.name)
        # rest_dir = os.path.join(rootdir, "cfg_rest")
        # ahn2_dir = os.path.join(rootdir, "cfg_ahn2")
        # ahn3_dir = os.path.join(rootdir, "cfg_ahn3")
        # for d in [rest_dir, ahn2_dir, ahn3_dir]:
        #     if os.path.isdir(d):
        #         rmtree(d, ignore_errors=True, onerror=None)
        #     try:
        #         os.makedirs(d, exist_ok=False)
        #         log.debug("Created %s", d)
        #     except Exception as e:
        #         log.exception(e)
        # cfg['config']['out_rest'] = os.path.join(rest_dir, "bag3d_cfg_rest.yml")
        # cfg['config']['out_border_ahn2'] = os.path.join(
        #     ahn2_dir,
        #     "bag3d_cfg_border_ahn2.yml"
        # )
        # cfg['config']['out_border_ahn3'] = os.path.join(
        #     ahn3_dir,
        #     "bag3d_cfg_border_ahn3.yml"
        # )
        cfg['threads'] = int(threads)
        cfg['monitor_log'] = monitor_log
        cfg['monitor_interval'] = monitor_interval

        # # -- Get config file parameters
        # # database connection
        # cfg['database'] = cfg_stream['database']
        #
        # # 2D polygons
        # cfg['input_polygons'] = cfg_stream['input_polygons']
        # try:
        #     # in case user gave " " or "" for 'extent'
        #     if len(cfg_stream['input_polygons']['extent']) <= 1:
        #         extent_file = None
        #         log.debug('extent string has length <= 1')
        #     cfg['input_polygons']['extent_file'] = os.path.abspath(
        #         cfg_stream['input_polygons']['extent'])
        #     cfg['input_polygons']['tile_list'] = None
        # except (NameError, AttributeError, TypeError):
        #     tile_list = cfg_stream['input_polygons']['tile_list']
        #     assert isinstance(
        #         tile_list,
        #         list), "Please provide input for tile_list as a list: [...]"
        #     cfg['input_polygons']['tile_list'] = tile_list
        #     cfg['input_polygons']['extent_file'] = None
        # # 'user_schema' is used for the '_clip3dfy_' and '_union' views, thus
        # # only use 'user_schema' if 'extent' is provided
        # user_schema = cfg_stream['input_polygons']['user_schema']
        # if (user_schema is None) or (extent_file is None):
        #     log.debug("user_schema or extent is None")
        #     cfg['input_polygons']['user_schema'] = cfg['input_polygons'][
        #         'tile_schema']
        #
        # # AHN point cloud
        # cfg['input_elevation'] = cfg_stream['input_elevation']
        # cfg['input_elevation']['dataset_dir'] = add_abspath(
        #     cfg_stream['input_elevation']['dataset_dir'])
        #
        # # quality checks
        # if cfg_stream['quality']['ahn2_rast_dir']:
        #     os.makedirs(cfg_stream['quality']['ahn2_rast_dir'], exist_ok=True)
        # if cfg_stream['quality']['ahn3_rast_dir']:
        #     os.makedirs(cfg_stream['quality']['ahn3_rast_dir'], exist_ok=True)
        # cfg['quality'] = cfg_stream['quality']
        #
        # # partitioning of the 2D polygons
        # cfg['tile_index'] = cfg_stream['tile_index']
        #
        # # output control
        # cfg['output'] = cfg_stream['output']
        # cfg['output']['staging']['dir'] = os.path.abspath(
        #     cfg_stream['output']['staging']['dir'])
        # os.makedirs(cfg['output']['staging']['dir'], exist_ok=True)
        # cfg['output']['production']['dir'] = os.path.abspath(
        #     cfg_stream['output']['production']['dir'])
        # os.makedirs(cfg['output']['production']['dir'], exist_ok=True)
        #
        # # executables
        # cfg['path_3dfier'] = cfg_stream['path_3dfier']
        # cfg['path_lasinfo'] = cfg_stream['path_lasinfo']
        # log.info(f"Configured {self.__class__.__name__}")

        return cfg

    def configure(self, tiles, processor_key: str):
        """Configure the control logic."""
        # Configure the tiles
        # Configure the borders of the different AHN versions
        threedfier_worker = worker.factory.create('threedfier')
        self.cfg['worker'] = threedfier_worker.execute
        ahn_2 = tileconfig.DbTilesAHN(
            conn=db.Db(**self.cfg['config']['database']),
            index_schema=db.Schema(self.cfg['config']['elevation_index']),
            feature_schema=db.Schema(self.cfg['config']['features'])
        )
        ahn_2.configure(tiles=tiles, version=2)
        ahn_3 = tileconfig.DbTilesAHN(
            conn=db.Db(**self.cfg['config']['database']),
            index_schema=db.Schema(self.cfg['config']['elevation_index']),
            feature_schema=db.Schema(self.cfg['config']['features'])
        )
        ahn_3.configure(tiles=tiles, version=3)
        ahn_border = tileconfig.DbTilesAHN(
            conn=db.Db(**self.cfg['config']['database']),
            index_schema=db.Schema(self.cfg['config']['elevation_index']),
            feature_schema=db.Schema(self.cfg['config']['features'])
        )
        ahn_border.configure(tiles=tiles, on_border=True)
        parts = {
            'AHN2': ahn_2,
            'AHN3': ahn_3,
            'AHN_border': ahn_border
        }
        for part, ahntiles in parts.items():
            self.cfg['tiles'] = ahntiles
            self.processors[
                processor.factory.create(processor_key, name=part)] = part
        log.info(f"Configured {self.__class__.__name__}")

    def run(self):
        """Run the processors"""
        log.info(f"Running {self.__class__.__name__}")
        results = {}
        for proc in self.processors:
            proc.configure(**self.cfg)
            res = proc.process()
            results[proc.name] = res
        log.info(f"Done {self.__class__.__name__}. Failed: {results}")
        return results


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
factory.register_controller('template', TemplateController)
factory.register_controller('templatedb', TemplateDbController)
factory.register_controller('threedfier', ThreedfierController)
