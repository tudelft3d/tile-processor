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
from shutil import copyfile
from typing import List
from io import TextIOBase

import yaml
from click import echo, secho, exceptions

from tile_processor import processor, worker, tileconfig, db, output

log = logging.getLogger(__name__)
# logging.getLogger("pykwalify").setLevel(logging.WARNING)


class ConfigurationSchema:
    """Schema for validating a configuration file.

    For registering and removing configuration schemas,
    see the `register-schema` and `remove-schema` commands.
    """

    def __init__(self, name=None):
        self.name = name
        self.dir = os.path.join(os.path.dirname(__file__), "schemas")
        self.db_path = os.path.join(
            os.path.dirname(__file__), "schemas", "schemas.json"
        )
        self.db = self.fetch()
        self.schema = self.fetch(self.name) if self.name else None

    def fetch(self, name=None):
        """Load the schema database (schema.json) or a specific schema if
        name is provided."""
        if name is None:
            with open(self.db_path, "r") as fp:
                return json.load(fp)
        else:
            with open(self.db_path, "r") as fp:
                s = json.load(fp)
                try:
                    src = os.path.join(self.dir, s[name])
                except KeyError:
                    secho(
                        message=f"The configuration schema '{name}' is not "
                        f"registered, but it is expected by the "
                        f"Controller. You can register the schema "
                        f"with the 'register-schema' command.",
                        fg="red",
                    )
                    return None
            try:
                with open(src, "r") as cfgp:
                    return yaml.load(cfgp, Loader=yaml.FullLoader)
            except FileNotFoundError:
                raise exceptions.ClickException(
                    message=f"The configuration schema '{name}' is registered, "
                    f"but the file {src} is not found."
                )

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
            with open(self.db_path, "w") as fp:
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
            with open(self.db_path, "w") as fp:
                json.dump(self.db, fp)
        except KeyError:
            secho(
                f"Schema '{name}' not in the database, not removing anything",
                fg="yellow",
            )
            return
        try:
            p = os.path.join(self.dir, fname)
            os.remove(p)
            echo(f"Removed the configuration schema '{name}'")
        except FileNotFoundError:
            secho(
                f"Schema file '{fname}' is not in {self.dir}, "
                f"not removing anything",
                fg="yellow",
            )
            return

    def validate_configuration(self, config):
        """Validates a configuration file against the schema.

        Validation is done with `pykwalify
        <https://pykwalify.readthedocs.io/en/master/>`_.
        """
        # FIXME: do something about the schemas, either remove completely or use
        #  another library for validation. But better remove completely
        if config is None:
            log.warning(f"config is None")
            return None
        cfg = yaml.load(config, Loader=yaml.FullLoader)
        # if self.schema:
        #     try:
        #         c = pykwalify.core.Core(
        #             source_data=cfg, schema_data=self.schema
        #         )
        #         # return the validated configuration
        #         return c.validate(raise_exception=True)
        #     except pykwalify.errors.PyKwalifyException:
        #         log.exception("Configuration file is not valid")
        #         raise
        # else:
        #     log.warning("There is no registered schema, skipping validation")
        #     return cfg
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


class Controller:
    def __init__(
        self,
        configuration: TextIOBase,
        threads: int = 1,
        monitor_log: logging.Logger = None,
        monitor_interval: int = None,
        config_schema: str = None,
    ):
        self.schema = ConfigurationSchema(config_schema)
        self.cfg = self.parse_configuration(
            configuration, threads, monitor_log, monitor_interval
        )
        self.processors = {}

    def parse_configuration(
        self,
        configuration: TextIOBase,
        threads: int,
        monitor_log: logging.Logger,
        monitor_interval: int,
    ) -> dict:
        """Parse, validate and prepare the configuration file.

        :param monitor_log: Logger for monitoring
        :param monitor_interval: Monitoring interval in seconds
        :param configuration: A text stream, containing the configuration
        :param threads: Number of threads
        :return: Configuration
        """
        cfg = {}
        if configuration is None:
            log.error("Configuration file is empty")
            return cfg
        else:
            if isinstance(configuration, TextIOBase):
                try:
                    cfg_stream = self.schema.validate_configuration(configuration)
                    log.info(f"Configuration file is valid")
                except Exception as e:
                    log.exception(e)
                    raise
            elif isinstance(configuration, dict):
                # Makea copy so we can work with frozendict-s from dagster
                cfg_stream = {**configuration}
            else:
                raise ValueError("configuration is neither TextIOBase nor a dictionary")
            cfg["threads"] = int(threads)
            cfg["monitor_log"] = monitor_log
            cfg["monitor_interval"] = monitor_interval
            cfg["config"] = cfg_stream
            return cfg

    def configure(self, tiles, processor_key: str, worker_key: str = None,
                  worker_class = None):
        """Configure the controller.

        Input-specific subclasses need to implement this.
        """
        if worker_key:
            worker_init = worker.factory.create(worker_key)
        else:
            worker_init = worker_class
        self.cfg["worker"] = worker_init.execute

        # Configure the tiles (DBTiles in this case)
        tilescfg = tileconfig.DbTiles(
            conn=db.Db(**self.cfg["config"]["database"]),
            tile_index_schema=db.Schema(self.cfg["config"]["features_tiles"]),
            features_schema=db.Schema(self.cfg["config"]["features"]),
        )
        tilescfg.configure(tiles=tiles)
        out_dir = output.DirOutput(self.cfg["config"]["output"]["dir"])
        # Set up logic for processing different parts. Parst are required
        # for example when processing a large area that needs different tile
        # configurations. For instance the Netherlands with AHN2 and AHN3.
        parts = {
            "part_A": tilescfg,
        }

        # Create a processor for each part
        for part, _tilescfg in parts.items():
            _tilescfg.output = output.Output(
                dir=output.DirOutput(out_dir.join_path(part))
            )
            proc = processor.factory.create(
                processor_key, name=part, tiles=_tilescfg
            )
            self.processors[proc] = part
        log.info(f"Configured {self.__class__.__name__}")

    def run(self, restart: int = 0) -> dict:
        """Run the Controller.

        :return: `(processor.name : [tile ID])`
            Returns the tile IDs per Processor that failed even after
            restarts
        """
        log.info(f"Running {self.__class__.__name__}")
        results = {}
        for proc in self.processors:
            proc.configure(**self.cfg)
            res = proc.process(restart=restart)
            results[proc.name] = res
        log.info(f"Done {self.__class__.__name__}. {results}")
        return results


class ExampleController(Controller):
    """Controller for tiles that are stored in PostgreSQL."""

    def configure(self, tiles, processor_key: str, worker_key: str = None,
                  worker_class = None):
        """Configure the controller."""
        if worker_key:
            worker_init = worker.factory.create(worker_key)
        else:
            worker_init = worker_class
        self.cfg["worker"] = worker_init.execute

        if worker_key == "Example":
            tilescfg = tileconfig.FileTiles()
            tilescfg.configure(tiles=tiles)
            out_dir = output.DirOutput(self.cfg["config"]["output"]["dir"])
            # Set up logic
            parts = {"part_A": tilescfg, "part_B": tilescfg}
        else:
            # For the ExampleDb worker
            tilescfg = tileconfig.DbTiles(
                conn=db.Db(**self.cfg["config"]["database"]),
                tile_index_schema=db.Schema(
                    self.cfg["config"]["features_tiles"]
                ),
                features_schema=db.Schema(self.cfg["config"]["features"]),
            )
            tilescfg.configure(tiles=tiles)
            out_dir = output.DirOutput(self.cfg["config"]["output"]["dir"])
            # Set up logic
            parts = {
                "part_A": tilescfg,
            }

        for part, _tilescfg in parts.items():
            _tilescfg.output = output.Output(
                dir=output.DirOutput(out_dir.join_path(part))
            )
            proc = processor.factory.create(
                processor_key, name=part, tiles=_tilescfg
            )
            self.processors[proc] = part
        log.info(f"Configured {self.__class__.__name__}")


class AHNController(Controller):
    """Controller for AHN when only one version of AHN need to be processed."""

    def parse_configuration(
        self,
        configuration: TextIOBase,
        threads: int,
        monitor_log: logging.Logger,
        monitor_interval: int,
    ) -> dict:
        """Parse, validate and prepare the configuration file.

        :param monitor_log:
        :param monitor_interval:
        :param config: A text stream, containing the configuration
        :param threads: Number of threads
        :return: Configuration
        """
        cfg = {}
        if configuration is None:
            log.error("Configuration is empty")
            return cfg
        else:
            if isinstance(configuration, TextIOBase):
                try:
                    cfg_stream = self.schema.validate_configuration(configuration)
                    log.info(f"Configuration file is valid")
                except Exception as e:
                    log.exception(e)
                    raise
            elif isinstance(configuration, dict):
                # Makea copy so we can work with frozendict-s from dagster
                cfg_stream = {**configuration}
            else:
                raise ValueError("configuration is neither TextIOBase nor a dictionary")

            cfg["config"] = cfg_stream
            directory_mapping = {}
            for mapping in cfg_stream["elevation"]["directories"]:
                dir, properties = {**mapping}.popitem()
                if not os.path.isabs(dir):
                    raise ValueError(
                        f"Path {dir} is not absolute in "
                        f"elevation:directories"
                    )
                directory_mapping[dir] = properties
            cfg["config"]["directory_mapping"] = directory_mapping

            cfg["threads"] = int(threads)
            cfg["monitor_log"] = monitor_log
            cfg["monitor_interval"] = monitor_interval
            return cfg

    def configure(
        self, tiles, processor_key: str, worker_key: str = None, worker_class = None, restart: int = 0
    ):
        """Configure the control logic."""
        if worker_key:
            worker_init = worker.factory.create(worker_key)
        else:
            worker_init = worker_class
        self.cfg["worker"] = worker_init.execute

        # Configure the tiles
        conn = db.Db(**self.cfg["config"]["database"])
        elevation_tiles = tileconfig.DbTiles(
            conn=conn,
            tile_index_schema=db.Schema(self.cfg["config"]["elevation_tiles"]),
        )
        feature_tiles = tileconfig.DbTiles(
            conn=conn,
            tile_index_schema=db.Schema(self.cfg["config"]["features_tiles"]),
            features_schema=db.Schema(self.cfg["config"]["features"]),
        )
        # Configure feature tiles with elevation from AHN3
        ahntiles = tileconfig.DbTilesAHN(
            conn=conn,
            elevation_tiles=elevation_tiles,
            feature_tiles=feature_tiles,
        )
        ahntiles.configure(
            tiles=tiles,
            version=None,
            directory_mapping=self.cfg["config"]["directory_mapping"],
            tin=False,
        )
        # Set up outputs
        output_obj = output.Output()
        if "database" in self.cfg["config"]["output"]:
            output_obj.db = output.DbOutput(
                conn=db.Db(**self.cfg["config"]["output"]["database"])
            )
        if "dir" in self.cfg["config"]["output"]:
            # FIXME: Output.dir should be a dict maybe by default?
            output_obj.dir = {}
            if isinstance(self.cfg["config"]["output"]["dir"], str):
                output_obj.dir["path"] = output.DirOutput(
                    path=self.cfg["config"]["output"]["dir"]
                )
            elif isinstance(self.cfg["config"]["output"]["dir"], dict):
                for k,dirpath in self.cfg["config"]["output"]["dir"].items():
                    output_obj.dir[k] = output.DirOutput(dirpath)
            else:
                raise ValueError(f'Expected str or dict in {self.cfg["config"]["output"]["dir"]}')
        for k, v in self.cfg["config"]["output"].items():
            if k != "database" and k != "dir":
                output_obj.kwargs[k] = v
        ahntiles.output = output_obj
        name = "part1"
        proc = processor.factory.create(
            processor_key, name=name, tiles=ahntiles
        )
        self.processors[proc] = name
        log.info(f"Configured {self.__class__.__name__}")


class AhnTinController(AHNController):
    """Controller for AHN when the AHN tile boundaries are the features themselves."""

    def configure(self, tiles, processor_key: str, worker_key: str):
        """Configure the control logic."""
        worker_init = worker.factory.create(worker_key)
        self.cfg["worker"] = worker_init.execute

        # Configure feature tiles with elevation from AHN3
        conn = db.Db(**self.cfg["config"]["database"])
        elevation_tiles = tileconfig.DbTiles(
            conn=conn,
            tile_index_schema=db.Schema(self.cfg["config"]["elevation_tiles"]),
        )
        elevation_tiles.configure(tiles=tiles)
        ahntiles = tileconfig.DbTilesAHN(
            conn=None, elevation_tiles=None, feature_tiles=None
        )
        ahntiles.to_process = elevation_tiles.to_process
        elevation_file_paths = ahntiles.create_elevation_file_index(
            directory_mapping=self.cfg["config"]["directory_mapping"]
        )
        ahn_version = 3
        ahntiles.elevation_file_index = {}
        for i, ahn_id in enumerate(elevation_tiles.to_process):
            paths = []
            if ahn_id in elevation_file_paths:
                paths.extend(
                    (p, ahn_version) for p in elevation_file_paths[ahn_id]
                )
            else:
                log.debug(
                    f"File matching the AHN ID {ahn_id} not found, skipping tile"
                )
                del ahntiles.to_process[i]
            ahntiles.elevation_file_index[ahn_id] = paths
        # Set up outputs
        output_obj = output.Output()
        if "database" in self.cfg["config"]["output"]:
            output_obj.db = output.DbOutput(
                conn=db.Db(**self.cfg["config"]["output"]["database"])
            )
        elif "dir" in self.cfg["config"]["output"]:
            output_obj.dir = output.DirOutput(
                path=self.cfg["config"]["output"]["dir"]
            )
        for k, v in self.cfg["config"]["output"].items():
            if k != "database" and k != "dir":
                output_obj.kwargs[k] = v
        ahntiles.output = output_obj
        name = "part1"
        proc = processor.factory.create(
            processor_key, name=name, tiles=ahntiles
        )
        self.processors[proc] = name
        log.info(f"Configured {self.__class__.__name__}")


class AHNBoundaryController(Controller):
    """Controller for AHN when two versions of AHN need to be processed."""

    def parse_configuration(
        self,
        configuration: TextIOBase,
        threads: int,
        monitor_log: logging.Logger,
        monitor_interval: int,
    ) -> dict:
        """Parse, validate and prepare the configuration file.

        :param monitor_log:
        :param monitor_interval:
        :param config: A text stream, containing the configuration
        :param threads: Number of threads
        :return: Configuration
        """
        cfg = {}
        if configuration is None:
            log.error("Configuration is empty")
            return cfg
        else:
            if isinstance(configuration, TextIOBase):
                try:
                    cfg_stream = self.schema.validate_configuration(configuration)
                    log.info(f"Configuration file is valid")
                except Exception as e:
                    log.exception(e)
                    raise
            elif isinstance(configuration, dict):
                # Makea copy so we can work with frozendict-s from dagster
                cfg_stream = {**configuration}
            else:
                raise ValueError("configuration is neither TextIOBase nor a dictionary")


            cfg["config"] = cfg_stream
            directory_mapping = {}
            for mapping in cfg_stream["elevation"]["directories"]:
                dir, properties = {**mapping}.popitem()
                if not os.path.isabs(dir):
                    raise ValueError(
                        f"Path {dir} is not absolute in "
                        f"elevation:directories"
                    )
                directory_mapping[dir] = properties
            cfg["config"]["directory_mapping"] = directory_mapping

            cfg["threads"] = int(threads)
            cfg["monitor_log"] = monitor_log
            cfg["monitor_interval"] = monitor_interval
            return cfg

    def configure(self, tiles, processor_key: str, worker_key: str):
        """Configure the control logic."""
        worker_init = worker.factory.create(worker_key)
        self.cfg["worker"] = worker_init.execute

        # Configure the tiles
        _tilecfg = {
            "conn": db.Db(**self.cfg["config"]["database"]),
            "elevation_index_schema": db.Schema(
                self.cfg["config"]["elevation_tiles"]
            ),
            "tile_index_schema": db.Schema(
                self.cfg["config"]["features_tiles"]
            ),
            "features_schema": db.Schema(self.cfg["config"]["features"]),
        }
        conn = db.Db(**self.cfg["config"]["database"])
        elevation_tiles = tileconfig.DbTiles(
            conn=conn,
            tile_index_schema=db.Schema(self.cfg["config"]["elevation_tiles"]),
        )
        feature_tiles = tileconfig.DbTiles(
            conn=conn,
            tile_index_schema=db.Schema(self.cfg["config"]["features_tiles"]),
            features_schema=db.Schema(self.cfg["config"]["features"]),
        )
        # Configure feature tiles with elevation from AHN2
        ahn_2 = tileconfig.DbTilesAHN(
            conn=conn,
            elevation_tiles=elevation_tiles,
            feature_tiles=feature_tiles,
        )
        ahn_2.configure(
            tiles=tiles,
            version=2,
            directory_mapping=self.cfg["config"]["directory_mapping"],
            tin=False,
        )
        # Configure feature tiles with elevation from AHN3
        ahn_3 = tileconfig.DbTilesAHN(
            conn=conn,
            elevation_tiles=elevation_tiles,
            feature_tiles=feature_tiles,
        )
        ahn_3.configure(
            tiles=tiles,
            version=3,
            directory_mapping=self.cfg["config"]["directory_mapping"],
            tin=False,
        )
        # Configure feature tiles that are on the border of AHN2 and AHN3
        ahn_border = tileconfig.DbTilesAHN(
            conn=conn,
            elevation_tiles=elevation_tiles,
            feature_tiles=feature_tiles,
        )
        ahn_border.configure(
            tiles=tiles,
            on_border=True,
            directory_mapping=self.cfg["config"]["directory_mapping"],
            tin=False,
        )

        out_dir = output.DirOutput(self.cfg["config"]["output"]["dir"])
        # Set up logic
        parts = {"AHN2": ahn_2, "AHN3": ahn_3, "AHN_border": ahn_border}
        for part, ahntiles in parts.items():
            ahntiles.output = output.Output(
                dir=output.DirOutput(out_dir.join_path(part))
            )
            proc = processor.factory.create(
                processor_key, name=part, tiles=ahntiles
            )
            self.processors[proc] = part
        log.info(f"Configured {self.__class__.__name__}")


class AHNTINBoundaryController(AHNBoundaryController):
    def configure(self, tiles, processor_key: str, worker_key: str):
        """Configure the control logic."""
        worker_init = worker.factory.create(worker_key)
        self.cfg["worker"] = worker_init.execute

        ahntiles = tileconfig.DbTilesAHN(
            conn=db.Db(**self.cfg["config"]["database"]),
            elevation_tiles=db.Schema(self.cfg["config"]["elevation_tiles"]),
            feature_tiles=None,
        )
        ahntiles.configure(
            tiles=tiles,
            directory_mapping=self.cfg["config"]["directory_mapping"],
            tin=True,
        )
        part = "AHN3"
        ahntiles.output = output.Output(
            dir=output.DirOutput(self.cfg["config"]["output"]["dir"])
        )
        proc = processor.factory.create(
            processor_key, name=part, tiles=ahntiles
        )
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
factory.register_controller("Example", ExampleController)
# factory.register_controller('ExampleDb', ExampleDbController)
factory.register_controller("AHN", AHNController)
factory.register_controller("AHNTin", AhnTinController)
factory.register_controller("AHNboundary", AHNBoundaryController)
factory.register_controller("AHNboundaryTIN", AHNTINBoundaryController)
