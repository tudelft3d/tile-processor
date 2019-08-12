# -*- coding: utf-8 -*-

"""Control the execution of several Processors. In some cases it is necessary
to run several Processors in a loop. When there is a large number of tiles and
a few subsets of these tiles need to be processed with different settings,
you might want to maximize the resource use of the Processors, so one can
finish as quickly as possible. Then probably won't be enough resource left
to run a second Processor in parallel. This is where a Controller comes in,
which can control the execution of the Processors."""

import os
import logging
import json
from shutil import copyfile

import yaml
import pykwalify.core
import pykwalify.errors
from click import echo, secho
from click import exceptions

log = logging.getLogger(__name__)
logging.getLogger("pykwalify").setLevel(logging.WARNING)


class ConfigurationSchema:
    """Schema for validating a configuration file."""

    def __init__(self, name=None):
        self.name = name
        self.dir = os.path.join(os.path.dirname(__file__), 'schemas')
        self.db_path = os.path.join(os.path.dirname(__file__), 'schemas', 'schemas.json')
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
            secho(f"Schema '{name}' is not in the database, not removing anything",
                  fg='yellow')
            return
        try:
            p = os.path.join(self.dir, fname)
            os.remove(p)
            echo(f"Removed the configuration schema '{name}'")
        except FileNotFoundError:
            secho(f"Schema file '{fname}' is not in {self.dir}, not removing anything",
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


class ControlThreedfier:
    """Controller for 3dfier"""

    def __init__(self):
        self.schema = ConfigurationSchema('threedfier')

    def configure(self, config):
        cfg = {}

        # -- Get command line parameters, configure temporary files, validate config file
        try:
            cfg_stream = self.schema.validate_configuration(config)
            log.info("Configuration file is valid")
        except:
            raise
        #
        #
        # cfg['config'] = {}
        # cfg['config']['in'] = args_in['cfg_file']
        # rootdir = os.path.dirname(args_in['cfg_file'])
        # rest_dir = os.path.join(rootdir, "cfg_rest")
        # ahn2_dir = os.path.join(rootdir, "cfg_ahn2")
        # ahn3_dir = os.path.join(rootdir, "cfg_ahn3")
        # for d in [rest_dir, ahn2_dir, ahn3_dir]:
        #     if os.path.isdir(d):
        #         rmtree(d, ignore_errors=True, onerror=None)
        #     try:
        #         os.makedirs(d, exist_ok=False)
        #         logger.debug("Created %s", d)
        #     except Exception as e:
        #         logger.error(e)
        # cfg['config']['out_rest'] = os.path.join(rest_dir, "bag3d_cfg_rest.yml")
        # cfg['config']['out_border_ahn2'] = os.path.join(ahn2_dir, "bag3d_cfg_border_ahn2.yml")
        # cfg['config']['out_border_ahn3'] = os.path.join(ahn3_dir, "bag3d_cfg_border_ahn3.yml")
        # cfg['config']['threads'] = int(args_in['threads'])
        #
        # #-- Get config file parameters
        # # database connection
        # cfg['database'] = cfg_stream['database']
        #
        # # 2D polygons
        # cfg['input_polygons'] = cfg_stream['input_polygons']
        # try:
        #     # in case user gave " " or "" for 'extent'
        #     if len(cfg_stream['input_polygons']['extent']) <= 1:
        #         EXTENT_FILE = None
        #         logger.debug('extent string has length <= 1')
        #     cfg['input_polygons']['extent_file'] = os.path.abspath(
        #         cfg_stream['input_polygons']['extent'])
        #     cfg['input_polygons']['tile_list'] = None
        # except (NameError, AttributeError, TypeError):
        #     tile_list = cfg_stream['input_polygons']['tile_list']
        #     assert isinstance(
        #         tile_list, list), "Please provide input for tile_list as a list: [...]"
        #     cfg['input_polygons']['tile_list'] = tile_list
        #     cfg['input_polygons']['extent_file'] = None
        # # 'user_schema' is used for the '_clip3dfy_' and '_union' views, thus
        # # only use 'user_schema' if 'extent' is provided
        # USER_SCHEMA = cfg_stream['input_polygons']['user_schema']
        # if (USER_SCHEMA is None) or (EXTENT_FILE is None):
        #     logger.debug("user_schema or extent is None")
        #     cfg['input_polygons']['user_schema'] = cfg['input_polygons']['tile_schema']
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
        # cfg['output']['staging']['dir'] = os.path.abspath(cfg_stream['output']['staging']['dir'])
        # os.makedirs(cfg['output']['staging']['dir'], exist_ok=True)
        # cfg['output']['production']['dir'] = os.path.abspath(cfg_stream['output']['production']['dir'])
        # os.makedirs(cfg['output']['production']['dir'], exist_ok=True)
        #
        # # executables
        # cfg['path_3dfier'] = cfg_stream['path_3dfier']
        # cfg['path_lasinfo'] = cfg_stream['path_lasinfo']
        #
        # return cfg