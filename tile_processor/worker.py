# -*- coding: utf-8 -*-

"""Workers run the executables. Executables can be anything, but most likely
they are compiled software that are called in a subprocess, for example
*3dfier* (threedfier). In order to implement your own Worker, implement your
class/function here and register it in the click command.
The Factory-pattern reference: `https://realpython.com/factory-method-python/
<https://realpython.com/factory-method-python/>`_
"""

import os
import logging
from locale import getpreferredencoding
from subprocess import PIPE
from time import sleep
from typing import Sequence
import json

from psutil import Popen
import yaml

from tile_processor.tileconfig import DbTilesAHN

log = logging.getLogger(__name__)

# TODO BD: might be worth to make a Worker parent class with the run_subprocess
# method in it. On the other hand, not every Worker might need a subprocess
# runner

class WorkerFactory:
    """Registers and instantiates an Worker.

    A Worker is responsible for running an executable, e.g. 3dfier in case of
    :py:class:`.ThreedfierWorker`
    """

    def __init__(self):
        self._executors = {}

    def register_worker(self, key, worker):
        """Register a worker for use.

        :param key: Name of the worker
        :param worker: Can be a function, a class, or an object that implements
            `.__call__()`
        """
        self._executors[key] = worker

    def create(self, key, **kwargs):
        """Instantiate a worker"""
        worker = self._executors.get(key)
        if not worker:
            raise ValueError(key)
        return worker(**kwargs)


class ExampleWorker:
    """Runs the template."""

    def execute(self, monitor_log, monitor_interval, tile, **ignore) -> bool:
        """Execute the TemplateWorker with the provided configuration.

        The worker will execute the `./src/simlate_memory_use.sh` script, which
        allocates a constant amount of RAM (~600Mb) and 'holds' it for 10s.

        :return: True/False on success/failure
        """
        log.debug(f"Running {self.__class__.__name__}:{tile}")
        package_dir = os.path.dirname(os.path.dirname(__file__))
        exe = os.path.join(package_dir, 'src', 'simulate_memory_use.sh')
        command = ['bash', exe, '5s']
        res = run_subprocess(command, monitor_log=monitor_log,
                             monitor_interval=monitor_interval, tile_id=tile)
        return res


class ExampleDbWorker:
    """Runs the template."""

    def execute(self, monitor_log, monitor_interval, tile, **ignore) -> bool:
        """Execute the TemplateWorker with the provided configuration.

        Simply print the processed tile ID into a file.

        :return: True/False on success/failure
        """
        log.debug(f"Running {self.__class__.__name__}:{tile}")
        package_dir = os.path.dirname(os.path.dirname(__file__))
        exe = os.path.join(package_dir, 'src', 'exampledb_processor.sh')
        command = ['bash', exe, 'exampledb.output', tile]
        res = run_subprocess(command, monitor_log=monitor_log,
                             monitor_interval=monitor_interval, tile_id=tile)
        return res


class ThreedfierWorker:
    """Runs 3dfier."""

    def create_yaml(self, tile, dbtilesahn, ahn_paths):
        """Create the YAML configuration for 3dfier."""
        ahn_file = ""
        if len(ahn_paths) > 1:
            for p,v in ahn_paths:
                ahn_file += "- " + p + "\n" + "              "
        else:
            ahn_file += "- " + ahn_paths[0]
        ahn_version = set([version for path,version in ahn_paths])

        if dbtilesahn.conn.password:
            d = 'PG:dbname={dbname} host={host} port={port} user={user} password={pw} schemas={schema_tiles} tables={bag_tile}'
            dns = d.format(dbname=dbtilesahn.conn.dbname,
                           host=dbtilesahn.conn.host,
                           port=dbtilesahn.conn.port,
                           user=dbtilesahn.conn.user,
                           pw=dbtilesahn.conn.password,
                           schema_tiles=dbtilesahn.feature_tiles.features.schema.string,
                           bag_tile=dbtilesahn.feature_views[tile])
        else:
            d = 'PG:dbname={dbname} host={host} port={port} user={user} schemas={schema_tiles} tables={bag_tile}'
            dns = d.format(dbname=dbtilesahn.conn.dbname,
                           host=dbtilesahn.conn.host,
                           port=dbtilesahn.conn.port,
                           user=dbtilesahn.conn.user,
                           schema_tiles=dbtilesahn.feature_tiles.features.schema.string,
                           bag_tile=dbtilesahn.feature_views[tile])

        if ahn_version == {2}:
            las_building = [1]
        elif ahn_version == {3}:
            las_building = [6]
        elif ahn_version == {2, 3}:
            las_building = [1, 6]
        else:
            las_building = None
        uniqueid = dbtilesahn.feature_tiles.features.field.uniqueid.string

        yml = yaml.load(f"""
        input_polygons:
          - datasets:
              - "{dns}"
            uniqueid: {uniqueid}
            lifting: Building

        lifting_options:
          Building:
            roof:
              height: percentile-95
              use_LAS_classes: {las_building}
            ground:
              height: percentile-10
              use_LAS_classes: [2]

        input_elevation:
          - datasets:
              {ahn_file}
            omit_LAS_classes:
            thinning: 0

        options:
          building_radius_vertex_elevation: 0.5
          radius_vertex_elevation: 0.5
          threshold_jump_edges: 0.5
        """, yaml.FullLoader)
        return yml

    def execute(self, tile, tiles, path_executable, monitor_log, monitor_interval,
                **ignore) -> bool:
        """

        :param tile:
        :param tiles: DbTilesAHN object
        :param path_executable:
        :param monitor_log:
        :param monitor_interval:
        :param ignore:
        :return:
        """
        log.debug(f"Running {self.__class__.__name__}:{tile}")
        if len(tiles.elevation_file_index[tile]) == 0:
            log.debug(f"Elevation files are not available for tile {tile}")
            return False
        else:
            yml = self.create_yaml(tile=tile, dbtilesahn=tiles,
                                   ahn_paths=tiles.elevation_file_index[tile])
            yml_path = tiles.output.add(f"{tile}.yml")
            try:
                with open(yml_path, "w") as fo:
                    yaml.dump(yml, fo)
            except BaseException as e:
                log.exception(f"Error: cannot write {yml_path}")

            output_path = tiles.output.add(f"{tile}.csv")
            command = [path_executable, yml_path, "--stat_RMSE",
                       "--CSV-BUILDINGS-MULTIPLE",
                       output_path]
            try:
                success = run_subprocess(
                    command, shell=True, doexec=True,
                    monitor_log=monitor_log, monitor_interval=monitor_interval,
                    tile_id=tile)
                return success
            except BaseException as e:
                log.exception("Cannot run 3dfier on tile %s", tile)
                return False
            finally:
                try:
                    os.remove(yml_path)
                except Exception as e:
                    log.error(e)


class ThreedfierTINWorker:

    def create_yaml(self, tile, dbtilesahn, ahn_paths, tinsimp):
        """Create the YAML configuration for 3dfier."""
        ahn_file = ""
        if len(ahn_paths) > 1:
            for p,v in ahn_paths:
                ahn_file += "- " + p + "\n" + "              "
        else:
            ahn_file += "- " + ahn_paths[0]

        if dbtilesahn.conn.password:
            d = 'PG:dbname={dbname} host={host} port={port} user={user} password={pw} schemas={schema_tiles} tables={bag_tile}'
            dns = d.format(dbname=dbtilesahn.conn.dbname,
                           host=dbtilesahn.conn.host,
                           port=dbtilesahn.conn.port,
                           user=dbtilesahn.conn.user,
                           pw=dbtilesahn.conn.password,
                           schema_tiles=dbtilesahn.feature_tiles.features.schema.string,
                           bag_tile=dbtilesahn.feature_views[tile])
        else:
            d = 'PG:dbname={dbname} host={host} port={port} user={user} schemas={schema_tiles} tables={bag_tile}'
            dns = d.format(dbname=dbtilesahn.conn.dbname,
                           host=dbtilesahn.conn.host,
                           port=dbtilesahn.conn.port,
                           user=dbtilesahn.conn.user,
                           schema_tiles=dbtilesahn.feature_tiles.features.schema.string,
                           bag_tile=dbtilesahn.feature_views[tile])

        uniqueid = dbtilesahn.feature_tiles.features.field.uniqueid.string

        tinsimp = str(tinsimp)
        yml = yaml.load(f"""
        input_polygons:
          - datasets:
              - "{dns}"
            uniqueid: {uniqueid}
            lifting: Terrain

        lifting_options:
          Terrain:
            simplification_tinsimp: {tinsimp}
            inner_buffer: 0.1
            use_LAS_classes:
              - 2

        input_elevation:
          - datasets:
              {ahn_file}
            omit_LAS_classes:
            thinning: 0

        options:
          building_radius_vertex_elevation: 0.5
          radius_vertex_elevation: 0.5
          threshold_jump_edges: 0.5
        """, yaml.FullLoader)
        return yml

    def execute(self, tile, tiles, tinsimp, out_format, out_format_ext,
                path_executable, monitor_log,
                monitor_interval,
                **ignore) -> bool:
        log.debug(f"Running {self.__class__.__name__}:{tile}")
        if len(tiles.elevation_file_index[tile]) == 0:
            log.debug(f"Elevation files are not available for tile {tile}")
            return False
        else:
            yml = self.create_yaml(tile=tile,
                                   dbtilesahn=tiles,
                                   ahn_paths=tiles.elevation_file_index[tile],
                                   tinsimp=tinsimp)
            yml_path = tiles.output.add(f"{tile}.yml")
            log.debug(f"{yml_path}\n{yml}")
            try:
                with open(yml_path, "w") as fo:
                    yaml.dump(yml, fo)
            except BaseException as e:
                log.exception(f"Error: cannot write {yml_path}")

            output_path = tiles.output.add(f"{tile}.{out_format_ext}")
            command = [path_executable, yml_path, out_format,
                       output_path]
            try:
                success = run_subprocess(
                    command, shell=True, doexec=True,
                    monitor_log=monitor_log, monitor_interval=monitor_interval,
                    tile_id=tile)
                return success
            except BaseException as e:
                log.exception("Cannot run 3dfier on tile %s", tile)
                return False
            finally:
                try:
                    os.remove(yml_path)
                except Exception as e:
                    log.error(e)


class LoD13Worker:

    def execute(self, tile, tiles, path_executable, monitor_log,
                monitor_interval, **ignore):
        log.debug(f"Running {self.__class__.__name__}:{tile}")
        ahn_file = tiles.file_index[tile][0]
        # FIXME: hardcoded schema
        dsn = f"'PG:dbname={tiles.conn.dbname} host={tiles.conn.host} " \
              f"user={tiles.conn.user} port={tiles.conn.port} " \
              f"schemas=bag_tiles tables=t_{tile}'"
        out_lod10 = tiles.output.add(f"{tile}_lod10_p95.gpkg")
        out_lod13 = tiles.output.add(f"{tile}_lod13_p95.gpkg")
        command = [
            path_executable,
            '--regularise_footprint',
            '--percentile', '0.95',
            '--las', ahn_file,
            '--footprints', dsn,
            '--output10', out_lod10,
            '--outputH', out_lod13
        ]
        try:
            success = run_subprocess(
                command, shell=True, doexec=True,
                monitor_log=monitor_log, monitor_interval=monitor_interval,
                tile_id=tile)
            return success
        except BaseException as e:
            log.exception(f"Cannot run {os.path.basename(path_executable)} "
                          f"on tile {tile}")
            return False


class Geoflow:

    def create_configuration(self, *args, **kwargs):
        """Create a tile-specific configuration file."""

    def execute(self, tile: str, tiles: DbTilesAHN, path_executable: str,
                path_flowchart: str,
                path_config: str, monitor_log: logging.Logger,
                monitor_interval: int,
                **ignore) -> bool:
        """Execute Geoflow.

        :param tile: Tile ID to process
        :param tiles: Feature tiles configuration object
        :param path_executable: Absolute path to the Geoflow exe
        :param path_flowchart: Absolute path to the Geoflow flowchart
        :param path_config: Absolute path to the Geoflow configuration file
        :param monitor_log:
        :param monitor_interval:
        :return: Success or Failiure
        """
        log.debug(f"Running {self.__class__.__name__}:{tile}")
        pc_match = tiles.match_elevation_tile(feature_tile=tile,
                                              idx_identical=False)
        ahn_file = tiles.elevation_file_index[tile][0]
        _json = self.create_configuration(
            tile=tile, feature_tiles=tiles, pc_match=pc_match,
            path_config=path_config)
        json_path = tiles.output.add(f"{tile}.json")
        log.debug(f"{json_path}\n{_json}")
        try:
            with open(json_path, "w") as fo:
                json.dump(_json, fo)
        except BaseException as e:
            log.exception(f"Error: cannot write {json_path}")
        command = [
            path_executable,
            '-f', path_flowchart,
            '-c', path_config
        ]
        try:
            success = run_subprocess(
                command, shell=True, doexec=True,
                monitor_log=monitor_log, monitor_interval=monitor_interval,
                tile_id=tile)
            return success
        except BaseException as e:
            log.exception(f"Cannot run {os.path.basename(path_executable)} "
                          f"on tile {tile}")
            return False
        finally:
            try:
                os.remove(json_path)
            except Exception as e:
                log.error(e)


class GeoflowWorker(Geoflow):

    def create_configuration(self,
                             tile: str=None,
                             feature_tiles: DbTilesAHN=None,
                             path_config: str=None,
                             pc_match: Sequence[str]=None):
        output_prefix = f'{tile}_'
        input_pc_files = pc_match
        feature_view = feature_tiles.feature_views[tile]
        if feature_tiles.conn.password:
            d = 'PG:dbname={dbname} host={host} port={port} user={user} password={pw} active_schema={schema_tiles} tables={tile}'
            dns = d.format(dbname=feature_tiles.conn.dbname,
                           host=feature_tiles.conn.host,
                           port=feature_tiles.conn.port,
                           user=feature_tiles.conn.user,
                           pw=feature_tiles.conn.password,
                           schema_tiles=feature_tiles.elevation_index_schema.schema.string,
                           tile='t' + feature_view)
        else:
            d = 'PG:dbname={dbname} host={host} port={port} user={user} active_schema={schema_tiles} tables={tile}'
            dns = d.format(dbname=feature_tiles.conn.dbname,
                           host=feature_tiles.conn.host,
                           port=feature_tiles.conn.port,
                           user=feature_tiles.conn.user,
                           schema_tiles=feature_tiles.elevation_index_schema.schema.string,
                           tile='t' + feature_view)

        with open(path_config, 'r') as fo:
            j = json.load(fo)
        j['nodes']['OGRLoader']['parameters']['filepath'] = dns
        j['nodes']['OGRWriter']['parameters']['layername'] = tile
        return j


def run_subprocess(command: Sequence[str],
                   shell: bool = False,
                   doexec: bool = True,
                   monitor_log: logging.Logger = None,
                   monitor_interval: int = 5,
                   tile_id: str = None) -> bool:
    """Runs a subprocess with `psutil.Popen` and monitors its status.

    If subprocess returns non-zero exit code, STDERR is sent to the log.

    :param command: The command to execute.
    :param shell: Passed to `psutil.Popen`. Defaults to False.
    :param doexec: Do execute the subprocess or just print out the concatenated
        command. Used for testing.
    :param monitor_log: A resource logger, which is returned by
        :func:`~.recorder.configure_resource_logging`.
    :param monitor_interval: How often query the resource usage of the process?
        In seconds.
    :param tile_id: Used for monitoring only.
    :return: True/False on success/failure
    """
    if doexec:
        cmd = " ".join(command)
        if shell:
            command = cmd
        log.debug(f"Tile {tile_id} command: {command}")
        popen = Popen(command, shell=shell, stderr=PIPE, stdout=PIPE)
        if monitor_log is not None:
            while True:
                sleep(monitor_interval)
                monitor_log.info(
                    f"{tile_id}\t{popen.pid}\t{popen.cpu_times().user}"
                    f"\t{popen.cpu_times().system}\t{popen.memory_info().rss}")
                return_code = popen.poll()
                if return_code is not None:
                    break
        stdout, stderr = popen.communicate()
        err = stderr.decode(getpreferredencoding(do_setlocale=True))
        out = stdout.decode(getpreferredencoding(do_setlocale=True))
        popen.wait()
        log.debug(f"Tile {tile_id} stdout: {out}")
        log.debug(f"Tile {tile_id} stderr: {err}")
        if popen.returncode != 0 or 'error' in err.lower():
            log.error(f"Tile {tile_id} process returned with non-zero exit "
                      f"code {popen.returncode}")
            log.error(f"Tile {tile_id} err: \n{out}")
            log.error(f"Tile {tile_id} err: \n{err}")
            return False
        else:
            return True
    else:
        log.debug(f"Tile {tile_id} not executing {command}")
        return True


factory = WorkerFactory()
factory.register_worker('Example', ExampleWorker)
factory.register_worker('ExampleDb', ExampleDbWorker)
factory.register_worker('3dfier', ThreedfierWorker)
factory.register_worker('3dfierTIN', ThreedfierTINWorker)
factory.register_worker('LoD13', LoD13Worker)
factory.register_worker('Geoflow', GeoflowWorker)
