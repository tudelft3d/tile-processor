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
from typing import Sequence, List
from time import time, sleep

from psutil import Popen, STATUS_ZOMBIE, STATUS_SLEEPING
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
        exe = os.path.join(package_dir, "src", "simulate_memory_use.sh")
        command = ["bash", exe, "5s"]
        res = run_subprocess(
            command,
            monitor_log=monitor_log,
            monitor_interval=monitor_interval,
            tile_id=tile,
        )
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
        exe = os.path.join(package_dir, "src", "exampledb_processor.sh")
        command = ["bash", exe, "exampledb.output", tile]
        res = run_subprocess(
            command,
            monitor_log=monitor_log,
            monitor_interval=monitor_interval,
            tile_id=tile,
        )
        return res


class ThreedfierWorker:
    """Runs 3dfier."""

    def create_yaml(self, tile, dbtilesahn, ahn_paths):
        """Create the YAML configuration for 3dfier."""
        ahn_file = ""
        if len(ahn_paths) > 1:
            for p, v in ahn_paths:
                ahn_file += "- " + p + "\n" + "              "
        else:
            ahn_file += "- " + ahn_paths[0]
        ahn_version = set([version for path, version in ahn_paths])

        if dbtilesahn.conn.password:
            d = "PG:dbname={dbname} host={host} port={port} user={user} password={pw} schemas={schema_tiles} tables={bag_tile}"
            dsn = d.format(
                dbname=dbtilesahn.conn.dbname,
                host=dbtilesahn.conn.host,
                port=dbtilesahn.conn.port,
                user=dbtilesahn.conn.user,
                pw=dbtilesahn.conn.password,
                schema_tiles=dbtilesahn.feature_tiles.features.schema.string,
                bag_tile=dbtilesahn.feature_views[tile],
            )
        else:
            d = "PG:dbname={dbname} host={host} port={port} user={user} schemas={schema_tiles} tables={bag_tile}"
            dsn = d.format(
                dbname=dbtilesahn.conn.dbname,
                host=dbtilesahn.conn.host,
                port=dbtilesahn.conn.port,
                user=dbtilesahn.conn.user,
                schema_tiles=dbtilesahn.feature_tiles.features.schema.string,
                bag_tile=dbtilesahn.feature_views[tile],
            )

        if ahn_version == {2}:
            las_building = [1]
        elif ahn_version == {3}:
            las_building = [6]
        elif ahn_version == {2, 3}:
            las_building = [1, 6]
        else:
            las_building = None
        uniqueid = dbtilesahn.feature_tiles.features.field.uniqueid.string

        yml = yaml.load(
            f"""
        input_polygons:
          - datasets:
              - "{dsn}"
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
        """,
            yaml.FullLoader,
        )
        return yml

    def execute(
        self,
        tile,
        tiles,
        path_executable,
        monitor_log,
        monitor_interval,
        **ignore,
    ) -> bool:
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
            yml = self.create_yaml(
                tile=tile,
                dbtilesahn=tiles,
                ahn_paths=tiles.elevation_file_index[tile],
            )
            yml_path = str(tiles.output.dir.join_path(f"{tile}.yml"))
            try:
                with open(yml_path, "w") as fo:
                    yaml.dump(yml, fo)
            except BaseException as e:
                log.exception(f"Error: cannot write {yml_path}")

            output_path = str(tiles.output.dir.join_path(f"{tile}.csv"))
            command = [
                path_executable,
                yml_path,
                "--stat_RMSE",
                "--CSV-BUILDINGS-MULTIPLE",
                output_path,
            ]
            try:
                success = run_subprocess(
                    command,
                    shell=True,
                    doexec=True,
                    monitor_log=monitor_log,
                    monitor_interval=monitor_interval,
                    tile_id=tile,
                )
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
    def create_yaml(self, tile, dbtilesahn, ahn_paths, simplification_tinsimp):
        """Create the YAML configuration for 3dfier."""
        ahn_file = ""
        if len(ahn_paths) > 1:
            for p, v in ahn_paths:
                ahn_file += "- " + p + "\n" + "              "
        else:
            ahn_file += "- " + ahn_paths[0]

        if dbtilesahn.conn.password:
            d = "PG:dbname={dbname} host={host} port={port} user={user} password={pw} schemas={schema_tiles} tables={bag_tile}"
            dsn = d.format(
                dbname=dbtilesahn.conn.dbname,
                host=dbtilesahn.conn.host,
                port=dbtilesahn.conn.port,
                user=dbtilesahn.conn.user,
                pw=dbtilesahn.conn.password,
                schema_tiles=dbtilesahn.feature_tiles.features.schema.string,
                bag_tile=dbtilesahn.feature_views[tile],
            )
        else:
            d = "PG:dbname={dbname} host={host} port={port} user={user} schemas={schema_tiles} tables={bag_tile}"
            dsn = d.format(
                dbname=dbtilesahn.conn.dbname,
                host=dbtilesahn.conn.host,
                port=dbtilesahn.conn.port,
                user=dbtilesahn.conn.user,
                schema_tiles=dbtilesahn.feature_tiles.features.schema.string,
                bag_tile=dbtilesahn.feature_views[tile],
            )

        uniqueid = dbtilesahn.feature_tiles.features.field.uniqueid.string

        simplification_tinsimp = str(simplification_tinsimp)
        yml = yaml.load(
            f"""
        input_polygons:
          - datasets:
              - "{dsn}"
            uniqueid: {uniqueid}
            lifting: Terrain

        lifting_options:
          Terrain:
            simplification_tinsimp: {simplification_tinsimp}
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
        """,
            yaml.FullLoader,
        )
        return yml

    def execute(
        self,
        tile,
        tiles,
        simplification_tinsimp,
        out_format,
        out_format_ext,
        path_executable,
        monitor_log,
        monitor_interval,
        **ignore,
    ) -> bool:
        log.debug(f"Running {self.__class__.__name__}:{tile}")
        if len(tiles.elevation_file_index[tile]) == 0:
            log.debug(f"Elevation files are not available for tile {tile}")
            return False
        else:
            yml = self.create_yaml(
                tile=tile,
                dbtilesahn=tiles,
                ahn_paths=tiles.elevation_file_index[tile],
                simplification_tinsimp=simplification_tinsimp,
            )
            yml_path = str(tiles.output.dir.join_path(f"{tile}.yml"))
            log.debug(f"{yml_path}\n{yml}")
            try:
                with open(yml_path, "w") as fo:
                    yaml.dump(yml, fo)
            except BaseException as e:
                log.exception(f"Error: cannot write {yml_path}")

            output_path = str(
                tiles.output.dir.join_path(f"{tile}.{out_format_ext}")
            )
            command = [path_executable, yml_path, out_format, output_path]
            try:
                success = run_subprocess(
                    command,
                    shell=True,
                    doexec=True,
                    monitor_log=monitor_log,
                    monitor_interval=monitor_interval,
                    tile_id=tile,
                )
                return success
            except BaseException as e:
                log.exception("Cannot run 3dfier on tile %s", tile)
                return False
            finally:
                try:
                    os.remove(yml_path)
                except Exception as e:
                    log.error(e)


class Geoflow:
    def create_configuration(self, tile: str, tiles: DbTilesAHN, kwargs) -> List:
        """Create a tile-specific configuration file."""
        pass

    def execute(
        self,
        tile: str,
        tiles: DbTilesAHN,
        path_executable: str,
        path_flowchart: str,
        monitor_log: logging.Logger,
        monitor_interval: int,
        path_toml: str = None,
        doexec: bool = True,
        run_reference: str = None,
        **ignore,
    ) -> bool:
        """Execute Geoflow.

        :param path_toml:
        :param tile: Tile ID to process
        :param tiles: Feature tiles configuration object
        :param path_executable: Absolute path to the Geoflow exe
        :param path_flowchart: Absolute path to the Geoflow flowchart
        :param path_toml: Absolute path to the Geoflow configuration TOML file that holds default values
        :param monitor_log:
        :param monitor_interval:
        :param doexec:
        :return: Success or Failure
        """
        log.debug(f"Running {self.__class__.__name__}:{tile}")
        if len(tiles.elevation_file_index[tile]) == 0:
            log.debug(f"Elevation files are not available for tile {tile}")
            return False
        kwargs = {"run_reference": run_reference}
        config = self.create_configuration(tile=tile, tiles=tiles, kwargs=kwargs)
        if config is not None and len(config) > 0:
            if path_toml is not None and len(path_toml) > 0:
                command = [
                    path_executable,
                    path_flowchart,
                    "--config",
                    path_toml,
                ] + config
            else:
                command = [path_executable, path_flowchart] + config
            try:
                success = run_subprocess(
                    command,
                    shell=False,
                    doexec=doexec,
                    monitor_log=monitor_log,
                    monitor_interval=monitor_interval,
                    tile_id=tile,
                )
                return success
            except BaseException:
                log.exception(
                    f"Cannot run {os.path.basename(path_executable)} on tile {tile}"
                )
                return False
        else:
            return False


class BuildingReconstructionWorker(Geoflow):
    def create_configuration(self, tile: str, tiles: DbTilesAHN, kwargs):
        # Create the Postgres connection string
        dsn_in = (
            f"PG:dbname={tiles.conn.dbname} "
            f"host={tiles.conn.host} "
            f"port={tiles.conn.port} "
            f"user={tiles.conn.user} "
            f"schemas={tiles.feature_tiles.features.schema.string} "
            f"tables={tiles.feature_views[tile]}"
        )
        if tiles.conn.password:
            dsn_in += f" password={tiles.conn.password}"
        # Select the las file paths for the tile
        input_las_files = [p[0] for p in tiles.elevation_file_index[tile]]
        # Create the output connection string
        if tiles.output.db is not None:
            dsn_out = tiles.output.db.dsn_no_relation()
            if tiles.output.db.schema is not None:
                out_layer_template = f"{tiles.output.db.schema}.{tiles.output.kwargs.get('table_prefix', '')}"
            else:
                out_layer_template = tiles.output.kwargs.get("table_prefix", "")
            t_lod11_2d = out_layer_template + "lod11_2d"
            t_lod12_2d = out_layer_template + "lod12_2d"
            t_lod12_3d = out_layer_template + "lod12_3d"
            t_lod13_2d = out_layer_template + "lod13_2d"
            t_lod13_3d = out_layer_template + "lod13_3d"
            t_lod22_2d = out_layer_template + "lod22_2d"
            t_lod22_3d = out_layer_template + "lod22_3d"
            t_lod12_3d_tri = out_layer_template + "lod12_3d_tri"
            t_lod13_3d_tri = out_layer_template + "lod13_3d_tri"
            t_lod22_3d_tri = out_layer_template + "lod22_3d_tri"
            format_out = "PostgreSQL"
        else:
            raise ValueError(f"Invalid Output type {type(tiles.output)}")
        # Put together the configuration
        config = []
        config.append(f"--INPUT_FOOTPRINT_SOURCE={dsn_in}")

        config.append(f"--overwrite_output=false")

        config.append(f"--OUTPUT_DB_CONNECTION={dsn_out}")

        config.append(f"--OUTPUT_LAYERNAME_LOD11_2D={t_lod11_2d}")
        config.append(f"--OUTPUT_LAYERNAME_LOD12_2D={t_lod12_2d}")
        config.append(f"--OUTPUT_LAYERNAME_LOD12_3D={t_lod12_3d}")
        config.append(f"--OUTPUT_LAYERNAME_LOD13_2D={t_lod13_2d}")
        config.append(f"--OUTPUT_LAYERNAME_LOD13_3D={t_lod13_3d}")
        config.append(f"--OUTPUT_LAYERNAME_LOD22_2D={t_lod22_2d}")
        config.append(f"--OUTPUT_LAYERNAME_LOD22_3D={t_lod22_3d}")

        config.append(f"--OUTPUT_LAYERNAME_LOD12_3D_tri={t_lod12_3d_tri}")
        config.append(f"--OUTPUT_LAYERNAME_LOD13_3D_tri={t_lod13_3d_tri}")
        config.append(f"--OUTPUT_LAYERNAME_LOD22_3D_tri={t_lod22_3d_tri}")

        config.append(f"--TILE_ID={tile}")
        config.append(f"--OUTPUT_FORMAT={format_out}")
        if tiles.output.dir is not None and "obj" in tiles.output.dir:
            config.append(f"--OUTPUT_OBJ_DIR={tiles.output.dir['obj'].path}")
        if tiles.output.dir is not None and "cityjson" in tiles.output.dir:
            config.append(f"--OUTPUT_CITYJSON_DIR={tiles.output.dir['cityjson'].path}")

        config.append(f"--RUN_REFERENCE={kwargs['run_reference']}")

        config.append(f"--INPUT_LAS_FILES=")
        config.extend(input_las_files)

        return config


class AlphaShapeWorker(Geoflow):
    def create_configuration(self, tile: str, tiles: DbTilesAHN, kwargs):
        # Select the las file paths for the tile
        input_las_files = [p[0] for p in tiles.elevation_file_index[tile]]
        # --OUTPUT_LAYER_PREFIX and table name in tables= in the --OUTPUT_SOURCE must
        # be the same.
        table_name = (
            tiles.output.kwargs["table_prefix"] + "alpha_shape_buildings"
        )
        # Create the output connection string
        if tiles.output.db is not None:
            dsn_out = tiles.output.db.with_table(table_name)
            format_out = "PostgreSQL"
        else:
            raise ValueError(f"Invalid Output type {type(tiles.output)}")
        # Put together the configuration
        config = []
        config.append(f"--overwrite_output=false")
        config.append(f"--OUTPUT_LAYER_PREFIX={table_name}")
        config.append(f"--OUTPUT_SOURCE={dsn_out}")
        config.append(f"--OUTPUT_FORMAT={format_out}")
        config.append(f"--INPUT_LAS_FILES=")
        config.extend(input_las_files)
        return config


class TileExporter:
    def execute(
        self,
        tile: str,
        tiles: DbTilesAHN,
        path_lasmerge,
        path_ogr2ogr,
        out_dir,
        monitor_log: logging.Logger,
        monitor_interval: int,
        doexec: bool = True,
        **ignore,
    ) -> bool:
        log.debug(f"Running {self.__class__.__name__}:{tile}")
        results = []
        # Create the Postgres connection string
        dsn_in = (
            f"PG:dbname={tiles.conn.dbname} "
            f"host={tiles.conn.host} "
            f"port={tiles.conn.port} "
            f"user={tiles.conn.user} "
            f"schemas={tiles.feature_tiles.features.schema.string} "
            f"tables={tiles.feature_views[tile]}"
        )
        if tiles.conn.password:
            dsn_in += f" password={tiles.conn.password}"
        # Select the las file paths for the tile
        input_las_files = [p[0] for p in tiles.elevation_file_index[tile]]

        if len(tiles.elevation_file_index[tile]) == 0:
            log.debug(f"Elevation files are not available for tile {tile}")
            return False

        log.debug(f"Exporting footprints to GPKG:{tile}")
        # FIXME: this doesnt work on windows
        command = [
            path_ogr2ogr,
            "-f",
            "GPKG",
            f"{out_dir}/{tile}.gpkg",
            dsn_in,
        ]
        try:
            success = run_subprocess(
                command,
                shell=False,
                doexec=doexec,
                monitor_log=monitor_log,
                monitor_interval=monitor_interval,
                tile_id=tile,
            )
            results.append(success)
        except BaseException:
            log.exception(
                f"Cannot run {os.path.basename(path_ogr2ogr)} on tile {tile}"
            )
            results.append(False)

        log.debug(f"Merging LAZ files for:{tile}")
        command = [path_lasmerge, "-i"]
        command.extend(input_las_files)
        # FIXME: this doesnt work on windows
        command.extend(["-o", f"{out_dir}/{tile}.laz"])
        try:
            success = run_subprocess(
                command,
                shell=False,
                doexec=doexec,
                monitor_log=monitor_log,
                monitor_interval=monitor_interval,
                tile_id=tile,
            )
            results.append(success)
        except BaseException:
            log.exception(
                f"Cannot run {os.path.basename(path_lasmerge)} on tile {tile}"
            )
            results.append(False)
        return all(results)


def run_subprocess(
    command: Sequence[str],
    shell: bool = False,
    doexec: bool = True,
    monitor_log: logging.Logger = None,
    monitor_interval: int = 5,
    tile_id: str = None,
) -> bool:
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
        start = time()
        popen = Popen(command, shell=shell, stderr=PIPE, stdout=PIPE)
        if monitor_log is not None:
            while True:
                sleep(monitor_interval)
                monitor_log.info(
                    f"{tile_id}\t{popen.pid}\t{popen.cpu_times().user}"
                    f"\t{popen.memory_info().rss}"
                )
                if (
                    not popen.is_running()
                    or popen.status() == STATUS_ZOMBIE
                    or popen.status() == STATUS_SLEEPING
                ):
                    break
        stdout, stderr = popen.communicate()
        err = stderr.decode(getpreferredencoding(do_setlocale=True))
        out = stdout.decode(getpreferredencoding(do_setlocale=True))
        finish = time()
        log.info(f"Tile {tile_id} finished in {(finish-start)/60} minutes")
        if popen.returncode != 0:
            log.error(f"Tile {tile_id} process returned with {popen.returncode}")
        else:
            log.debug(f"Tile {tile_id} process returned with {popen.returncode}")
        log.debug(f"Tile {tile_id} stdout: \n{out}")
        log.debug(f"Tile {tile_id} stderr: \n{err}")
        return True if popen.returncode == 0 else False
    else:
        log.debug(f"Tile {tile_id} not executing {command}")
        return True


factory = WorkerFactory()
factory.register_worker("Example", ExampleWorker)
factory.register_worker("ExampleDb", ExampleDbWorker)
factory.register_worker("3dfier", ThreedfierWorker)
factory.register_worker("3dfierTIN", ThreedfierTINWorker)
factory.register_worker("BuildingReconstruction", BuildingReconstructionWorker)
factory.register_worker("AlphaShape", AlphaShapeWorker)
factory.register_worker("TileExporter", TileExporter)
