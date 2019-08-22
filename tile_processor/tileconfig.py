# -*- coding: utf-8 -*-

"""Tile configuration."""


import logging
import os
import re
from random import shuffle
from typing import List, Tuple

import fiona
from psycopg2 import sql
from shapely import geos
from shapely.geometry import shape, Polygon

from tile_processor import db

log = logging.getLogger(__name__)


class DbTiles:
    """Configures the tiles and tile index that is stored in PostgreSQL."""

    def __init__(self, conn: db.Db, index_schema: db.Schema,
                 feature_schema: db.Schema, output=None):
        self.conn = conn
        self.to_process = []
        self.index = index_schema
        self.features = feature_schema
        self.output = output

    def configure(self, tiles: List[str] = None, extent=None):
        """Configure the tiles for processing.

        You can provide either `tiles` or `extent`.

        :param tiles: A list of tile IDs, or a list with a single item
            `['all',]` for processing all tiles.
        :param extent: A polygon for selecting the tiles. The selected tiles
            are clipped by the `extent`. If the area of the `extent` is less
            than that of a single tile, than the clipped tile-parts within the
            extent are dissolved into a single tile.
        """
        if (extent and tiles) or ((not extent) and (not tiles)):
            raise AttributeError("Provide either 'tiles' or 'extent'.")
        if extent:
            self.to_process = self._with_extent(extent)
        else:
            self.to_process = self._with_list(tiles)

    def _with_extent(self, extent) -> List:
        """Select tiles based on a polygon."""
        log.info("Clipping the tiles to the extent.")
        poly, ewkb = self.read_extent(extent)
        return self.within_extent(ewkb=ewkb)

    def _with_list(self, tiles) -> List:
        """Select tiles based on a list of tile IDs."""
        if ['all',] == tiles:
            log.info("Getting all tiles from the index.")
            in_index = self.all_in_index()
        else:
            log.info("Verifying if the provided tiles are in the index.")
            in_index = self.tiles_in_index(tiles)
        if len(in_index) == 0:
            raise AttributeError("None of the provided tiles are present in the"
                                 " index.")
        else:
            return in_index

    @staticmethod
    def read_extent(extent: str) -> Tuple[Polygon, str]:
        """Reads a polygon from a file and returns it as Shapely polygon and
        EWKB.

        :param extent: Path to a file (eg GeoJSON), contiaining a single polygon
        :return: A tuple of (Shapely Polygon, EWKB). If the extent doesn't have
            a CRS, then a WKB is returned instead of the EWKB.
        """
        pattern = re.compile(r'(?<=epsg:)\d+', re.IGNORECASE)
        # Get clip polygon and set SRID
        with fiona.open(extent, 'r') as src:
            epsg = pattern.search(src.crs['init']).group()
            poly = shape(src[0]['geometry'])
            if epsg is None:
                log.warning(f"Did not find the EPSG code of the CRS: {src.crs}")
                wkb = poly.wkb_hex
                return poly, wkb
            else:
                # Change a the default mode to add this, if SRID is set
                geos.WKBWriter.defaults['include_srid'] = True
                # set SRID for polygon
                geos.lgeos.GEOSSetSRID(poly._geom, int(epsg))
                ewkb = poly.wkb_hex
                return poly, ewkb

    def _within_extent(self, ewkb: str) -> sql.Composed:
        """Return a query for the features that are `within
        <http://postgis.net/docs/manual-2.5/ST_Within.html>`_ the extent. If
        features are not provided (as `feature_schema`), then select the tiles
        that intersect the extent.
        """
        if self.features:
            query_params = {
                'features': self.features.schema + self.features.table,
                'feature_pk': self.features.table + self.features.field.pk,
                'feature_geom': self.features.table + self.features.field.geometry,
                'index_': self.index.schema + self.index.table,
                'ewkb': sql.Literal(ewkb),
                'tile': self.index.table + self.index.field.tile,
                'index_pk': self.index.table + self.index.field.pk
            }
            query = sql.SQL("""
            SELECT {features}.*, {tile} AS tile_id
            FROM {features} JOIN {index_} ON {feature_pk} = {index_pk}
            WHERE st_within({feature_geom}, {ewkb}::geometry)
            """).format(**query_params)
        else:
            query_params = {
                'index_': self.index.schema + self.index.table,
                'ewkb': sql.Literal(ewkb),
                'tile': self.index.table + self.index.field.tile,
                'index_geom': self.index.table + self.index.field.geometry
            }
            query = sql.SQL("""
            SELECT {tile} AS tile_id
            FROM {index_}
            WHERE st_intersects({index_geom}, {ewkb}::geometry)
            """).format(**query_params)
        return query

    def within_extent(self, ewkb: str, reorder: bool = True) -> List[str]:
        """Get a list of tiles that are within the extent."""
        within_query = self._within_extent(ewkb)
        query = sql.SQL("""
        SELECT DISTINCT within.tile_id
        FROM ({}) within;
        """).format(within_query)
        log.debug(self.conn.print_query(query))
        resultset = self.conn.get_query(query)
        if reorder:
            shuffle(resultset)
        tiles = [tile[0] for tile in resultset]
        log.debug(f"Nr. of tiles in extent: {len(tiles)}")
        return tiles

    def tiles_in_index(self, tiles) -> List[str]:
        """Return the tile IDs that are present in the tile index."""
        query_params = {
            'tiles': sql.Literal(tiles),
            'index_': self.index.schema + self.index.table,
            'tile': self.index.field.tile.sqlid
        }
        query = sql.SQL("""
        SELECT DISTINCT {tile}
        FROM {index_}
        WHERE {tile} = ANY({tiles}::VARCHAR[])
        """).format(**query_params)
        log.debug(self.conn.print_query(query))
        in_index = [t[0] for t in self.conn.get_query(query)]
        diff = set(tiles) - set(in_index)
        if len(diff) > 0:
            log.warning(f"The provided tile IDs {diff} are not in the index, "
                        f"they are skipped.")
        return in_index

    def all_in_index(self) -> List[str]:
        """Get all tile IDs from the tile index."""
        query_params = {
            'index_': self.index.schema + self.index.table,
            'tile': self.index.field.tile.sqlid
        }
        query = sql.SQL("""
        SELECT DISTINCT {tile} FROM {index_}
        """).format(**query_params)
        log.debug(self.conn.print_query(query))
        return [t[0] for t in self.conn.get_query(query)]


class DbTilesAHN(DbTiles):
    """AHN tiles where the tile index is stored in PostgreSQL, the point cloud
    is stored in files on the file system."""

    def __init__(self, conn: db.Db, index_schema: db.Schema,
                 feature_schema: db.Schema, output=None):
        super().__init__(conn, index_schema, feature_schema, output)
        self.file_index = None

    def versions(self) -> List[int]:
        query_params = {
            'index_': self.index.schema + self.index.table,
            'version': self.index.field.version.sqlid
        }
        query = sql.SQL("""
        SELECT DISTINCT {version} FROM {index_};
        """).format(**query_params)
        log.debug(self.conn.print_query(query))
        r = []
        for row in self.conn.get_query(query):
            try:
                r.append(int(row[0]))
            except TypeError or ValueError:
                pass
        return r

    def _version_border(self) -> List:
        """Return a list of tiles that are on the border between two AHN
        versions."""
        query_params = {
            'tile': self.index.field.tile.sqlid,
            'borders': self.index.schema + self.index.borders
        }
        query = sql.SQL("""
        SELECT {tile} FROM {borders};
        """).format(**query_params)
        log.debug(self.conn.print_query(query))
        return [row[0] for row in self.conn.get_query(query)]

    def _version_not_border(self) -> dict:
        """Return a list of tiles that are not on the border between two AHN
        versions."""
        query_params = {
            'index_': self.index.schema + self.index.table,
            'borders': self.index.schema + self.index.borders,
            'tile': self.index.field.tile.sqlid
        }
        query = sql.SQL("""
            SELECT 
                sub.ahn_version,
                array_agg(sub.a_bladnr) AS tiles
            FROM
                (
                    SELECT
                        a.{tile} a_bladnr,
                        b.{tile} b_bladnr,
                        a.ahn_version
                    FROM
                        {index_} a
                    LEFT JOIN {borders} b ON
                        a.{tile} = b.{tile}
                ) sub
            WHERE 
                sub.b_bladnr IS NULL
            GROUP BY sub.ahn_version;
            """).format(**query_params)
        log.debug(self.conn.print_query(query))
        return {key:value for key, value in self.conn.get_query(query)}
        # return self.conn.get_dict(query)

    @staticmethod
    def create_file_index(directory_mapping: dict) -> dict:
        """Create an index of files in the given directories.

        Maps the location of the files to the tile IDs. This assumes that there
        is a tile index, and the content of each tile (the features) are stored
        in one file per tile. And the file names contain the corresponting
        tile ID.

        You can provide the ``directory_mapping`` as a sequence of directory
        configurations:

        .. code-block:: yaml

            directories:
                -   < directory_path >:
                        file_pattern: "<pattern>{tile}<pattern>"
                        priority: 1

        * ``directory_path`` is the path to the directory that stores the
            files

        * ``file_pattern`` the naming pattern for the files, that indicates how
            to match the file to the tile. Thus the file name must contain the tile
            ID and the place of the tile ID is marked with ``{tile}`` in the pattern.
            For example, the pattern ``C_{tile}.LAZ`` will match all of ``C_25gn1.LAZ,
            C_25GN2.LAZ, c_25Gn3.laz``, where ``25gn1, 25gn2, 25gn3`` are tile IDs.
            Matching is case insensitive.

        * ``priority`` sets the priority for a directory in case you have multiple
            directories. The lower number indicates higher priority, thus read it as
            "priority number-one, priority number-two etc.". This setting is
            useful lots of tiles in multiple versions, where the coverage of the
            versions partially overlap, and you want to use the tile of the latest
            version in the overlapping areas. In this case you would set the
            directory containing the files of the latest version to ``priority: 1``,
            and thus always the latest version of files will be used for each tile.

        :return: { tile_id: [ path/to/file ] }
        """
        if not directory_mapping:
            log.debug("directory_mapping is None")
            return None

        f_idx = {}
        priority = []
        file_index = {}
        # 'priority' is elevation:directories:<directory>:priority
        def get_priority(d):
            return d[1]['priority']

        dir_by_priority = sorted(directory_mapping.items(), key=get_priority)
        # 'file_pattern' is elevation: directories: < directory >: file_pattern
        for dir, properties in dir_by_priority:
            idx = {}
            file_pattern = properties['file_pattern']
            l = file_pattern[:file_pattern.find('{')]
            r = file_pattern[file_pattern.find('}') + 1:]
            regex = '(?<=' + l + ').*(?=' + r + ')'
            tile_pattern = re.compile(regex, re.IGNORECASE)
            for item in os.listdir(dir):
                path = os.path.join(dir, item)
                if os.path.isfile(path):
                    file_tile = tile_pattern.search(item)
                    if file_tile:
                        tile = file_tile.group(0).lower()
                        idx[tile] = [path]
            f_idx[dir] = idx
        for dir, properties in reversed(dir_by_priority):
            if len(priority) == 0:
                file_index = f_idx[dir]
            else:
                if priority[-1] == properties['priority']:
                    tiles = f_idx[dir].keys()
                    for t in tiles:
                        try:
                            file_index[t] += f_idx[dir][t]
                        except KeyError:
                            file_index[t] = f_idx[dir][t]
                else:
                    f = {**file_index, **f_idx[dir]}
                    file_index = f
            priority.append(properties['priority'])
        log.debug(f"File index: {file_index}")
        return file_index

    def match_feature_tile(self, feature_tile, idx_identical: bool=True):
        """Find the elevation tiles that match the footprint tile."""
        if idx_identical:
            query_params = {
                'index_': self.index.schema + self.index.table,
                'table_': self.index.table.sqlid,
                'tile_field': self.index.field.tile.sqlid,
                'tile': sql.Literal(feature_tile)
            }
            query = sql.SQL("""
                SELECT
                    {tile_field}
                    ,{table_}.ahn_version
                FROM
                    {index_}
                WHERE {tile_field} = {tile};
                """).format(**query_params)
            log.debug(self.conn.print_query(query))
            resultset = self.conn.get_query(query)
            tiles = {}
            for tile in resultset:
                tile_id = tile[0].lower()
                if tile[1]:
                    if tile_id not in tiles:
                        tiles[tile_id] = int(tile[1])
                    else:
                        log.error(f"Tile ID {tile_id} is duplicate")
                else:
                    log.warning(f"Tile {tile_id} ahn_version is NULL")
        else:
           raise NotImplementedError("Only identical feature and elevation tiles"
                                     " are implemented. "
                                     "See bag3d.batch3dfier.find_pc_tiles() for "
                                     "implementing this path.")
        return tiles


    def configure(self, tiles: List[str] = None, extent=None,
                      version: int=None, on_border: bool=False,
                      directory_mapping: dict=None):
            """Prepare the AHN tiles for processing.

            First `tiles` and `extent` are evaluated, then `version` and
            `on_border`. The arguments `version` and `border` are mutually exclusive

            :param tiles: See :meth:`.DbTiles.configure`
            :param extent: See :meth:`.DbTiles.configure`
            :param version: Limit the tiles to AHN provided version. This selection
                *excludes* the AHN version border. If `None` then no limitation.
            :param on_border: If `True` limit the tiles to the border of the two
                AHN version coverages. If `False`, exclude this border area. If
                `None`, no limitation.
            """
            super().configure(tiles=tiles, extent=extent)
            if on_border is None and version is None:
                log.info(f"{self.__class__.__name__} configuration done.")
            elif version is not None and on_border is False:
                versions = self.versions()
                if version not in versions:
                    raise ValueError(f"Version {version} is not in the index.")
                else:
                    tiles_per_version = self._version_not_border()
                    version_set = set(tiles_per_version[version])
                    process_set = set(self.to_process)
                    self.to_process = list(version_set.intersection(process_set))
                    file_index = self.create_file_index(directory_mapping)
                    self.file_index = {tile: file
                                       for tile, file in file_index.items()
                                       if tile in self.to_process}
                    log.info(f"{self.__class__.__name__} configuration done.")
            elif on_border:
                border_set = set(self._version_border())
                process_set = set(self.to_process)
                self.to_process = list(border_set.intersection(process_set))
                file_index = self.create_file_index(directory_mapping)
                self.file_index = {tile: file
                                   for tile, file in file_index.items()
                                   if tile in self.to_process}
                log.info(f"{self.__class__.__name__} configuration done.")
            else:
                raise AttributeError(
                    f"Unknown configuration tiles:{tiles}, extent:{extent}, "
                    f"version:{version}, on_border:{on_border}.")
