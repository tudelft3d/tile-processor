# -*- coding: utf-8 -*-

"""Tile configuration."""


import logging
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
                 feature_schema: db.Schema):
        self.conn = conn
        self.to_process = []
        self.index = index_schema
        self.features = feature_schema

    def configure(self, tiles: List[str] = None, extent=None):
        """Configure the tiles for processing.

        You can provide either `tiles` or `extent`.

        :param tiles:
            A list of tile IDs, or a list with a single item `['all',]` for
            processing all tiles.
        :param extent:
            A polygon for selecting the tiles. The selected tiles are clipped by
             the `extent`. If the area of the `extent` is less than that of a
            single tile, than the clipped tile-parts within the extent are
            dissolved into a single tile.
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
    """AHN tiles where the tile index is stored in PostgreSQL."""

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

    def configure(self, tiles: List[str] = None, extent=None,
                  version: int=None, on_border: bool=False):
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
                log.info(f"{self.__class__.__name__} configuration done.")
        elif on_border:
            border_set = set(self._version_border())
            process_set = set(self.to_process)
            self.to_process = list(border_set.intersection(process_set))
            log.info(f"{self.__class__.__name__} configuration done.")
        else:
            raise AttributeError(
                f"Unknown configuration tiles:{tiles}, extent:{extent}, "
                f"version:{version}, on_border:{on_border}.")
