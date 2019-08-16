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


class DBTiles:
    """Configures the tiles and tile index that is stored in PostgreSQL."""

    def __init__(self, conn: db.DB, index_schema: db.Schema,
                 feature_schema: db.Schema):
        self.conn = conn
        self.to_process = []
        self.index = index_schema
        self.features = feature_schema

    def configure(self, tiles: List[str]=None, extent=None):
        """Configure the tiles for processing.

        :param tiles:
            A list of tile IDs, or a list with a single item `['all',]`.
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
        poly, ewkb = self.read_extent(extent)
        return self.within_extent(ewkb=ewkb)

    def _with_list(self, tiles) -> List:
        """Select tiles based on a list of tile IDs."""
        return []

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
        <http://postgis.net/docs/manual-2.5/ST_Within.html>`_ the extent.
        """
        query_params = {
            'features': self.features.schema + self.features.table,
            'feature_pk': self.features.table + self.features.field.pk,
            'feature_geom': self.features.table + self.features.field.geometry,
            'index_': self.index.schema + self.index.table,
            'ewkb': sql.Literal(ewkb)
        }
        query = sql.SQL("""
        SELECT {features}.*, idx.tile_id
        FROM {features} JOIN {index_} idx ON {feature_pk} = idx.gid
        WHERE st_within({feature_geom}, {ewkb}::geometry)
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
