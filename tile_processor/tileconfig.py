# -*- coding: utf-8 -*-

"""Tile configuration."""


import re
import logging
from typing import List, Tuple
from random import shuffle

from psycopg2 import sql
import fiona
from shapely import geos
from shapely.geometry import shape, Polygon

from tile_processor import db

log = logging.getLogger(__name__)


class DBTiles:
    """Configures the tiles and tile index that is stored in PostgreSQL."""

    def __init__(self, conn: db.DB, index_schema: db.Schema):
        self.conn = conn
        self.to_process = []
        self.index = index_schema

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
        if extent:
            self.to_process = self._with_extent(extent)
        elif tiles:
            self.to_process = self._with_list(tiles)
        else:
            raise AttributeError("Either 'tiles' or 'extent' is required.")

    def _with_extent(self, extent) -> List:
        """Select tiles based on a polygon."""
        return []

    def _with_list(self, tiles) -> List:
        """Select tiles based on a list of tile IDs."""
        return []

    @staticmethod
    def read_extent(extent: str) -> Tuple[Polygon, str]:
        """Reads a polygon from a file and returns it as Shapely polygon and
        EWKB.

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

    def _get_tiles_in_extent(self, ewkb, reorder=True):
        """Get a list of tiles that overlap the extent."""
        ewkb_q = sql.Literal(ewkb)
        # TODO: user input for a.unit
        query = sql.SQL("""
        SELECT {table}.{field_idx_unit}
        FROM {schema}.{table}
        WHERE st_intersects({table}.{field_idx_geom}, {ewkb}::geometry);
        """).format(schema=self.index.schema.identifier,
                    table=self.index.table.identifier,
                    field_idx_unit=self.index.field.tile.identifier,
                    field_idx_geom=self.index.field.geometry.identifier,
                    ewkb=ewkb_q)
        resultset = self.conn.get_query(query)
        if reorder:
            shuffle(resultset)
        tiles = [tile[0] for tile in resultset]
        log.debug(f"Nr. of tiles in clip extent: {len(tiles)}")
        return tiles
