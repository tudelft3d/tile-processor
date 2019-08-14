# -*- coding: utf-8 -*-

"""Tile configuration."""


import re
import logging
from typing import List

import psycopg2
from psycopg2 import extensions
import fiona
from shapely import geos
from shapely.geometry import shape


log = logging.getLogger(__name__)


class DbTiles:
    """Configures the tiles and tile index that is stored in PostgreSQL."""

    def __init__(self, conn: psycopg2.extensions.connection):
        self.conn = conn
        self.tiles = []
        self.extent = None

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
            self._with_extent(extent)
        elif tiles:
            self._with_list(tiles)
        else:
            raise AttributeError("Either 'tiles' or 'extent' is required.")

    def _with_extent(self, extent):
        """Select tiles based on a polygon."""

    def _with_list(self, tiles):
        """Select tiles based on a list of tile IDs."""

    def _read_extent(self):
        """Reads a polygon from a file and returns it as Shapely polygon and
        EWKB."""
        pattern = re.compile(r'(?<=epsg:)\d+', re.IGNORECASE)
        # Get clip polygon and set SRID
        with fiona.open(self.extent, 'r') as src:
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
