# -*- coding: utf-8 -*-

"""Tests for `.tileconfig` module."""

import os

import pytest

from tile_processor import tileconfig
from tile_processor import db


@pytest.fixture("module")
def polygons(data_dir):
    yield {'file': os.path.join(data_dir, 'extent_small.geojson'),
           'ewkb': '010300002040710000010000000A000000DC5806A57984FD4047175D5475B01D41FEC869BE0583FD4062E2FD2847AF1D415FAB6787D87EFD40D24517BD20AE1D418C2EBAE89980FD4025A7F9FA6AAC1D41F17EE434E48AFD40F923A7597EAC1D41B0D5B3430B8AFD405A06A562CFAD1D411526DE8F028DFD40E3FDC8893BAF1D41D47CAD9E298CFD40CCA054383BB01D414A8589F71387FD401626DE2FB7B01D41DC5806A57984FD4047175D5475B01D41',
           'wkt': 'POLYGON ((120903.6027892562 486429.3323863637, 120880.3589876033 486353.7900309918, 120813.5330578512 486280.1846590909, 120841.6193181818 486170.7450929753, 121006.2629132231 486175.587551653, 120992.7040289256 486259.8463326447, 121040.1601239669 486350.8845557852, 121026.6012396694 486414.8050103306, 120945.2479338843 486445.7967458678, 120903.6027892562 486429.3323863637))'}


class TestInit:

    def test_init(self, bag3d_db):
        tiles = tileconfig.DBTiles(bag3d_db)

class TestExtent:

    def test_read_extent(self, polygons):
        tiles = tileconfig.DBTiles(None)
        extent = polygons['file']
        poly, ewkb = tiles.read_extent(extent)
        assert ewkb == polygons['ewkb']
        assert poly.wkt == polygons['wkt']

    def test_get_tiles_in_extent(self, bag3d_db, polygons):
        expectation = set(['25gn1_10', '25gn1_11', '25gn1_6', '25gn1_7'])
        sch = {'schema': 'tile_index',
               'table': 'bag_index',
               'field': {
                   'pk': 'id',
                   'geometry': 'geom',
                   'tile': 'unit'}}
        index_schema = db.Schema(sch)
        tiles = tileconfig.DBTiles(bag3d_db, index_schema=index_schema)
        result = tiles._get_tiles_in_extent(polygons['ewkb'])
        assert set(result) == expectation
