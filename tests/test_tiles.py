# -*- coding: utf-8 -*-

"""Tests for `.tileconfig` module."""

import os

import pytest

from tile_processor import tileconfig, db


@pytest.fixture('module')
def polygons(data_dir):
    yield {'file': os.path.join(data_dir, 'extent_small.geojson'),
           'ewkb': '010300002040710000010000000A000000DC5806A57984FD4047175D5475B01D41FEC869BE0583FD4062E2FD2847AF1D415FAB6787D87EFD40D24517BD20AE1D418C2EBAE89980FD4025A7F9FA6AAC1D41F17EE434E48AFD40F923A7597EAC1D41B0D5B3430B8AFD405A06A562CFAD1D411526DE8F028DFD40E3FDC8893BAF1D41D47CAD9E298CFD40CCA054383BB01D414A8589F71387FD401626DE2FB7B01D41DC5806A57984FD4047175D5475B01D41',
           'wkt': 'POLYGON ((120903.6027892562 486429.3323863637, 120880.3589876033 486353.7900309918, 120813.5330578512 486280.1846590909, 120841.6193181818 486170.7450929753, 121006.2629132231 486175.587551653, 120992.7040289256 486259.8463326447, 121040.1601239669 486350.8845557852, 121026.6012396694 486414.8050103306, 120945.2479338843 486445.7967458678, 120903.6027892562 486429.3323863637))'}


@pytest.fixture('module')
def idx_sch():
    """Schema of the tile index"""
    yield {'schema': 'bag_tiles',
           'table': 'index',
           'field': {
               'pk': 'gid',
               'geometry': 'geom',
               'tile': 'tile'}
           }


@pytest.fixture('module')
def features_sch():
    """Schema of the features"""
    yield {'schema': 'bagactueel',
           'table': 'pandactueelbestaand',
           'field': {
               'pk': 'gid',
               'geometry': 'geovlak',
               'tile': 'tile'}
           }


@pytest.fixture('module')
def ahn_sch():
    """Schema for the AHN index"""
    yield {'schema': 'tile_index',
           'table': 'ahn_index',
           'borders': 'border_tiles',
           'field': {
               'pk': 'gid',
               'geometry': 'geom',
               'tile': 'tile',
               'version': 'ahn_version'}
           }


class TestInit:

    def test_init(self, bag3d_db):
        tiles = tileconfig.DbTiles(bag3d_db, index_schema=None,
                                   feature_schema=None)

class TestExtent:

    def test_read_extent(self, polygons):
        tiles = tileconfig.DbTiles(conn=None, index_schema=None,
                                   feature_schema=None)
        extent = polygons['file']
        poly, ewkb = tiles.read_extent(extent)
        assert ewkb == polygons['ewkb']
        assert poly.wkt == polygons['wkt']

    def test_clip_to_extent(self, bag3d_db, polygons, idx_sch, features_sch):
        expectation = {'25gn1_10', '25gn1_11', '25gn1_6', '25gn1_7'}
        tiles = tileconfig.DbTiles(bag3d_db,
                                   index_schema=db.Schema(idx_sch),
                                   feature_schema=db.Schema(features_sch))
        result = tiles.within_extent(polygons['ewkb'])
        assert set(result) == expectation

    def test_config_extent(self, bag3d_db, polygons, idx_sch, features_sch):
        expectation = {'25gn1_10', '25gn1_11', '25gn1_6', '25gn1_7'}
        tiles = tileconfig.DbTiles(bag3d_db,
                                   index_schema=db.Schema(idx_sch),
                                   feature_schema=db.Schema(features_sch))
        tiles.configure(extent=polygons['file'])
        assert set(tiles.to_process) == expectation

    def test_invalid_params(self):
        with pytest.raises(AttributeError):
            tiles = tileconfig.DbTiles(None, None, None)
            tiles.configure()

        with pytest.raises(AttributeError):
            tiles = tileconfig.DbTiles(None, None, None)
            tiles.configure(extent='some_file', tiles=['all'])

class TestList:

    def test_tiles_in_index(self, bag3d_db, idx_sch):
        to_process = ['25gn1_10', '25gn1_11', '25gn1_6', 'not_in_index']
        expectation = ['25gn1_10', '25gn1_11', '25gn1_6']
        tiles = tileconfig.DbTiles(bag3d_db,
                                   index_schema=db.Schema(idx_sch),
                                   feature_schema=None)
        result = tiles.tiles_in_index(to_process)
        assert set(result) == set(expectation)

    def test_invalid_tiles(self, bag3d_db, idx_sch):
        to_process = ['bla', 'not_in_index']
        tiles = tileconfig.DbTiles(bag3d_db,
                                   index_schema=db.Schema(idx_sch),
                                   feature_schema=None)
        with pytest.raises(AttributeError):
            tiles.configure(tiles=to_process)

    def test_all_tiles(self, bag3d_db, idx_sch):
        expectation = ["25gn1_1","25gn1_2","25gn1_3","25gn1_4","25gn1_5","25gn1_6","25gn1_7","25gn1_8","25gn1_9","25gn1_10","25gn1_11","25gn1_12","25gn1_13","25gn1_14","25gn1_15","25gn1_16"]
        to_process = ['all',]
        tiles = tileconfig.DbTiles(bag3d_db,
                                   index_schema=db.Schema(idx_sch),
                                   feature_schema=None)
        tiles.configure(tiles=to_process)
        assert set(tiles.to_process) == set(expectation)

    def test_all_tiles(self, bag3d_db, idx_sch):
        expectation = ["25gn1_1","25gn1_2","25gn1_3","25gn1_4","25gn1_5","25gn1_6","25gn1_7","25gn1_8","25gn1_9","25gn1_10","25gn1_11","25gn1_12","25gn1_13","25gn1_14","25gn1_15","25gn1_16"]
        to_process = ['all']
        tiles = tileconfig.DbTiles(bag3d_db,
                                   index_schema=db.Schema(idx_sch),
                                   feature_schema=None)
        tiles.configure(tiles=to_process)
        assert set(tiles.to_process) == set(expectation)


class TestAHN:

    def test_versions(self, bag3d_db, ahn_sch):
        expectation = [2,3]
        ahn_tiles = tileconfig.DbTilesAHN(
            conn=bag3d_db,
            index_schema=db.Schema(ahn_sch),
            feature_schema=None
        )
        result = ahn_tiles.versions()
        assert set(result) == set(expectation)

    def test_version_border(self, bag3d_db, ahn_sch):
        expectation = ["25gn1_3", "25gn1_4", "25gn1_6", "25gn1_7", "25gn1_10", "25gn1_14"]
        ahn_tiles = tileconfig.DbTilesAHN(
            conn=bag3d_db,
            index_schema=db.Schema(ahn_sch),
            feature_schema=None
        )
        result = ahn_tiles._version_border()
        assert set(result) == set(expectation)

    def test_version_not_border(self, bag3d_db, ahn_sch):
        expectation = {3: ['25gn1_1', '25gn1_2', '25gn1_5', '25gn1_9', '25gn1_13'],
                       2: ['25gn1_8', '25gn1_11', '25gn1_12', '25gn1_15', '25gn1_16']}
        ahn_tiles = tileconfig.DbTilesAHN(
            conn=bag3d_db,
            index_schema=db.Schema(ahn_sch),
            feature_schema=None
        )
        result = ahn_tiles._version_not_border()
        assert set(result) == set(expectation)

    def test_configure_v3(self, bag3d_db, ahn_sch):
        ahn_tiles = tileconfig.DbTilesAHN(
            conn=bag3d_db,
            index_schema=db.Schema(ahn_sch),
            feature_schema=None
        )
        expectation = ['25gn1_1', '25gn1_2', '25gn1_5', '25gn1_9', '25gn1_13']
        ahn_tiles.configure(tiles=['all'],
                            extent=None,
                            version=3,
                            on_border=False)
        assert set(ahn_tiles.to_process) == set(expectation)

    def test_configure_v2(self, bag3d_db, ahn_sch):
        ahn_tiles = tileconfig.DbTilesAHN(
            conn=bag3d_db,
            index_schema=db.Schema(ahn_sch),
            feature_schema=None
        )
        expectation = ['25gn1_8', '25gn1_11', '25gn1_12', '25gn1_15', '25gn1_16']
        ahn_tiles.configure(tiles=['all'],
                            extent=None,
                            version=2,
                            on_border=False)
        assert set(ahn_tiles.to_process) == set(expectation)

    def test_configure_v2_list(self, bag3d_db, ahn_sch):
        ahn_tiles = tileconfig.DbTilesAHN(
            conn=bag3d_db,
            index_schema=db.Schema(ahn_sch),
            feature_schema=None
        )
        expectation = ['25gn1_8', '25gn1_11']
        ahn_tiles.configure(tiles=['25gn1_8', '25gn1_11', '25gn1_2', '25gn1_5'],
                            extent=None,
                            version=2,
                            on_border=False)
        assert set(ahn_tiles.to_process) == set(expectation)

    def test_configure_border(self, bag3d_db, ahn_sch):
        ahn_tiles = tileconfig.DbTilesAHN(
            conn=bag3d_db,
            index_schema=db.Schema(ahn_sch),
            feature_schema=None
        )
        expectation = ["25gn1_10", "25gn1_14"]
        ahn_tiles.configure(tiles=['25gn1_10', '25gn1_11', '25gn1_14', '25gn1_15'],
                            extent=None,
                            version=2,
                            on_border=True)
        assert set(ahn_tiles.to_process) == set(expectation)

    def test_configure_extent(self, bag3d_db, ahn_sch, polygons):
        ahn_tiles = tileconfig.DbTilesAHN(
            conn=bag3d_db,
            index_schema=db.Schema(ahn_sch),
            feature_schema=None
        )
        expectation = ["25gn1_10", "25gn1_6", "25gn1_7"]
        ahn_tiles.configure(tiles=None,
                            extent=polygons['file'],
                            version=2,
                            on_border=True)
        assert set(ahn_tiles.to_process) == set(expectation)

        expectation = ["25gn1_11"]
        ahn_tiles.configure(tiles=None,
                            extent=polygons['file'],
                            version=2,
                            on_border=False)
        assert set(ahn_tiles.to_process) == set(expectation)

        expectation = []
        ahn_tiles.configure(tiles=None,
                            extent=polygons['file'],
                            version=3,
                            on_border=False)
        assert set(ahn_tiles.to_process) == set(expectation)
