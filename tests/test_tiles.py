# -*- coding: utf-8 -*-

"""Tests for `.tileconfig` module."""

import os

import pytest
import yaml

from tile_processor import tileconfig, db


@pytest.fixture(scope='function')
def polygons(data_dir):
    yield {'file': os.path.join(data_dir, 'extent_small.geojson'),
           'ewkb': '010300002040710000010000000A000000DC5806A57984FD4047175D5475B01D41FEC869BE0583FD4062E2FD2847AF1D415FAB6787D87EFD40D24517BD20AE1D418C2EBAE89980FD4025A7F9FA6AAC1D41F17EE434E48AFD40F923A7597EAC1D41B0D5B3430B8AFD405A06A562CFAD1D411526DE8F028DFD40E3FDC8893BAF1D41D47CAD9E298CFD40CCA054383BB01D414A8589F71387FD401626DE2FB7B01D41DC5806A57984FD4047175D5475B01D41',
           'wkt': 'POLYGON ((120903.6027892562 486429.3323863637, 120880.3589876033 486353.7900309918, 120813.5330578512 486280.1846590909, 120841.6193181818 486170.7450929753, 121006.2629132231 486175.587551653, 120992.7040289256 486259.8463326447, 121040.1601239669 486350.8845557852, 121026.6012396694 486414.8050103306, 120945.2479338843 486445.7967458678, 120903.6027892562 486429.3323863637))'}


@pytest.fixture(scope='function')
def features_idx_sch(cfg_bag3d):
    """Schema of the features tile index"""
    return cfg_bag3d['features_tiles']


@pytest.fixture(scope='function')
def features_sch(cfg_bag3d):
    """Schema of the features"""
    return cfg_bag3d['features']


@pytest.fixture(scope='function')
def elevation_idx_sch(cfg_bag3d):
    """Schema for the AHN index"""
    return cfg_bag3d['elevation_tiles']


@pytest.fixture(scope='function')
def elevation_tiles(bag3d_db, elevation_idx_sch) -> tileconfig.DbTiles:
    return tileconfig.DbTiles(
        conn=bag3d_db,
        tile_index_schema=db.Schema(elevation_idx_sch)
    )

@pytest.fixture(scope='function')
def feature_tiles(bag3d_db, features_idx_sch, features_sch) -> tileconfig.DbTiles:
    return tileconfig.DbTiles(
        conn=bag3d_db,
        tile_index_schema=db.Schema(features_idx_sch),
        features_schema=db.Schema(features_sch)
    )


@pytest.fixture(scope='function')
def ahn_tiles(bag3d_db, elevation_tiles, feature_tiles):
    return tileconfig.DbTilesAHN(conn=bag3d_db,
                                elevation_tiles=elevation_tiles,
                                feature_tiles=feature_tiles,
                                output=None)

@pytest.fixture(scope='function')
def file_index_ahn(data_dir):
    dirs = {
        '25gn1_6': [
            'ahn/ahn3/c25gn1_6.laz'],
        '25gn1_15': [
            'ahn/ahn2/unit_25gn1_15.laz'],
        '25gn1_9': [
            'ahn/ahn3/c25gn1_9.laz'],
        '25gn1_8': [
            'ahn/ahn2/unit_25gn1_8.laz'],
        '25gn1_16': [
            'ahn/ahn2/unit_25gn1_16.laz'],
        '25gn1_13': [
            'ahn/ahn3/c25gn1_13.laz'],
        '25gn1_3': [
            'ahn/ahn3/c25gn1_3.laz'],
        '25gn1_10': [
            'ahn/ahn3/c25gn1_10.laz'],
        '25gn1_12': [
            'ahn/ahn2/unit_25gn1_12.laz'],
        '25gn1_7': [
            'ahn/ahn3/C25gn1_7.laz'],
        '25gn1_4': [
            'ahn/ahn3/c25gn1_4.laz'],
        '25gn1_14': [
            'ahn/ahn3/c25gn1_14.laz'],
        '25gn1_11': [
            'ahn/ahn2/unit_25gn1_11.laz'],
        '25gn1_1': [
            'ahn/ahn3/C25gn1_1.laz'],
        '25gn1_5': [
            'ahn/ahn3/c25gn1_5.laz'],
        '25gn1_2': [
            'ahn/ahn3/C25gn1_2.laz']
    }
    expectation = {}
    for tile,file in dirs.items():
        expectation[tile] = [os.path.join(data_dir,file[0])]
    return expectation


@pytest.fixture(scope='function')
def directory_mapping(data_dir):
    bag3d_cfg = os.path.join(data_dir, 'bag3d_config.yml')
    with open(bag3d_cfg, 'r') as fo:
        f = yaml.load(fo, yaml.FullLoader)

    directory_mapping = {}
    for mapping in f['elevation']['directories']:
        dir, value = mapping.popitem()
        abs_dir = os.path.join(data_dir, dir)
        directory_mapping[abs_dir] = value
    return directory_mapping


class TestInit:
    def test_init_dbtiles(self, output_obj, bag3d_db):
        tiles = tileconfig.DbTiles(
            conn=bag3d_db, tile_index_schema=None, features_schema=None,
            output=output_obj
        )

    def test_init_ahntiles(self, output_obj, bag3d_db, elevation_idx_sch,
                           features_idx_sch, features_sch):
        tiles = tileconfig.DbTilesAHN(conn=bag3d_db,
                                      elevation_tiles=elevation_idx_sch,
                                      feature_tiles=features_idx_sch)

class TestExtent:
    """Configure the feature tiles with the provided polygonal extent."""

    def test_read_extent(self, polygons):
        tiles = tileconfig.DbTiles(conn=None, tile_index_schema=None,
                                   features_schema=None)
        extent = polygons['file']
        poly, ewkb = tiles.read_extent(extent)
        assert ewkb == polygons['ewkb']
        assert poly.wkt == polygons['wkt']

    def test_within_extent(self, bag3d_db, polygons, features_idx_sch,
                            features_sch):
        """Tile selection with a polygon should return the tiles that
        intersect with the polygon."""
        tiles = tileconfig.DbTiles(bag3d_db,
                                   tile_index_schema=db.Schema(features_idx_sch),
                                   features_schema=db.Schema(features_sch))
        result = tiles.within_extent(polygons['ewkb'])
        if features_idx_sch['index']['table'] == 'bag_index_identical':
            expectation = {'25gn1_10', '25gn1_11', '25gn1_6', '25gn1_7'}
            assert set(result) == expectation
        elif features_idx_sch['index']['table'] == 'bag_index':
            expectation = {'u2', 'u5'}
            assert set(result) == expectation
        else:
            pytest.fail(msg=f"Unexpected features_tiles.index.table "
                            f"{features_idx_sch['index']['table']}",
                        pytrace=False)

    def test_invalid_params(self):
        with pytest.raises(ValueError):
            tiles = tileconfig.DbTiles(None, None, None)
            tiles.configure()

        with pytest.raises(ValueError):
            tiles = tileconfig.DbTiles(None, None, None)
            tiles.configure(extent='some_file', tiles=['all'])

class TestList:
    """Configure the feature tiles with the provided list of tile IDs."""

    def test_tiles_in_index(self, bag3d_db, features_idx_sch):
        if features_idx_sch['index']['table'] == 'bag_index_identical':
            to_process = ['25gn1_10', '25gn1_11', '25gn1_6', 'not_in_index']
            expectation = ['25gn1_10', '25gn1_11', '25gn1_6']
            tiles = tileconfig.DbTiles(bag3d_db,
                                       tile_index_schema=db.Schema(
                                           features_idx_sch),
                                       features_schema=None)
            result = tiles.tiles_in_index(to_process)
            assert set(result) == set(expectation)
        elif features_idx_sch['index']['table'] == 'bag_index':
            to_process = ['u1', 'u2', 'u5', 'not_in_index']
            expectation = ['u1', 'u2', 'u5']
            tiles = tileconfig.DbTiles(bag3d_db,
                                       tile_index_schema=db.Schema(
                                           features_idx_sch),
                                       features_schema=None)
            result = tiles.tiles_in_index(to_process)
            assert set(result) == set(expectation)
        else:
            pytest.fail(msg=f"Unexpected features_tiles.index.table "
                            f"{features_idx_sch['index']['table']}",
                        pytrace=False)

    def test_invalid_tiles(self, bag3d_db, features_idx_sch):
        to_process = ['bla', 'not_in_index']
        tiles = tileconfig.DbTiles(bag3d_db,
                                   tile_index_schema=db.Schema(features_idx_sch),
                                   features_schema=None)
        with pytest.raises(ValueError):
            tiles.with_list(tiles=to_process)

    def test_all_in_index(self, bag3d_db, features_idx_sch):
        if features_idx_sch['index']['table'] == 'bag_index_identical':
            expectation = ["25gn1_1", "25gn1_2", "25gn1_3", "25gn1_4",
                           "25gn1_5", "25gn1_6", "25gn1_7", "25gn1_8",
                           "25gn1_9", "25gn1_10", "25gn1_11", "25gn1_12",
                           "25gn1_13", "25gn1_14", "25gn1_15", "25gn1_16"]
            tiles = tileconfig.DbTiles(bag3d_db,
                                       tile_index_schema=db.Schema(
                                           features_idx_sch),
                                       features_schema=None)
            result = tiles.all_in_index()
            assert set(result) == set(expectation)
        elif features_idx_sch['index']['table'] == 'bag_index':
            expectation = ['u4', 'u1', 'u2', 'u3', 'u5', 'u6']
            tiles = tileconfig.DbTiles(bag3d_db,
                                       tile_index_schema=db.Schema(
                                           features_idx_sch),
                                       features_schema=None)
            result = tiles.all_in_index()
            assert set(result) == set(expectation)
        else:
            pytest.fail(msg=f"Unexpected features_tiles.index.table "
                            f"{features_idx_sch['index']['table']}",
                        pytrace=False)


class TestAHN:

    def test_versions(self, bag3d_db, elevation_tiles, feature_tiles):
        expectation = [2, 3]
        ahn_tiles = tileconfig.DbTilesAHN(conn=bag3d_db,
                                          elevation_tiles=elevation_tiles,
                                          feature_tiles=feature_tiles)
        result = ahn_tiles.versions()
        assert set(result) == set(expectation)

    def test_version_boundary(self, bag3d_db, elevation_tiles, feature_tiles):
        expectation = ["25gn1_3", "25gn1_4", "25gn1_6", "25gn1_7", "25gn1_10", "25gn1_14"]
        ahn_tiles = tileconfig.DbTilesAHN(conn=bag3d_db,
                                          elevation_tiles=elevation_tiles,
                                          feature_tiles=feature_tiles)
        result = ahn_tiles.version_boundary()
        assert set(result) == set(expectation)

    def test_version_not_boundary(self, bag3d_db, elevation_tiles, feature_tiles):
        table = feature_tiles.tile_index.index.table.string
        if table == 'bag_index_identical':
            expectation = {3: ['25gn1_1', '25gn1_2', '25gn1_5', '25gn1_9', '25gn1_13'],
                           2: ['25gn1_8', '25gn1_11', '25gn1_12', '25gn1_15', '25gn1_16']}
            ahn_tiles = tileconfig.DbTilesAHN(conn=bag3d_db,
                                              elevation_tiles=elevation_tiles,
                                              feature_tiles=feature_tiles)
            result = ahn_tiles.version_not_boundary()
            assert result == expectation
        elif table == 'bag_index':
            pytest.skip("No appropriate data for testing this branch")
        else:
            pytest.fail(msg=f"Unexpected features_tiles.index.table "
                            f"{table}",
                        pytrace=False)

    def test_configure_v3(self, ahn_tiles, directory_mapping):
        """The selected feature tiles should intersect only with the AHN3
        elevation tiles that are not on the boundary of AHN2-3."""
        table = ahn_tiles.feature_tiles.tile_index.index.table.string
        if table == 'bag_index_identical':
            expectation = ['25gn1_1', '25gn1_2', '25gn1_5', '25gn1_9', '25gn1_13']
            ahn_tiles.configure(tiles=['all'],
                                extent=None,
                                version=3,
                                on_border=False,
                                directory_mapping=directory_mapping)
            assert set(ahn_tiles.to_process) == set(expectation)
        elif table == 'bag_index':
            expectation = ['u1', 'u2', 'u4']
            ahn_tiles.configure(tiles=['all'],
                                extent=None,
                                version=3,
                                on_border=False,
                                directory_mapping=directory_mapping)
            assert set(ahn_tiles.to_process) == set(expectation)
        else:
            pytest.fail(msg=f"Unexpected features_tiles.index.table "
                            f"{table}",
                        pytrace=False)

    def test_configure_v2(self, ahn_tiles, directory_mapping):
        """The selected feature tiles should intersect only with the AHN2
        elevation tiles that are not on the boundary of AHN2-3."""
        table = ahn_tiles.feature_tiles.tile_index.index.table.string
        if table == 'bag_index_identical':
            expectation = ['25gn1_8', '25gn1_11', '25gn1_12', '25gn1_15',
                           '25gn1_16']
            ahn_tiles.configure(tiles=['all'],
                                extent=None,
                                version=2,
                                on_border=False,
                                directory_mapping=directory_mapping)
            assert set(ahn_tiles.to_process) == set(expectation)
        elif table == 'bag_index':
            expectation = ['u3', 'u5', 'u6']
            ahn_tiles.configure(tiles=['all'],
                                extent=None,
                                version=2,
                                on_border=False,
                                directory_mapping=directory_mapping)
            assert set(ahn_tiles.to_process) == set(expectation)
        else:
            pytest.fail(msg=f"Unexpected features_tiles.index.table "
                            f"{table}",
                        pytrace=False)

    def test_configure_v2_list(self, ahn_tiles, directory_mapping):
        table = ahn_tiles.feature_tiles.tile_index.index.table.string
        if table == 'bag_index_identical':
            expectation = ['25gn1_8', '25gn1_11']
            ahn_tiles.configure(
                tiles=['25gn1_8', '25gn1_11', '25gn1_2', '25gn1_5'],
                extent=None,
                version=2,
                on_border=False,
                directory_mapping=directory_mapping)
            assert set(ahn_tiles.to_process) == set(expectation)
        elif table == 'bag_index':
            expectation = ['u3']
            ahn_tiles.configure(
                tiles=['u1', 'u2', 'u3'],
                extent=None,
                version=2,
                on_border=False,
                directory_mapping=directory_mapping)
            assert set(ahn_tiles.to_process) == set(expectation)
        else:
            pytest.fail(msg=f"Unexpected features_tiles.index.table "
                            f"{table}",
                        pytrace=False)

    def test_configure_border(self, ahn_tiles, directory_mapping):
        table = ahn_tiles.feature_tiles.tile_index.index.table.string
        if table == 'bag_index_identical':
            expectation = ["25gn1_10", "25gn1_14"]
            ahn_tiles.configure(tiles=['25gn1_10', '25gn1_11', '25gn1_14', '25gn1_15'],
                                extent=None,
                                version=2,
                                on_border=True,
                                directory_mapping=directory_mapping)
            assert set(ahn_tiles.to_process) == set(expectation)
        elif table == 'bag_index':
            pytest.skip("No appropriate data for testing this branch")
        else:
            pytest.fail(msg=f"Unexpected features_tiles.index.table "
                            f"{table}",
                        pytrace=False)

    def test_configure_extent(self, ahn_tiles, polygons, directory_mapping):
        table = ahn_tiles.feature_tiles.tile_index.index.table.string
        if table == 'bag_index_identical':
            expectation = ["25gn1_10", "25gn1_6", "25gn1_7"]
            ahn_tiles.configure(tiles=None,
                                extent=polygons['file'],
                                version=2,
                                on_border=True,
                                directory_mapping=directory_mapping)
            assert set(ahn_tiles.to_process) == set(expectation)

            ahn_tiles.to_process = []
            expectation = ["25gn1_11"]
            ahn_tiles.configure(tiles=None,
                                extent=polygons['file'],
                                version=2,
                                on_border=False,
                                directory_mapping=directory_mapping)
            assert set(ahn_tiles.to_process) == set(expectation)

            ahn_tiles.to_process = []
            expectation = []
            ahn_tiles.configure(tiles=None,
                                extent=polygons['file'],
                                version=3,
                                on_border=False,
                                directory_mapping=directory_mapping)
            assert set(ahn_tiles.to_process) == set(expectation)
        elif table == 'bag_index':
            pytest.skip("No appropriate data for testing this branch")
        else:
            pytest.fail(msg=f"Unexpected features_tiles.index.table "
                            f"{table}",
                        pytrace=False)

    def test_create_file_index(self, directory_mapping, file_index_ahn):
        ft = tileconfig.DbTilesAHN(conn=None, elevation_tiles=None,
                                   feature_tiles=None, output=None)
        result = ft.create_elevation_file_index(directory_mapping)
        assert result == file_index_ahn

    def test_create_tile_views(self):
        """Tile views are not empty"""
        pytest.xfail("Not implemented")
