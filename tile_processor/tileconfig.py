# -*- coding: utf-8 -*-

"""Tile configuration."""


import logging
import os
import re
from random import shuffle
from typing import Sequence, Tuple, Union, List, Mapping
from abc import ABC, abstractmethod

from psycopg2 import sql
from psycopg2 import Error as pgError

from tile_processor import db, output

log = logging.getLogger(__name__)


class Tiles(ABC):
    """Basic tile configuration"""

    def __init__(self, out: output.Output = None) -> None:
        """
        :param output: An Output object
        """
        self.to_process = []
        self.output = output

    @abstractmethod
    def configure(self) -> None:
        pass


class FileTiles(Tiles):
    """Configures the tiles and tile index that is stored in files."""

    def __init__(self, output: output.Output = None) -> None:
        """
        :param output: An Output object
        """
        super().__init__(out=output)

    def configure(self, tiles=Sequence[str]) -> None:
        self.to_process = tiles


class DbTiles(Tiles):
    """Configures the tiles and tile index that is stored in PostgreSQL."""

    def __init__(
        self,
        conn: db.Db,
        tile_index_schema: db.Schema = None,
        features_schema: db.Schema = None,
        output: output.Output = None,
    ):
        """

        :param conn: Database connection object
        :param tile_index_schema: Schema of the tile index of the features.
        :param features_schema: Schema of the `features`
        """
        super().__init__(out=output)
        self.conn = conn
        self.tile_index = tile_index_schema
        self.features = features_schema

    def configure(
        self, tiles: Sequence[str] = None, extent: str = None
    ) -> None:
        """Configure the tiles for processing.

        You can provide either `tiles` or `extent`.

        :param tiles: A list of tile IDs, or a list with a single item
            `['all',]` for processing all tiles.
        :param extent: Path to a file (GeoJSON), containing a single
            polygon. A polygon for selecting the tiles. The selected tiles
            are clipped by the `extent`. If the area of the `extent` is less
            than that of a single tile, than the clipped tile-parts within the
            extent are dissolved into a single tile.
        """
        if (extent and tiles) or ((not extent) and (not tiles)):
            raise ValueError("Provide either 'tiles' or 'extent'.")
        if extent:
            self.to_process = self.with_extent(extent)
        else:
            self.to_process = self.with_list(tiles)

    def with_extent(self, extent) -> List:
        """Select tiles based on a polygon."""
        log.info("Clipping the tiles to the extent.")
        poly, ewkb = self.read_extent(extent)
        return self.within_extent(ewkb=ewkb)

    @staticmethod
    def read_extent(extent: str) -> None:
        """Reads a polygon from a file and returns it as Shapely polygon and
        EWKB.

        :param extent: Path to a file (eg GeoJSON), contiaining a single polygon
        :return: A tuple of (Shapely Polygon, EWKB). If the extent doesn't have
            a CRS, then a WKB is returned instead of the EWKB.
        """
        raise NotImplementedError(
            "FIXME: reading geojson is superfluous, read WKT instead"
        )
        # FIXME: reading geojson is superfluous, read WKT instead
        # pattern = re.compile(r"(?<=epsg:)\d+", re.IGNORECASE)
        # # Get clip polygon and set SRID
        # with fiona.open(extent, "r") as src:
        #     epsg = pattern.search(src.crs["init"]).group()
        #     poly = shape(src[0]["geometry"])
        #     if epsg is None:
        #         log.warning(f"Did not find the EPSG code of the CRS: {src.crs}")
        #         wkb = poly.wkb_hex
        #         return poly, wkb
        #     else:
        #         # Change a the default mode to add this, if SRID is set
        #         geos.WKBWriter.defaults["include_srid"] = True
        #         # set SRID for polygon
        #         geos.lgeos.GEOSSetSRID(poly._geom, int(epsg))
        #         ewkb = poly.wkb_hex
        #         return poly, ewkb

    def within_extent(self, ewkb: str, reorder: bool = True) -> List[str]:
        """Get a list of tiles that are within the extent."""
        within_query = self.within_extent_subquery(ewkb)
        query = sql.SQL(
            """
        SELECT DISTINCT within.tile_id
        FROM ( {} ) AS within;
        """
        ).format(within_query)
        log.debug(self.conn.print_query(query))
        resultset = self.conn.get_query(query)
        if reorder:
            shuffle(resultset)
        tiles = [tile[0] for tile in resultset]
        log.debug(f"Nr. of tiles in extent: {len(tiles)}")
        return tiles

    def within_extent_subquery(self, ewkb: str) -> sql.Composed:
        """Return a query for the features that are `within
        <http://postgis.net/docs/manual-2.5/ST_Within.html>`_ the extent.

        If `self.features` is set, then select the tile IDs from the features
        that are `ST_Within()` the provided `ewkb` polygon.

        Else if `self.features` is None, then select the tile IDs whose
        boundaries intersect with the `ewkb` polygon.
        """
        if self.features:
            # Select the features within the provided EWKB polygon
            query_params = {
                "features": self.features.schema + self.features.table,
                "feature_pk": self.features.table + self.features.field.pk,
                "feature_geom": self.features.table
                + self.features.field.geometry,
                "tile_index": self.tile_index.index.schema
                + self.tile_index.index.table,
                "ewkb": sql.Literal(ewkb),
                "tile": self.tile_index.index.table
                + self.tile_index.index.field.tile,
                "index_pk": self.tile_index.index.table
                + self.tile_index.index.field.pk,
            }
            query = sql.SQL(
                """
            SELECT {features}.*, {tile} AS tile_id
            FROM {features} JOIN {tile_index} ON {feature_pk} = {index_pk}
            WHERE st_within( {feature_geom}, {ewkb}::geometry )
            """
            ).format(**query_params)
        else:
            query_params = {
                "tile_boundaries": self.tile_index.boundaries.schema
                + self.tile_index.boundaries.table,
                "ewkb": sql.Literal(ewkb),
                "tile": self.tile_index.boundaries.table
                + self.tile_index.boundaries.field.tile,
                "boundary_geom": self.tile_index.boundaries.table
                + self.tile_index.boundaries.field.geometry,
            }
            query = sql.SQL(
                """
            SELECT {tile} AS tile_id
            FROM {tile_boundaries}
            WHERE st_intersects( {boundary_geom}, {ewkb}::geometry )
            """
            ).format(**query_params)
        return query

    def with_list(self, tiles) -> List:
        """Select tiles based on a list of tile IDs."""
        if ["all",] == tiles:
            log.info("Getting all tiles from the index.")
            in_index = self.all_in_index()
        else:
            log.info("Verifying if the provided tiles are in the index.")
            in_index = self.tiles_in_index(tiles)
        if len(in_index) == 0:
            raise ValueError(
                "None of the provided tiles are present in the" " index."
            )
        else:
            return in_index

    def all_in_index(self) -> List[str]:
        """Get all tile IDs from the tile index."""
        query_params = {
            "index_": self.tile_index.boundaries.schema
            + self.tile_index.boundaries.table,
            "tile": self.tile_index.boundaries.field.tile.sqlid,
        }
        query = sql.SQL(
            """
        SELECT DISTINCT {tile} FROM {index_}
        """
        ).format(**query_params)
        log.debug(self.conn.print_query(query))
        return [t[0] for t in self.conn.get_query(query)]

    def tiles_in_index(self, tiles) -> List[str]:
        """Return the tile IDs that are present in the tile index."""
        query_params = {
            "tiles": sql.Literal(tiles),
            "index_": self.tile_index.boundaries.schema
            + self.tile_index.boundaries.table,
            "tile": self.tile_index.boundaries.field.tile.sqlid,
        }
        query = sql.SQL(
            """
        SELECT DISTINCT {tile}
        FROM {index_}
        WHERE {tile} = ANY( {tiles} )
        """
        ).format(**query_params)
        log.debug(self.conn.print_query(query))
        in_index = [t[0] for t in self.conn.get_query(query)]
        diff = set(tiles) - set(in_index)
        if len(diff) > 0:
            log.warning(
                f"The provided tile IDs {diff} are not in the index, "
                f"they are skipped."
            )
        return in_index


class DbTilesAHN(Tiles):
    """AHN tiles where the tile index is stored in PostgreSQL, the point cloud
    is stored in files on the file system."""

    def __init__(
        self,
        conn: db.Db,
        elevation_tiles: DbTiles,
        feature_tiles: DbTiles,
        output: output.Output = None,
    ):
        """
        :param conn:
        :param elevation_tiles:
        :param feature_tiles:
        :param output:
        """
        # assert isinstance(conn, db.Db), "conn must be a Db object"
        # assert isinstance(elevation_tiles, DbTiles), "elevation_tiles must be a DbTiles object"
        # assert isinstance(feature_tiles, DbTiles), "feature_tiles must be a DbTiles object"
        super().__init__(out=output)
        self.conn = conn
        self.elevation_tiles = elevation_tiles
        self.feature_tiles = feature_tiles
        self.elevation_file_index = None  # { feature tile ID: [ (matching AHN file path, AHN version), ... ] }
        self.feature_views = None

    def configure(
        self,
        tiles: List[str] = None,
        extent: str = None,
        version: int = None,
        on_border: bool = False,
        directory_mapping: dict = None,
        tin: bool = False,
    ):
        """Prepare the AHN tiles for processing.

            First `tiles` and `extent` are evaluated, then `version` and
            `on_border`. The arguments `version` and `on_boundary` are mutually exclusive.

            The final list of tiles to process is the intersection of the
            selected feature tiles AND the elevation tiles for the provided
            version.

            :param tiles: See :meth:`.DbTiles.configure`
            :param extent: See :meth:`.DbTiles.configure`
            :param version: Limit the tiles to AHN provided version. This selection
                *excludes* the AHN version boundary. If `None` then no limitation.
            :param on_border: If `True` limit the tiles to the boundary of the two
                AHN version coverages. If `False`, exclude this boundary area. If
                `None`, no limitation.
            """
        self.feature_views = dict()
        # Select the tiles of the footprints to be processed with either a
        # list of tile IDs or a polygon extent
        self.feature_tiles.configure(tiles=tiles, extent=extent)
        # Find the available AHN files for each feature tile
        self.elevation_file_index = dict()
        elevation_file_paths = self.create_elevation_file_index(
            directory_mapping
        )
        # This below is the meat of this class. It further configures the
        # tile list by selecting the latest AHN version that is available
        # on the filesystem
        if version is None and (on_border is None or on_border is False):
            self.to_process = self.feature_tiles.to_process
            for tile in self.to_process:
                elevation_match = self.match_elevation_tile(
                    feature_tile=tile, idx_identical=False
                )
                paths = []
                for ahn_id, ahn_version in elevation_match.items():
                    if ahn_id in elevation_file_paths:
                        paths.extend(
                            (p, ahn_version)
                            for p in elevation_file_paths[ahn_id]
                        )
                    else:
                        log.debug(
                            f"File matching the AHN ID {ahn_id} not found"
                        )
                self.elevation_file_index[tile] = paths
                # Create tile views
                self.feature_views[tile] = self.create_tile_view(tile, tin=tin)

            del paths, elevation_file_paths, elevation_match
            log.info(f"{self.__class__.__name__} configuration done.")
        elif version is not None and on_border is False:
            versions = self.versions()
            if version not in versions:
                raise ValueError(f"AHN version {version} is not in the index.")
            else:
                tiles_per_version = self.version_not_boundary()
                if len(tiles_per_version) > 0:
                    version_set = set(tiles_per_version[version])
                    process_set = set(self.feature_tiles.to_process)
                    self.to_process = list(
                        version_set.intersection(process_set)
                    )
                    for tile in self.to_process:
                        elevation_match = self.match_elevation_tile(
                            feature_tile=tile, idx_identical=False
                        )
                        paths = []
                        for ahn_id, ahn_version in elevation_match.items():
                            paths.extend(
                                (p, ahn_version)
                                for p in elevation_file_paths[ahn_id]
                            )
                        self.elevation_file_index[tile] = paths
                        # Create tile views
                        self.feature_views[tile] = self.create_tile_view(
                            tile, tin=tin
                        )
                    del paths, elevation_file_paths, elevation_match
                else:
                    log.warning(
                        f"Did not find any feature tiles for "
                        f"AHN version {version}"
                    )
                log.info(f"{self.__class__.__name__} configuration done.")
        elif on_border:
            border_set = set(self.version_boundary())
            process_set = set(self.feature_tiles.to_process)
            self.to_process = list(border_set.intersection(process_set))
            for tile in self.to_process:
                elevation_match = self.match_elevation_tile(
                    feature_tile=tile, idx_identical=False
                )
                paths = []
                for ahn_id, ahn_version in elevation_match.items():
                    paths.extend(
                        (p, ahn_version) for p in elevation_file_paths[ahn_id]
                    )
                self.elevation_file_index[tile] = paths
                # Create tile views
                self.feature_views[tile] = self.create_tile_view(tile, tin=tin)
            del paths, elevation_file_paths, elevation_match
            log.info(f"{self.__class__.__name__} configuration done.")
        else:
            raise AttributeError(
                f"Unknown configuration tiles:{tiles}, extent:{extent}, "
                f"version:{version}, on_border:{on_border}."
            )

    @staticmethod
    def create_elevation_file_index(directory_mapping: Mapping) -> dict:
        """Create an index of files in the given directories.

        Maps the location of the files to the tile IDs. This assumes that there
        is a tile index, and the content of each tile (the features) are stored
        in one file per tile. And the file names contain the corresponding
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

        :return: { tile_id: [ path/to/file, ... ] }
        """
        if not directory_mapping:
            log.debug("directory_mapping is None")
            return None

        f_idx = {}
        priority = []
        file_index = {}
        # 'priority' is elevation:directories:<directory>:priority
        def get_priority(d):
            return d[1]["priority"]

        dir_by_priority = sorted(directory_mapping.items(), key=get_priority)
        # 'file_pattern' is elevation: directories: < directory >: file_pattern
        for dir, properties in dir_by_priority:
            idx = {}
            file_pattern = properties["file_pattern"]
            l = file_pattern[: file_pattern.find("{")]
            r = file_pattern[file_pattern.find("}") + 1 :]
            regex = "(?<=" + l + ").*(?=" + r + ")"
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
                if priority[-1] == properties["priority"]:
                    tiles = f_idx[dir].keys()
                    for t in tiles:
                        try:
                            file_index[t] += f_idx[dir][t]
                        except KeyError:
                            file_index[t] = f_idx[dir][t]
                else:
                    f = {**file_index, **f_idx[dir]}
                    file_index = f
            priority.append(properties["priority"])
        log.debug(f"File index length: {len(file_index)}")
        return file_index

    def versions(self) -> List[int]:
        """Get the AHN versions from the elevation tile index.

        :returns: List of version numbers
        """
        query_params = {
            "index_": self.elevation_tiles.tile_index.boundaries.schema
            + self.elevation_tiles.tile_index.boundaries.table,
            "version": self.elevation_tiles.tile_index.boundaries.field.version.sqlid,
        }
        query = sql.SQL(
            """
        SELECT DISTINCT {version} FROM {index_};
        """
        ).format(**query_params)
        log.debug(self.conn.print_query(query))
        r = []
        for row in self.conn.get_query(query):
            try:
                r.append(int(row[0]))
            except TypeError or ValueError:
                pass
        return r

    def version_boundary(self) -> List[str]:
        """Return a list of elevation tile IDs that are on the boundary
        of two AHN versions.

        :returns: List of elevation tile IDs
        """
        query_params = {
            "tile": self.elevation_tiles.tile_index.boundaries.field.tile.sqlid,
            "borders": self.elevation_tiles.tile_index.boundaries.schema
            + self.elevation_tiles.tile_index.boundaries.borders,
        }
        query = sql.SQL(
            """
        SELECT {tile} FROM {borders};
        """
        ).format(**query_params)
        log.debug(self.conn.print_query(query))
        return [row[0] for row in self.conn.get_query(query)]

    # def feature_tile_per_ahn_version(self)
    def version_not_boundary(self) -> Mapping[str, List[str]]:
        """Return a list of feature tile IDs that are not on the boundary of
        two different AHN versions.

        :returns: { AHN version: [ feature tile IDs ] }
        """
        query_params = {
            "index_": self.elevation_tiles.tile_index.boundaries.schema
            + self.elevation_tiles.tile_index.boundaries.table,
            "boundary": self.elevation_tiles.tile_index.boundaries.schema
            + self.elevation_tiles.tile_index.boundaries.borders,
            "tile": self.elevation_tiles.tile_index.boundaries.field.tile.sqlid,
            "geom": self.elevation_tiles.tile_index.boundaries.field.geometry.sqlid,
            "version": self.elevation_tiles.tile_index.boundaries.field.version.sqlid,
            "features_index": self.feature_tiles.tile_index.boundaries.schema
            + self.feature_tiles.tile_index.boundaries.table,
            "ft_geom": self.feature_tiles.tile_index.boundaries.field.geometry.sqlid,
            "ft_tile": self.feature_tiles.tile_index.boundaries.field.tile.sqlid,
        }

        query = sql.SQL(
            """
        WITH ahn_versions AS (
            SELECT
                sub.ahn_version,
                ST_UnaryUnion(ST_Collect(sub.geom)) geom
            FROM
                (
                    SELECT
                        a.{tile} a_bladnr,
                        b.{tile} b_bladnr,
                        a.{version} ahn_version,
                        a.{geom} geom
                    FROM
                        {index_} a
                    LEFT JOIN {boundary} b ON
                        a.{tile} = b.{tile}
                ) sub
            WHERE
                sub.b_bladnr IS NULL
            GROUP BY sub.ahn_version
        )
        SELECT 
            ahn_versions.ahn_version, 
            array_agg(bt.{ft_tile}) AS tiles
        FROM ahn_versions, {features_index} bt 
        WHERE ST_Relate(ahn_versions.geom, bt.{ft_geom}, '212101212')
           OR ST_Covers(ahn_versions.geom, bt.{ft_geom})
        GROUP BY ahn_versions.ahn_version;
        """
        ).format(**query_params)

        log.debug(self.conn.print_query(query))
        return {key: value for key, value in self.conn.get_query(query)}

    def match_elevation_tile(
        self, feature_tile: str, idx_identical: bool = True
    ):
        """Find the elevation tiles that match the footprint tile.

        :param feature_tile: ID of the feature tile
        :param idx_identical: If **True**, elevation and feature tiles are
            matched on IDs without any spatial comparison. If **False**,
            elevation and feature tiles are matched with an intersection check.
        """
        if idx_identical:
            query_params = {
                "elevation_boundaries": self.elevation_tiles.tile_index.boundaries.schema
                + self.elevation_tiles.tile_index.boundaries.table,
                "elevation_tiles": self.elevation_tiles.tile_index.boundaries.field.tile.sqlid,
                "elevation_version": self.elevation_tiles.tile_index.boundaries.field.version.sqlid,
                "feature_tile": sql.Literal(feature_tile),
            }
            query = sql.SQL(
                """
            SELECT
                {elevation_tiles},
                {elevation_version}
            FROM
                {elevation_boundaries}
            WHERE {elevation_tiles} = {feature_tile};
            """
            ).format(**query_params)
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
            query_params = {
                "elevation_boundaries": self.elevation_tiles.tile_index.boundaries.schema
                + self.elevation_tiles.tile_index.boundaries.table,
                "elevation_tiles": self.elevation_tiles.tile_index.boundaries.field.tile.sqlid,
                "elevation_version": self.elevation_tiles.tile_index.boundaries.field.version.sqlid,
                "elevation_geom": self.elevation_tiles.tile_index.boundaries.field.geometry.sqlid,
                "features_boundaries": self.feature_tiles.tile_index.boundaries.schema
                + self.feature_tiles.tile_index.boundaries.table,
                "features_tiles": self.feature_tiles.tile_index.boundaries.field.tile.sqlid,
                "features_geom": self.feature_tiles.tile_index.boundaries.field.geometry.sqlid,
                "feature_tile": sql.Literal(feature_tile),
            }

            query = sql.SQL(
                """
            SELECT
                e.{elevation_tiles},
                e.{elevation_version}
            FROM
               {elevation_boundaries} e,
               {features_boundaries} f
            WHERE f.{features_tiles} = {feature_tile}
              AND ST_Intersects( e.{elevation_geom}, f.{features_geom} );
            """
            ).format(**query_params)
            log.debug(self.conn.print_query(query))
            resultset = self.conn.get_query(query)
            tiles = {}
            for tile in resultset:
                tile_id = tile[0].lower()
                if tile[1]:
                    if id not in tiles:
                        tiles[tile_id] = int(tile[1])
                    else:
                        log.error(f"Tile ID {tile_id} is duplicate")
                else:
                    log.warning(f"Tile {tile_id} ahn_version is NULL")
        return tiles

    def create_tile_view(
        self, feature_tile: str, tin: bool = False
    ) -> Union[None, str]:
        """Create a temporary view with a single tile polygon for GDAL to
        connect to.

        Otherwise it would be required to manually create a view or table for
        each tile polygon.

        .. note:: The rationale of this method is to keep the feature tiles in
            single table instead of having to manually create a table/view for
            each tile separately. Especially in case of TINs where the tile
            polygon IS the feature itself.

        :returns: The schema and view name of the created view
        """
        # FIXME: this should be done better than passing tin-switch as a parameter
        view = "_" + feature_tile
        if tin:
            raise NotImplementedError(
                "Balázs review this branch and make sure it works after the refactoring!"
            )
            view_sql = sql.Identifier(view)
            query_params = {
                "view": view_sql,
                "feature_table": self.feature_tiles.tile_index.boundaries.schema
                + self.feature_tiles.tile_index.boundaries.table,
                "uniqueid": self.feature_tiles.tile_index.boundaries.field.uniqueid.sqlid,
                "tile": sql.Literal(feature_tile),
            }
            query = sql.SQL(
                """
            CREATE OR REPLACE VIEW {view}
            AS SELECT * FROM {feature_table} WHERE {uniqueid} = {tile};
            """
            ).format(**query_params)
            log.debug(self.conn.print_query(query))
        else:
            view_sql = sql.Identifier(
                self.feature_tiles.features.schema.string, view
            )
            query_params = {
                "view": view_sql,
                "features": self.feature_tiles.features.schema
                + self.feature_tiles.features.table,
                "f_pk": self.feature_tiles.features.field.pk.sqlid,
                "features_index": self.feature_tiles.tile_index.index.schema
                + self.feature_tiles.tile_index.index.table,
                "fi_pk": self.feature_tiles.tile_index.index.field.pk.sqlid,
                "fi_tileid": self.feature_tiles.tile_index.index.field.tile.sqlid,
                "tile": sql.Literal(feature_tile),
            }
            query = sql.SQL(
                """
            CREATE OR REPLACE VIEW {view}
            AS SELECT f.* 
            FROM {features} f 
                JOIN {features_index} fi ON f.{f_pk} = fi.{fi_pk} 
            WHERE fi.{fi_tileid} = {tile};
            """
            ).format(**query_params)
            log.debug(self.conn.print_query(query))
        try:
            self.conn.send_query(query)
        except pgError as e:
            log.error(f"{e.pgcode}\t{e.pgerror}")
            return None
        return view
