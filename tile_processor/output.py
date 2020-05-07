# -*- coding: utf-8 -*-

"""Output handling."""

import logging
from pathlib import Path
from typing import Mapping

from tile_processor.db import Db

log = logging.getLogger(__name__)


class Output:
    def __init__(self, dir: "DirOutput" = None, db: "DbOutput" = None, kwargs=None):
        if kwargs is None:
            kwargs = {}
        self.dir = dir
        self.db = db
        self.kwargs = kwargs


class DirOutput:
    """Output to a directory. Creates `path` if not exists."""

    def __init__(self, path: Path = None):
        self.path = path

    @property
    def path(self):
        return self.__path

    @path.setter
    def path(self, value):
        abs_p = Path(value).absolute()
        try:
            abs_p.mkdir(exist_ok=False)
            log.info(f"Created directory {abs_p}")
        except OSError:
            log.debug(f"Directory already exists, {abs_p}")
        self.__path = abs_p

    @path.deleter
    def path(self):
        try:
            self.path.rmdir()
        except OSError as e:
            log.exception(e)

    def join_path(self, path: str) -> Path:
        """Join the input with self and return a path."""
        return self.path / path


class DbOutput:
    """Write to a database."""

    def __init__(self, conn: Db, schema: str = None, table: str = None):
        self.conn = conn
        if self.conn is not None:
            # No need to keep this connection open, but its important to test if the
            # connection can be opened by Db
            self.conn.close()
        if schema:
            self.schema = schema
        elif self.conn.schema:
            self.schema = self.conn.schema
        else:
            self.schema = None
        self.table = table
        self.dsn = None

    @property
    def dsn(self):
        if self.__dsn is not None:
            return self.__dsn
        else:
            # Create the dsn
            _dsn = " ".join(
                [
                    f"PG:dbname={self.conn.dbname}",
                    f"host={self.conn.host}",
                    f"port={self.conn.port}",
                    f"user={self.conn.user}",
                ]
            )
            if self.conn.password is not None:
                _dsn = " ".join([_dsn, f"password={self.conn.password}"])
            if self.schema:
                _dsn = " ".join([_dsn, f"schemas={self.schema}"])
            if self.table:
                _dsn = " ".join([_dsn, f"tables={self.table}"])
            return _dsn

    @dsn.setter
    def dsn(self, value):
        self.__dsn = value

    @dsn.deleter
    def dsn(self):
        self.__dsn = None

    def with_table(self, table: str) -> str:
        """Returns a PostgreSQL DSN for GDAL with the table set to the value.
        Replaces the tables in the dsn if the table declaration is already part of it.
        """
        i = self.dsn.find("tables")
        if self.schema:
            _tbl = '.'.join([self.schema, table])
        else:
            _tbl = table
        if i >= 0:
            # Replace the tables specification
            return " ".join([self.dsn[:i], f"tables={_tbl}"])
        else:
            return " ".join([self.dsn, f"tables={_tbl}"])
