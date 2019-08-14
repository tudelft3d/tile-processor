# -*- coding: utf-8 -*-

"""Database connection."""

import logging
import re
from typing import List, Tuple

import psycopg2
from psycopg2 import sql, extras, extensions

log = logging.getLogger(__name__)


class DB(object):
    """A database connection class.

    :raise: :class:`psycopg2.OperationalError`
    """

    def __init__(self, dbname, host, port, user, password=None):
        self.dbname = dbname
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        try:
            self.conn = psycopg2.connect(
                dbname=dbname, host=host, port=port, user=user,
                password=password
            )
            log.debug(f"Opened connection to {self.conn.get_dsn_parameters()}")
        except psycopg2.OperationalError:
            log.exception("I'm unable to connect to the database")
            raise

    def send_query(self, query: psycopg2.sql.Composable):
        """Send a query to the DB when no results need to return (e.g. CREATE).
        """
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(query)

    def get_query(self, query: psycopg2.sql.Composable) -> List[Tuple]:
        """DB query where the results need to return (e.g. SELECT)."""
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchall()

    def get_dict(self, query: psycopg2.sql.Composable) -> dict:
        """DB query where the results need to return as a dictionary."""
        with self.conn:
            with self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query)
                return cur.fetchall()

    def print_query(self, query: psycopg2.sql.Composable) -> str:
        """Format a SQL query for printing by replacing newlines and tab-spaces.
        """

        def repl(matchobj):
            if matchobj.group(0) == '    ':
                return ' '
            else:
                return ' '

        s = query.as_string(self.conn).strip()
        return re.sub(r'[\n    ]{1,}', repl, s)

    def vacuum(self, schema: str, table: str):
        """Vacuum analyze a table."""
        self.conn.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        schema = psycopg2.sql.Identifier(schema)
        table = psycopg2.sql.Identifier(table)
        query = psycopg2.sql.SQL("""
        VACUUM ANALYZE {schema}.{table};
        """).format(schema=schema, table=table)
        self.send_query(query)

    def vacuum_full(self):
        """Vacuum analyze the whole database."""
        self.conn.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        query = psycopg2.sql.SQL("VACUUM ANALYZE;")
        self.send_query(query)

    def check_postgis(self):
        """Create the PostGIS extension if not exists."""
        self.send_query("CREATE EXTENSION IF NOT EXISTS postgis;")

    def get_fields(self, schema, table):
        """List the fields in a table."""
        query = sql.SQL("SELECT * FROM {s}.{t} LIMIT 0;").format(
            s=sql.Identifier(schema), t=sql.Identifier(table))
        cols = self.get_query(query)
        yield [c[0] for c in cols]

    def close(self):
        """Close connection."""
        self.conn.close()
        log.debug("Closed database successfully")
