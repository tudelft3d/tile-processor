# -*- coding: utf-8 -*-

"""Tests for `.db` module."""

import pytest
from psycopg2 import OperationalError, sql

from tile_processor import db


class TestDB():
    """Testing config.db"""

    def test_failed_connection(self, bag3d_db):
        """Failed connection raises OperationalError"""
        with pytest.raises(OperationalError) as excinfo:
            # invalid dbname
            db.DB(dbname='invalid', host=bag3d_db.host, port=bag3d_db.port,
                  user=bag3d_db.user)
            assert 'database "invalid" does not exist' in str(excinfo.value)
        with pytest.raises(OperationalError) as excinfo:
            # invalid host
            db.DB(dbname=bag3d_db.dbname, host='invalid', port=bag3d_db.port,
                  user=bag3d_db.user)
            assert 'could not translate host name "invalid" to address' in str(excinfo.value)
        with pytest.raises(OperationalError) as excinfo:
            # invalid port
            db.DB(dbname=bag3d_db.dbname, host=bag3d_db.host, port=1,
                  user=bag3d_db.user)
            assert 'TCP/IP connections on port 1?' in str(excinfo.value)
        with pytest.raises(OperationalError) as excinfo:
            # invalid user
            db.DB(dbname=bag3d_db.dbname, host=bag3d_db.host,
                  port=bag3d_db.port, user='invalid')


class TestSchema:

    @pytest.fixture('class')
    def relations(self):
        relations = {'schema': 'tile_index',
                     'table': 'bag_index_test',
                     'fields': {
                         'geometry': 'geom',
                         'primary_key': 'id',
                         'unit_name': 'bladnr'}
                     }
        yield relations

    def test_init(self, relations):
        index = db.Schema(relations)
        assert index.schema.string == 'tile_index'
        assert index.schema.sqlid == sql.Identifier('tile_index')

    def test_concatenate(self, relations):
        index = db.Schema(relations)
        result = index.schema + index.table
        assert result == sql.Identifier('tile_index', 'bag_index_test')
