# -*- coding: utf-8 -*-

"""pytest configuration"""


import os
import pytest
from tile_processor import db

#-------------------------------------------------------------------- testing DB
@pytest.fixture(scope="session")
def bag3d_db(request):
    dbs = db.Db(dbname='bag3d_db', host='localhost', port=5590,
                user='bag3d_tester', password='bag3d_test')
    yield dbs
    dbs.close()

@pytest.fixture('session')
def tests_dir():
    yield os.path.abspath(os.path.dirname(__file__))

@pytest.fixture('session')
def data_dir():
    yield os.path.abspath(os.path.join(os.path.dirname(__file__), 'data'))

@pytest.fixture('session')
def root_dir():
    yield os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

@pytest.fixture('session')
def package_dir(root_dir):
    yield os.path.join(root_dir, 'tile_processor')
