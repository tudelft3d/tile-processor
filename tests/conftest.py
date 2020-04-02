# -*- coding: utf-8 -*-

"""pytest configuration"""


import os
import pytest
from tile_processor import db

#------------------------------------ add option for running the full test set
def pytest_addoption(parser):
    parser.addoption("--integration-test", action="store_true",
                     default=False,
                     help="run integration tests")
    parser.addoption("--slow-integration-test", action="store_true",
                     default=False,
                     help="run slow integration tests")

def pytest_collection_modifyitems(config, items):
    if config.getoption("--integration-test"):
        return
    if config.getoption("--slow-integration-test"):
        return
    skip_integration = pytest.mark.skip(
        reason="need --integration-test option to run")
    skip_slow_integration = pytest.mark.skip(
        reason="need --slow-integration-test option to run")
    for item in items:
        if "integration_test" in item.keywords:
            item.add_marker(skip_integration)
        if "slow_integration_test" in item.keywords:
            item.add_marker(skip_slow_integration)

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
