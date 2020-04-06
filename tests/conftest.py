# -*- coding: utf-8 -*-

"""pytest configuration"""


import os
import pytest
import yaml
from pathlib import Path
from io import StringIO
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
    yield Path(Path(__file__).parent / 'data').absolute()

@pytest.fixture(scope='function')
def output_dir(data_dir):
    outdir = Path(data_dir / 'output')
    outdir.mkdir(exist_ok=True)
    yield outdir

@pytest.fixture('session')
def root_dir():
    yield os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

@pytest.fixture('session')
def package_dir(root_dir):
    yield os.path.join(root_dir, 'tile_processor')

## Configurations

@pytest.fixture(scope='function')
def cfg_bag3d(data_dir):
    with open(data_dir / 'bag3d_config.yml', 'r') as fo:
        yield yaml.load(fo, Loader=yaml.FullLoader)

@pytest.fixture(scope='function')
def cfg_bag3d_path(data_dir):
    yield data_dir / 'bag3d_config.yml'

@pytest.fixture(scope='function')
def cfg_example(data_dir):
    with open(data_dir / 'exampledb_config.yml', 'r') as fo:
        yield yaml.load(fo, Loader=yaml.FullLoader)

@pytest.fixture(scope='function')
def cfg_ahn_abs(cfg_bag3d, data_dir) -> StringIO:
    """Absolute paths of the AHN directories in the directory mapping of the
    configuration file
    """
    # Replace the relative AHN directory paths to absolute paths
    cfg_abs = cfg_bag3d
    for i, d in enumerate(cfg_bag3d['elevation']['directories']):
        ahn_path, mapping = list(d.items())[0]
        absp = data_dir / ahn_path
        cfg_abs['elevation']['directories'][i] = {str(absp): mapping}
    cfg_abs_stream = yaml.dump(cfg_abs)
    yield StringIO(cfg_abs_stream)
