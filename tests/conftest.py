# -*- coding: utf-8 -*-

"""pytest configuration"""


import os
import pytest
import yaml
from pathlib import Path
from io import StringIO
from tile_processor import db, output

# ------------------------------------ add option for running the full test set
def pytest_addoption(parser):
    parser.addoption(
        "--integration-test",
        action="store_true",
        default=False,
        help="run integration tests",
    )
    parser.addoption(
        "--slow-integration-test",
        action="store_true",
        default=False,
        help="run slow integration tests",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--integration-test"):
        return
    if config.getoption("--slow-integration-test"):
        return
    skip_integration = pytest.mark.skip(
        reason="need --integration-test option to run"
    )
    skip_slow_integration = pytest.mark.skip(
        reason="need --slow-integration-test option to run"
    )
    for item in items:
        if "integration_test" in item.keywords:
            item.add_marker(skip_integration)
        if "slow_integration_test" in item.keywords:
            item.add_marker(skip_slow_integration)


# -------------------------------------------------------------------- testing DB
@pytest.fixture(scope="session")
def bag3d_db():
    dbs = db.Db(
        dbname="bag3d_db",
        host="localhost",
        port=5590,
        user="bag3d_tester",
        password="bag3d_test",
    )
    yield dbs
    dbs.close()


@pytest.fixture(scope="session")
def tests_dir():
    yield os.path.abspath(os.path.dirname(__file__))


@pytest.fixture(scope="session")
def data_dir():
    yield Path(Path(__file__).parent / "data").absolute()


@pytest.fixture(scope="function")
def output_dir(data_dir):
    outdir = Path(data_dir / "output")
    outdir.mkdir(exist_ok=True)
    yield outdir


@pytest.fixture(scope="function")
def output_obj(data_dir):
    outdir = Path(data_dir / "output")
    outdir.mkdir(exist_ok=True)
    return output.DirOutput(path=outdir)


@pytest.fixture(scope="session")
def root_dir():
    yield os.path.abspath(os.path.dirname(os.path.dirname(__file__)))


@pytest.fixture(scope="session")
def package_dir(root_dir):
    yield os.path.join(root_dir, "tile_processor")


## Configurations


@pytest.fixture(
    scope="session",
    params=[
        ("bag_tiles", "bag_index"),
        ("bag_tiles_identical", "bag_index_identical"),
    ],
    ids=["different_tiles", "identical_tiles"],
)
def cfg_bag3d(data_dir, request):
    """The YAML configuration file that is used for processing tiles with
    AHN elevation.
    bag_tiles: feature tiles have a different extent than elevation tiles
    bag_tiles_identical: features tiles have an identical extent to the
        elevation tiles
    """
    tile_boundaries = 0
    tile_index = 1
    with open(data_dir / "bag3d_config.yml", "r") as fo:
        cfg = yaml.full_load(fo)
        cfg["features_tiles"]["boundaries"]["table"] = request.param[
            tile_boundaries
        ]
        cfg["features_tiles"]["index"]["table"] = request.param[tile_index]
        return cfg


@pytest.fixture(scope="function")
def cfg_bag3d_path(data_dir):
    yield data_dir / "bag3d_config.yml"


@pytest.fixture(scope="function")
def cfg_example(data_dir):
    with open(data_dir / "exampledb_config.yml", "r") as fo:
        yield yaml.full_load(fo)


@pytest.fixture(scope="function")
def cfg_ahn_abs(cfg_bag3d, data_dir) -> StringIO:
    """Absolute paths of the AHN directories in the directory mapping of the
    configuration file
    """
    # Replace the relative AHN directory paths to absolute paths
    cfg_abs = cfg_bag3d
    for i, d in enumerate(cfg_bag3d["elevation"]["directories"]):
        ahn_path, mapping = list(d.items())[0]
        absp = data_dir / ahn_path
        cfg_abs["elevation"]["directories"][i] = {str(absp): mapping}
    yield StringIO(yaml.dump(cfg_abs))


@pytest.fixture(scope="function")
def cfg_ahn_geof(cfg_ahn_abs) -> StringIO:
    """Absolute paths of the AHN directories in the directory mapping of the
    configuration file
    """
    cfg = yaml.full_load(cfg_ahn_abs)
    # Replace the output
    outp = yaml.full_load(
        """
    prefix: lod13_
    database:
        dbname: bag3d_db
        host: localhost
        port: 5590
        user: bag3d_tester
        password: bag3d_test
        schema: out_schema
    """
    )
    cfg["output"] = outp
    cfg["path_executable"] = "/opt/geoflow/bin/geof"
    cfg[
        "path_flowchart"
    ] = "/home/balazs/Development/3dbag-tools/flowcharts/runner.json"
    cfg["doexec"] = True
    yield StringIO(yaml.dump(cfg))


@pytest.fixture(scope="function")
def cfg_ahn_export(cfg_ahn_abs) -> StringIO:
    """Absolute paths of the AHN directories in the directory mapping of the
    configuration file
    """
    cfg = yaml.full_load(cfg_ahn_abs)
    # Replace the output
    outp = yaml.full_load(
        """
    prefix: lod13_
    database:
        dbname: bag3d_db
        host: localhost
        port: 5590
        user: bag3d_tester
        password: bag3d_test
        schema: out_schema
    """
    )
    cfg["output"] = outp
    cfg["path_lasmerge"] = "/opt/LAStools/install/bin/lasmerge64"
    cfg["path_ogr2ogr"] = "/opt/gdal-2.4.4/install/bin/ogr2ogr"
    cfg["doexec"] = True
    yield StringIO(yaml.dump(cfg))
