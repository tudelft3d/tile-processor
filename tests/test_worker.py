# -*- coding: utf-8 -*-
# Copyright:    (C)  by Balázs Dukai. All rights reserved.
# Begin:        2020-04-06
# Email:        b.dukai@tudelft.nl

"""Testing the worker module and the various Workers."""

import pytest
from pathlib import Path

from tile_processor import controller, recorder

@pytest.mark.integration_test
class TestExample:
    def test_example(self, data_dir):
        tiles = ["25gn1_2", "25gn1_7", "25gn1_6"]
        threads = 3
        fp = Path(data_dir) / "exampledb_config.yml"
        configuration = fp.open("r", encoding="utf-8")
        ctrl = controller.factory.create(
            "Example",
            configuration=configuration,
            threads=threads,
            monitor_log=recorder.configure_ressource_logging(),
            monitor_interval=60,
        )
        ctrl.configure(
            tiles=tiles, processor_key="threadprocessor", worker_key="Example"
        )
        ctrl.run()
        results = ctrl.run()
        for part, failed in results.items():
            assert len(failed) == 0

    def test_exampledb(self, data_dir):
        tiles = [
            "all",
        ]
        threads = 3
        fp = Path(data_dir) / "exampledb_config.yml"
        configuration = fp.open("r", encoding="utf-8")
        ctrl = controller.factory.create(
            "Example",
            configuration=configuration,
            threads=threads,
            monitor_log=None,
            monitor_interval=None,
        )
        ctrl.configure(
            tiles=tiles,
            processor_key="threadprocessor",
            worker_key="ExampleDb",
        )
        ctrl.run()
        results = ctrl.run()
        for part, failed in results.items():
            assert len(failed) == 0


@pytest.mark.integration_test
class TestThreedfier:
    def test_for_debug(self, cfg_ahn_abs):
        """Running 3dfier"""
        threads = 3
        tiles = [
            "all",
        ]
        threedfier_controller = controller.factory.create(
            "AHNboundary",
            configuration=cfg_ahn_abs,
            threads=threads,
            monitor_log=None,
            monitor_interval=None,
        )
        threedfier_controller.configure(
            tiles=tiles, processor_key="threadprocessor", worker_key="3dfier"
        )
        threedfier_controller.run()


@pytest.mark.integration_test
class TestGeoflow:
    def test_for_debug(self, cfg_ahn_geof):
        """Running LoD1.3 reconstruction"""
        threads = 2
        tiles = [
            "all",
        ]
        lod13_controller = controller.factory.create(
            "AHN",
            configuration=cfg_ahn_geof,
            threads=threads,
            monitor_log=recorder.configure_ressource_logging(),
            monitor_interval=10,
        )
        lod13_controller.configure(
            tiles=tiles, processor_key="threadprocessor", worker_key="LoD13"
        )
        lod13_controller.run()


class TestTileExporter:
    def test_for_debug(self, cfg_ahn_export, output_dir):
        """Running LoD1.3 reconstruction"""
        threads = 1
        tiles = ["u1", "u2"]
        lod13_controller = controller.factory.create(
            "AHN",
            configuration=cfg_ahn_export,
            threads=threads,
            monitor_log=recorder.configure_ressource_logging(),
            monitor_interval=10,
        )
        lod13_controller.configure(
            tiles=tiles,
            processor_key="threadprocessor",
            worker_key="TileExporter",
        )
        lod13_controller.cfg["config"]["out_dir"] = output_dir
        lod13_controller.run()
