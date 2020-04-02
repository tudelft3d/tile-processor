#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `.controller` module."""

import os

import pytest

from tile_processor import controller


class TestConfgurationSchema:
    def test_schema(self, tests_dir):
        p = os.path.join(tests_dir, 'data', 'test_config_schema.yml')
        schema = controller.ConfigurationSchema()
        schema.register('test', p)
        assert 'test' in schema.db
        files = os.listdir(schema.dir)
        assert 'test_config_schema.yml' in files
        # Clean up
        schema.remove('test')
        assert 'test' not in schema.db
        files = os.listdir(schema.dir)
        assert 'test_config_schema.yml' not in files


@pytest.mark.parametrize('controller_key', controller.factory._controllers)
def test_factory(controller_key):
    controller.factory.create(
        controller_key,
        configuration=None,
        threads=None,
        monitor_log=None,
        monitor_interval=None,
        config_schema=None
    )


class TestController:
    def test_parse_configuration(self, data_dir):
        ctrl = controller.Controller(
            configuration=None,
            threads=None,
            monitor_interval=None,
            monitor_log=None,
            config_schema=None
        )
        fp = os.path.join(data_dir, 'exampledb_config.yml')
        configuration = open(fp, 'r', encoding='utf-8')
        cfg = ctrl.parse_configuration(
            configuration=configuration, threads=1, monitor_log=None,
            monitor_interval=None
        )
        assert len(cfg['config']) > 0
        assert 'database' in cfg['config']


@pytest.mark.integration_test
class TestExample:
    def test_example(self, data_dir):
        tiles = ['25gn1_2', '25gn1_7', '25gn1_6']
        threads=3
        fp = os.path.join(data_dir, 'exampledb_config.yml')
        configuration = open(fp, 'r', encoding='utf-8')
        ctrl = controller.factory.create('Example',
                                          configuration=configuration,
                                          threads=threads,
                                          monitor_log=None,
                                          monitor_interval=None
                                          )
        ctrl.configure(
            tiles=tiles,
            processor_key='threadprocessor',
            worker_key='Example'
        )
        ctrl.run()
        results = ctrl.run()
        for part, failed in results.items():
            assert len(failed) == 0

    def test_exampledb(self, data_dir):
        tiles = ['all',]
        threads=3
        fp = os.path.join(data_dir, 'exampledb_config.yml')
        configuration = open(fp, 'r', encoding='utf-8')
        ctrl = controller.factory.create('Example',
                                          configuration=configuration,
                                          threads=threads,
                                          monitor_log=None,
                                          monitor_interval=None
                                          )
        ctrl.configure(
            tiles=tiles,
            processor_key='threadprocessor',
            worker_key='ExampleDb'
        )
        ctrl.run()
        results = ctrl.run()
        for part, failed in results.items():
            assert len(failed) == 0


@pytest.mark.integration_test
class TestThreedfier:
    def test_for_debug(self, data_dir):
        threads=3
        tiles=['all',]
        fp = os.path.join(data_dir, 'bag3d_config_balazs.yml')
        configuration = open(fp, 'r', encoding='utf-8')
        threedfier_controller = controller.factory.create('AHN',
                                                          configuration=configuration,
                                                          threads=threads,
                                                          monitor_log=None,
                                                          monitor_interval=None
                                                          )
        threedfier_controller.configure(
            tiles=tiles,
            processor_key='threadprocessor',
            worker_key='3dfier'
        )
        threedfier_controller.run()
