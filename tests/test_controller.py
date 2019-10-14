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


class TestExample:

    @pytest.mark.long
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

    @pytest.mark.long
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

class TestThreedfier:

    @pytest.mark.long
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
