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


class TestTemplate:

    @pytest.mark.skip
    def test_for_debug(self):
        tiles = ['tile_1', 'tile_2', 'tile_3', 'tile_4', 'tile_5']
        configuration = {
            'cfg_3dfier': "config for 3dfier",
            'cfg_lod10': "config for the LoD1.0 reconstruction"
        }
        template_controller = controller.factory.create('template')
        template_controller.configure(
            threads=3,
            monitor_log=None,
            monitor_interval=5,
            tiles=tiles,
            processor_key='threadprocessor',
            configuration=configuration
        )
        results = template_controller.run()
        for part, failed in results.items():
            assert len(failed) == 0

    def test_configuration(self):
        pass


class TestThreedfier:

    def test_for_debug(self, data_dir):
        threads=3
        tiles=['all']
        fp = os.path.join(data_dir, 'bag3d_config_balazs.yml')
        configuration = open(fp, 'r', encoding='utf-8')
        threedfier_controller = controller.factory.create('AHN',
                                                          configuration=configuration,
                                                          threads=threads,
                                                          monitor_log=None,
                                                          monitor_interval=None
                                                          )
        threedfier_controller.configure(
            tiles=list(tiles),
            processor_key='threadprocessor',
            worker_key='3dfier'
        )
        threedfier_controller.run()
