#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `.controller` module."""

import os

import pytest
import yaml

from tile_processor import controller

@pytest.fixture('module')
def tests_dir():
    yield os.path.abspath(os.path.dirname(__file__))

@pytest.fixture('module')
def root_dir():
    yield os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

@pytest.fixture('module')
def package_dir(root_dir):
    yield os.path.join(root_dir, 'tile_processor')


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
    def test_for_debug(self):
        threads = 3
        tiles = ['tile_1', 'tile_2', 'tile_3', 'tile_4', 'tile_5']
        configuration = {
            'cfg_3dfier': "config for 3dfier",
            'cfg_lod10': "config for the LoD1.0 reconstruction"
        }
        #
        template_controller = controller.factory.create('template')
        template_controller.configure(
            threads=threads,
            monitor_log=None,
            monitor_interval=5,
            tiles=tiles,
            processor_key='threadprocessor',
            configuration=configuration
        )
        template_controller.run()
