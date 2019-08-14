#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `.controller` module."""

import os

import pytest
import logging

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
