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
