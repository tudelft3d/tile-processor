#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `tile_processor` package."""

import pytest
import logging

from tile_processor import processor

@pytest.fixture('module')
def generate_sample_processor():
    def _generate(worker):
        tiles = ['tile_1', 'tile_2', 'tile_3', 'tile_4', 'tile_5']
        args = {'arg1': 'argument 1', 'arg2': 'argument 2'}
        expectation = {'tile_1': True,
                       'tile_2': True,
                       'tile_3': True,
                       'tile_4': True,
                       'tile_5': True}
        threadprocessor = processor.factory.create(
            'threadprocessor', name='test')
        threadprocessor.configure(
            threads=3,
            monitor_log=None,
            monitor_interval=5,
            tiles=tiles,
            worker=worker,
            config=args)
        return threadprocessor, expectation
    return _generate

class TestThreadProcessor:


    def test_processor_raise_exception(self, generate_sample_processor):
        def sample_worker(arg0, arg1=None, arg2=None):
            print(arg0, arg1, arg2)
            return True
        threadprocessor, expectation = generate_sample_processor(sample_worker)
        res = threadprocessor._process()
        with pytest.raises(TypeError):
            result = {tile:r for tile,r in res}

    def test_process(self, generate_sample_processor):
        def sample_worker(arg1, arg2, **kwargs):
            print(f"arg1={arg1}, arg2={arg2}, kwargs={kwargs}")
            return True
        threadprocessor, expectation = generate_sample_processor(sample_worker)
        res = threadprocessor._process()
        result = {tile: r for tile, r in res}
        assert result == expectation

    def test_restart_processor(self, caplog, generate_sample_processor):
        def sample_worker(tile, **kwargs):
            """Simulate failing tiles"""
            if tile == 'tile_1' or tile == 'tile_2':
                return False
            else:
                return True
        threadprocessor, expectation = generate_sample_processor(sample_worker)
        threadprocessor.process(restart=3)
        restarts = [rec.message for rec in caplog.records
                    if "Restarting" in rec.message]
        assert len(restarts) == 3
