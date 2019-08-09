#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `tile_processor` package."""

import pytest

from tile_processor import processor

class TestThreadProcessor:
    def test_thread_executor(self):

        def sample_worker(arg0, arg1=None, arg2=None):
            print(arg0)
            return arg1,arg2

        tiles = ['tile_1', 'tile_2', 'tile_3', 'tile_4', 'tile_5']
        threads = 3
        args = {'arg1': 'argument 1', 'arg2': 'argument 2'}
        expectation = [('argument 1', 'argument 2') for i in range(5)]
        threadprocessor = processor.factory.create('threadprocessor')
        result = threadprocessor.process(threads, tiles, sample_worker, **args)
        assert list(result) == expectation
