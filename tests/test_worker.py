# -*- coding: utf-8 -*-
# Copyright:    (C)  by Balázs Dukai. All rights reserved.
# Begin:        2020-04-06
# Email:        b.dukai@tudelft.nl

"""Testing the worker module and the various Workers."""

import pytest

from tile_processor import controller

# @pytest.mark.integration_test
class TestThreedfier:
    def test_for_debug(self, cfg_ahn_abs):
        """Running 3dfier"""
        threads=3
        tiles=['all',]
        threedfier_controller = controller.factory.create('AHN',
                                                          configuration=cfg_ahn_abs,
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