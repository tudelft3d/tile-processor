# -*- coding: utf-8 -*-

"""Workers run the executables. Executables can be anything, but most likely they are compiled software that are called
in a subprocess, for example *3dfier* (threedfier).
In order to implement your own Worker, implement your class/function here
and register it in the click command.
The Factory-pattern reference: `https://realpython.com/factory-method-python/ <https://realpython.com/factory-method-python/>`_
"""

from time import sleep
import logging

# -------------------
# Test logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)
# -------------------


class WorkerFactory:
    """Registers and instantiates an Worker.
    A Worker is responsible for running an executable, e.g. 3dfier in case of :py:class:`.ThreedfierWorker`
    """
    def __init__(self):
        self._executors = {}

    def register_worker(self, key, worker):
        """Register a worker for use.

        :param key: Name of the worker
        :param worker: Can be a function, a class, or an object that implements `.__call__()`
        """
        self._executors[key] = worker

    def create(self, key, **kwargs):
        """Instantiate a worker"""
        worker = self._executors.get(key)
        if not worker:
            raise ValueError(key)
        return worker(**kwargs)


class ThreedfierWorker:
    """Runs 3dfier"""
    def __init__(self):
        self.name = '3dfier'

    def execute(self, cfg_3dfier, **ignore):
        """Execute 3dfier with the provided configuration"""
        logger.debug(f"Running {self.name}")
        sleep(5)
        return cfg_3dfier


class LoD10Worker:
    """Runs the LoD1.0 building reconstruction"""
    def __init__(self):
        self.name = 'LoD1.0'
        self.lod = '1.0'

    def execute(self, cfg_lod10, **ignore):
        logger.debug(f"Running {self.name} in level-of-detail {self.lod}")
        logger.debug(cfg_lod10)
