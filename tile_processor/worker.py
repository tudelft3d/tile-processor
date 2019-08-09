# -*- coding: utf-8 -*-

"""Workers run the executables. Executables can be anything, but most likely they are compiled software that are called
in a subprocess, for example *3dfier* (threedfier).
In order to implement your own Worker, implement your class/function here
and register it in the click command.
The Factory-pattern reference: `https://realpython.com/factory-method-python/ <https://realpython.com/factory-method-python/>`_
"""

from os import getcwd
import os.path as path
from psutil import Popen, Process, NoSuchProcess, ZombieProcess, AccessDenied, swap_memory, virtual_memory
from subprocess import PIPE
from time import sleep
from locale import getpreferredencoding
import logging

# -------------------
# Test logging setup
log = logging.getLogger(__name__)
# log.setLevel(logging.DEBUG)
# # create console handler and set level to debug
# ch = logging.StreamHandler()
# ch.setLevel(logging.DEBUG)
# # create formatter
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# # add formatter to ch
# ch.setFormatter(formatter)
# # add ch to log
# log.addHandler(ch)
# -------------------
# log_res = logging.getLogger('subprocess')
# log_res.setLevel(logging.DEBUG)
# # create console handler and set level to debug
# ch_perf = logging.FileHandler('performance.tsv')
# ch_perf.setLevel(logging.DEBUG)
# # create formatter
# formatter_perf = logging.Formatter('%(asctime)s %(message)s', "%Y-%m-%d %H:%M:%S")
# # add formatter to ch
# ch_perf.setFormatter(formatter_perf)
# # add ch to logger
# log_res.addHandler(ch_perf)
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

    def execute(self, monitor, monitor_interval, tile_id, cfg_3dfier=None, **ignore):
        """Execute 3dfier with the provided configuration"""
        log.debug(f"Running {self.name}")
        package_dir = getcwd()
        exe = path.join(package_dir, 'src', 'simulate_memory_use.sh')
        command = ['bash', exe, '10s']
        res = run_subprocess(command, monitor_log=monitor, monitor_interval=monitor_interval, tile_id=tile_id)
        return cfg_3dfier


class LoD10Worker:
    """Runs the LoD1.0 building reconstruction"""
    def __init__(self):
        self.name = 'LoD1.0'
        self.lod = '1.0'

    def execute(self, cfg_lod10, **ignore):
        log.debug(f"Running {self.name} in level-of-detail {self.lod}")
        log.debug(cfg_lod10)


def run_subprocess(command, shell=False, doexec=True, monitor_log=None, monitor_interval=5, tile_id=None):
    """Runs a subprocess with `psutil` and monitors its status

    If subprocess returns non-zero exit code, STDERR is sent to the log.
    """
    if doexec:
        cmd = " ".join(command)
        if shell:
            command = cmd
        log.debug(command)
        popen = Popen(command, shell=shell, stderr=PIPE, stdout=PIPE)
        if monitor_log is not None:
            while True:
                sleep(monitor_interval)
                monitor_log.info(f"{tile_id}\t{popen.pid}\t{popen.cpu_times().user}\t{popen.cpu_times().system}\t{popen.memory_info().rss}")
                return_code = popen.poll()
                if return_code is not None:
                    break
        stdout, stderr = popen.communicate()
        err = stderr.decode(getpreferredencoding(do_setlocale=True))
        popen.wait()
        if popen.returncode != 0:
            log.debug("Process returned with non-zero exit code: %s", popen.returncode)
            log.error(err)
            return False
        else:
            return True
    else:
        log.debug("Not executing %s", command)
        return True
