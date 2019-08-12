# -*- coding: utf-8 -*-

"""Workers run the executables. Executables can be anything, but most likely
they are compiled software that are called in a subprocess, for example
*3dfier* (threedfier). In order to implement your own Worker, implement your
class/function here and register it in the click command.
The Factory-pattern reference: `https://realpython.com/factory-method-python/
<https://realpython.com/factory-method-python/>`_
"""

import logging
import os.path as path
from locale import getpreferredencoding
from os import getcwd
from pprint import pformat
from subprocess import PIPE
from time import sleep
from typing import List

from psutil import Popen

log = logging.getLogger(__name__)


class WorkerFactory:
    """Registers and instantiates an Worker.

    A Worker is responsible for running an executable, e.g. 3dfier in case of
    :py:class:`.ThreedfierWorker`
    """

    def __init__(self):
        self._executors = {}

    def register_worker(self, key, worker):
        """Register a worker for use.

        :param key: Name of the worker
        :param worker: Can be a function, a class, or an object that implements
            `.__call__()`
        """
        self._executors[key] = worker

    def create(self, key, **kwargs):
        """Instantiate a worker"""
        worker = self._executors.get(key)
        if not worker:
            raise ValueError(key)
        return worker(**kwargs)


class TemplateWorker:
    """Runs the template"""

    def execute(self, monitor_log, monitor_interval, tile, **ignore):
        """Execute the TemplateWorker with the provided configuration.

        The worker will execute the `./src/simlate_memory_use.sh` script, which
        allocates a constant amount of RAM (~600Mb) and 'holds' it for 10s.
        """
        log.debug(f"Running {self.__class__.__name__}:{tile}")
        log.debug(pformat(ignore))
        package_dir = getcwd()
        exe = path.join(package_dir, 'src', 'simulate_memory_use.sh')
        command = ['bash', exe, '10s']
        res = run_subprocess(command, monitor_log=monitor_log,
                             monitor_interval=monitor_interval, tile_id=tile)
        # TODO: need to return the failed tile
        return res


def run_subprocess(command: List[str], shell: bool = False, doexec: bool = True,
                   monitor_log: logging.Logger = None,
                   monitor_interval: int = 5, tile_id: str = None) -> bool:
    """Runs a subprocess with `psutil.Popen` and monitors its status.

    If subprocess returns non-zero exit code, STDERR is sent to the log.

    :param command: The command to execute.
    :param shell: Passed to `psutil.Popen`. Defaults to False.
    :param doexec: Do execute the subprocess or just print out the concatenated
        command. Used for testing.
    :param monitor_log: A resource logger, which is returned by
        :func:`~.recorder.configure_resource_logging`.
    :param monitor_interval: How often query the resource usage of the process?
        In seconds.
    :param tile_id: Used for monitoring only.
    :return: True/False on success/failure
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
                monitor_log.info(
                    f"{tile_id}\t{popen.pid}\t{popen.cpu_times().user}\t{popen.cpu_times().system}\t{popen.memory_info().rss}")
                return_code = popen.poll()
                if return_code is not None:
                    break
        stdout, stderr = popen.communicate()
        err = stderr.decode(getpreferredencoding(do_setlocale=True))
        popen.wait()
        if popen.returncode != 0:
            log.debug("Process returned with non-zero exit code: %s",
                      popen.returncode)
            log.error(err)
            return False
        else:
            return True
    else:
        log.debug("Not executing %s", command)
        return True


factory = WorkerFactory()
factory.register_worker('template', TemplateWorker)
