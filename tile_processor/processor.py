# -*- coding: utf-8 -*-

"""Processors run Executors in parallel. Processors are responsible for
launching and monitoring the Executors and orchestrating the parallel
processing logic.
"""
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List
from pprint import pformat

log = logging.getLogger(__name__)


class ParallelProcessorFactory:
    """Registers and instantiates a ParallelProcessor that launches the
    Executors."""

    def __init__(self):
        self._processors = {}

    def register_processor(self, key, processor):
        """Register an processor for use.

        :param key: Name of the processor
        :param processor: Can be a function, a class, or an object that
            implements .__call__()
        """
        self._processors[key] = processor

    def create(self, key, **kwargs):
        """Instantiate a Processor"""
        processor = self._processors.get(key)
        if not processor:
            raise ValueError(key)
        return processor(**kwargs)


class ThreadProcessor:
    """For multithreaded processing."""

    def __init__(self, name: str, tiles):
        self.name = name
        self.worker_cfg = None
        self.cfg = None
        self.tiles = tiles
        self.worker = None

    def configure(
        self,
        threads: int,
        monitor_log: logging.Logger,
        monitor_interval: int,
        worker,
        config: dict,
    ):
        """Configure the Processor.

        :param monitor_interval: Monitoring interval in seconds
        :param config: Worker configuration
        :param monitor_log: Logger for resource monitoring
        :param threads: The max. number of workers to call
        :param worker: A callable worker, created by
            :meth:`~.worker.WorkerFactory.create`. For example in case of
            :class:`~.worker.ThreedfierWorker` you need to pass the
            :meth:`~.worker.ThreedfierWorker.execute` callable, and the
            class instance.
        """
        if self.tiles.output.dir:
            log.debug(f"Output directory: {self.tiles.output.dir.path}")
        if self.tiles.output.db:
            log.debug(f"Output database: {self.tiles.output.db.dsn}")
        config["tiles"] = self.tiles
        self.worker_cfg = config
        self.cfg = {
            "threads": threads,
            "monitor_log": monitor_log,
            "monitor_interval": monitor_interval,
        }
        self.worker = worker
        log.info(f"Configured {self.__class__.__name__}:{self.name}")
        log.debug(pformat(vars(self)))

    def process(self, restart: int = 0) -> List[str]:
        """Runs the workers asynchronously, using a `ThreadPoolExecutor
        <https://docs.python.org/3.6/library/concurrent.futures.html#threadpoolexecutor>`_
        and restarts the tiles that failed.

        :param restart: Nr. of restarts for failed tiles
        :return: The IDs of the tiles that failed even after the restarts
        """
        log.info(f"Running {self.__class__.__name__}:{self.name}")
        proc_result = self._process()
        failed_tiles = [tile for tile, result in proc_result if result is False]
        _restart = 0
        while _restart < restart:
            if failed_tiles is not None and len(failed_tiles) > 0:
                _restart += 1
                log.info(
                    f"Restarting {self.__class__.__name__}:{self.name} "
                    f"with {failed_tiles}"
                )
                self.tiles.to_process = failed_tiles
                proc_result = self._process()
                failed_tiles = [tile for tile, result in proc_result if result is False]
            else:
                break
        log.info(f"Done {self.__class__.__name__}:{self.name}")
        return failed_tiles

    def _process(self):
        """Runs the workers asynchronously, using a `ThreadPoolExecutor
        <https://docs.python.org/3.6/library/concurrent.futures.html#threadpoolexecutor>`_.

        :return: Yields the results from the worker.
        """
        with ThreadPoolExecutor(max_workers=self.cfg["threads"]) as executor:
            future_to_tile = {}
            for tile in self.tiles.to_process:
                self.worker_cfg["tile"] = tile
                future_to_tile[
                    executor.submit(self.worker, **self.cfg, **self.worker_cfg)
                ] = tile
            for future in as_completed(future_to_tile):
                tile = future_to_tile[future]
                try:
                    # yield the data that is created/returned by the worker
                    yield tile, future.result()
                except Exception as e:
                    log.exception(f"Tile {tile} raised an exception: {e}")
                    raise
                else:
                    log.info(f"Done with tile {tile}")


factory = ParallelProcessorFactory()
factory.register_processor("threadprocessor", ThreadProcessor)
