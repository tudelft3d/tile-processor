# -*- coding: utf-8 -*-

"""Processors run Executors in parallel.
Processors are responsible for launching and monitoring the Executors and orchestrating the parallel processing logic.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
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


class ParallelProcessorFactory:
    """Registers and intantiates a ParallelProcessor that launches the Executors."""
    def __init__(self):
        self._processors = {}

    def register_processor(self, key, processor):
        """Register an processor for use.

        :param key: Name of the processor
        :param processor: Can be a function, a class, or an object that implements .__call__()
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
    def process(self, threads, monitor_log, monitor_interval, tiles, worker, **worker_kwargs):
        """Runs the workers asynchronously, using a `ThreadPoolExecutor <https://docs.python.org/3.6/library/concurrent.futures.html#threadpoolexecutor>`_.
        Yields the results from the worker.

        :param threads: The max. number of workers to call
        :param tiles: The set of tiles that are processed by the workers
        :param worker: A callable worker, created by :meth:`~.worker.WorkerFactory.create`. For example in case of :class:`~.worker.ThreedfierWorker` you need to pass the :meth:`~.worker.ThreedfierWorker.execute` callable, and the the class instance.
        :param worker_kwargs: Arguments passed to the worker
        """
        log.debug(f"Running {self.__class__.__name__}")
        log.debug(f"threads: {threads}")
        with ThreadPoolExecutor(max_workers=threads) as executor:
            # TODO: solve where to pass the tile_id to the worker, because it shouldnt be a hack here below
            future_to_tile = {executor.submit(worker, monitor_log, monitor_interval, tile, **worker_kwargs): tile for tile in tiles}
            for future in as_completed(future_to_tile):
                tile = future_to_tile[future]
                try:
                    # yield the data that is created/returned by the worker
                    yield future.result()
                except Exception as e:
                    log.exception(f"Tile {tile} raised an exception: {e}")
                else:
                    log.info(f"Done with tile {tile}")


class MultiProcessor:
    """For multiprocessing"""
    def process(self):
        raise NotImplementedError(self.__class__.__name__)


factory = ParallelProcessorFactory()
factory.register_processor('threadprocessor', ThreadProcessor)
factory.register_processor('multiprocessor', MultiProcessor)
