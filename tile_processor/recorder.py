# -*- coding: utf-8 -*-

"""Process monitoring and logging"""

import logging
import sys

from click import echo
from datetime import datetime

MODULE_PANDAS_AVAILABLE = True
MODULE_MATPLOTLIB_AVAILABLE = True

try:
    import pandas
except ImportError as e:
    MODULE_PANDAS_AVAILABLE = False
try:
    import matplotlib.pyplot as plt
except ImportError as e:
    MODULE_MATPLOTLIB_AVAILABLE = False


log = logging.getLogger(__name__)


def configure_logging(verbosity):
    """Configures the general logging in the application"""
    log_level = max(10, 30 - 10 * verbosity)
    logging.basicConfig(
        stream=sys.stdout,
        level=log_level,
        format='[%(levelname)-8s] %(asctime)s %(module)s.%(funcName)s:%(lineno)s --- %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def configure_ressource_logging() -> logging.Logger:
    """Configures a logger for monitoring the resource usage of worker processes

    Returns a `Logger
    <https://docs.python.org/3.6/library/logging.html#logger-objects>`_, which
    is then passed to the Workers through the Processor.

    :return: A configured Logger
    """
    logname = "tile-resource-usage_" + datetime.utcnow().date().isoformat() + ".tsv"
    echo(f"Saving resource monitor log to '{logname}'")
    log_res = logging.getLogger('subprocess')
    log_res.propagate = False
    log_res.setLevel(logging.DEBUG)
    handler = logging.FileHandler(logname, mode='w', encoding='utf-8')
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s\t%(message)s',
                                  "%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    log_res.addHandler(handler)
    return log_res


def parse_log(logfile:str) -> pandas.DataFrame:
    """Reads a TSV log into a pandas dataframe

    :param logfile: Path to the logfile
    :return: A dataframe
    """
    log = pandas.read_csv(logfile,
                          parse_dates=True,
                          sep='\t',
                          header=None,
                          names=['timestamp', 'tile', 'pid', 'cpu_time_user',
                                 'cpu_time_sys', 'mem_rss'],
                          index_col=0)
    # Convert memory usage in bytes to megabytes
    log.loc[:, 'mem_rss'] *= 0.000001
    # CPU times (see: https://stackoverflow.com/a/556411)
    log['cpu_time_total'] = log['cpu_time_user'] + log['cpu_time_sys']
    # Convert seconds to minutes
    log.loc[:, 'cpu_time_total'] /= 60
    log = log.groupby('tile')
    return log


def save_mem_plot(log:pandas.DataFrame, file:str):
    """Plot the memory usage per tile and save it as a pdf

    :param log: DataFrame from :func:`.parse_log`
    :param file: File name for the plot
    """
    fig, ax = plt.subplots()
    p = log['mem_rss'].plot(legend=True, ax=ax)
    plt.suptitle('Memory usage per tile')
    ax.set_ylabel('Resident Set Size [Mb]')
    ax.set_xlabel('Time')
    fig.savefig(file)
    plt.close(fig)


def save_cpu_log(log:pandas.DataFrame, file:str):
    """Plot the CPU time per tile and save it as a pdf

    :param log: DataFrame from :func:`.parse_log`
    :param file: File name for the plot
    """
    p = log['cpu_time_total'].agg(max).plot.bar()
    fig = p.get_figure()
    plt.suptitle('CPU time per tile')
    p.set_ylabel('CPU time (User+Sys) [minutes]')
    p.set_xlabel('Tile')
    fig.savefig(file)
    plt.close(fig)
