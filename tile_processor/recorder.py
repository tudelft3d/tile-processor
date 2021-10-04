# -*- coding: utf-8 -*-

"""Process monitoring and logging"""

import logging
import sys
from datetime import datetime
from typing import Optional

from click import echo
import pandas
import matplotlib.pyplot as plt

log = logging.getLogger(__name__)


def configure_logging(log_level_stream, filename: Optional[str] = None,
                      log_level_file=None):
    """Configures the general logging in the application
    :param log_level_file:
    """
    handlers = []
    log_level_str = getattr(logging, log_level_stream.upper(), None)

    logger = logging.getLogger("tile_processor")
    logger.propagate = True

    formatter = logging.Formatter(
        fmt="%(asctime)s\t%(name)-24s\t%(lineno)s\t[%(levelname)-8s]\t%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    if filename:
        f_handler = logging.FileHandler(filename, mode="w", encoding="utf-8")
        f_handler.setFormatter(formatter)
        f_handler.setLevel(getattr(logging, log_level_file.upper(), None))
        # logger.addHandler(f_handler)
        handlers.append(f_handler)
    c_handler = logging.StreamHandler(stream=sys.stdout)
    c_handler.setFormatter(formatter)
    c_handler.setLevel(log_level_str)
    # logger.addHandler(c_handler)
    handlers.append(c_handler)
    # return logger
    logging.basicConfig(
        handlers=handlers
    )


def configure_ressource_logging() -> logging.Logger:
    """Configures a logger for monitoring the resource usage of worker processes

    Returns a `Logger
    <https://docs.python.org/3.6/library/logging.html#logger-objects>`_, which
    is then passed to the Workers through the Processor.

    :return: A configured Logger
    """
    tstamp = datetime.now().isoformat(timespec="seconds")
    logname = f"tile-resource-usage_{tstamp}.tsv"
    echo(f"Saving resource monitor log to '{logname}'")
    log_res = logging.getLogger("subprocess")
    log_res.propagate = False
    log_res.setLevel(logging.DEBUG)
    handler = logging.FileHandler(logname, mode="w", encoding="utf-8")
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s\t%(message)s", "%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    log_res.addHandler(handler)
    return log_res


def parse_log(logfile: str) -> pandas.DataFrame:
    """Reads a TSV log into a pandas dataframe

    :param logfile: Path to the logfile
    :return: A pandas dataframe
    """
    log = pandas.read_csv(
        logfile,
        parse_dates=True,
        sep="\t",
        header=None,
        names=[
            "timestamp",
            "tile",
            "pid",
            "cpu_time_user",
            "cpu_time_sys",
            "mem_rss",
        ],
        index_col=0,
    )
    # Convert memory usage in bytes to megabytes
    log.loc[:, "mem_rss"] *= 0.000001
    # CPU times (see: https://stackoverflow.com/a/556411)
    log["cpu_time_total"] = log["cpu_time_user"] + log["cpu_time_sys"]
    # Convert seconds to minutes
    log.loc[:, "cpu_time_total"] /= 60
    log = log.groupby("tile")
    return log


def save_mem_plot(log: pandas.DataFrame, file: str):
    """Plot the memory usage per tile and save it as a pdf

    :param log: DataFrame from :func:`.parse_log`
    :param file: File name for the plot
    """
    fig, ax = plt.subplots()
    p = log["mem_rss"].plot(legend=True, ax=ax)
    plt.suptitle("Memory usage per tile")
    ax.set_ylabel("Resident Set Size [Mb]")
    ax.set_xlabel("Time")
    fig.savefig(file)
    plt.close(fig)


def save_cpu_log(log: pandas.DataFrame, file: str):
    """Plot the CPU time per tile and save it as a pdf

    :param log: DataFrame from :func:`.parse_log`
    :param file: File name for the plot
    """
    p = log["cpu_time_total"].agg(max).plot.bar()
    fig = p.get_figure()
    plt.suptitle("CPU time per tile")
    p.set_ylabel("CPU time (User+Sys) [minutes]")
    p.set_xlabel("Tile")
    fig.savefig(file)
    plt.close(fig)
