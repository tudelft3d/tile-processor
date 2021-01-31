==============
Tile Processor
==============

A batch processor for concurrent, tile-based spatial data processing.

* Free software: MIT license

.. code-block::

    Usage: tile_processor [OPTIONS] COMMAND [ARGS]...

      Process data sets in tiles.

    Options:
      --log [DEBUG|INFO|WARNING|ERROR|CRITICAL]
                                      Set the logging level in the log file.
      -q, --quiet                     Decrease verbosity.
      -m, --monitor INTEGER           Monitor the resource usage of the processes
                                      that work on the tiles. The log is saved as
                                      TSV into tile-resource-usage_<date>.tsv. The
                                      monitoring interval is passed in seconds.
      --help                          Show this message and exit.

    Commands:
      export_tile_inputs  Export the input footprints and point cloud for a
                          list...
      list-schemas        Lists the registered configuration schemas.
      plot-monitor-log    Plot the data that the resource monitor recorded.
      register-schema     Registers a schema for parsing configuration files.
      remove-schema       Removes a configuration schema from the database
      run                 Run a process on multiple threads.


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
