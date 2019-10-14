==============
Tile Processor
==============


.. image:: https://img.shields.io/pypi/v/tile_processor.svg
        :target: https://pypi.python.org/pypi/tile_processor

.. image:: https://img.shields.io/travis/balazsdukai/tile_processor.svg
        :target: https://travis-ci.org/balazsdukai/tile_processor

.. image:: https://readthedocs.org/projects/tile-runner/badge/?version=latest
        :target: https://tile-runner.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status




A batch processor for concurrent, tile-based spatial data processing.

* Free software: MIT license
* Documentation: https://tile-processor.readthedocs.io.

.. code-block::

    Usage: tile_processor [OPTIONS] COMMAND [ARGS]...

      Console script for tile_processor.

    Options:
      -v, --verbose          Increase verbosity. You can increment the level by
                             chaining the argument, eg. -vvv
      -q, --quiet            Decrease verbosity.
      -m, --monitor INTEGER  Monitor the resource usage of the processes that work
                             on the tiles. The log is saved as TSV into tile-
                             resource-usage_<date>.tsv. The monitoring interval is
                             passed in seconds.
      --help                 Show this message and exit.

    Commands:
      list-schemas      Lists the registered configuration schemas.
      plot-monitor-log  Plot the data that the resource monitor recorded.
      register-schema   Registers a schema for parsing configuration files.
      remove-schema     Removes a configuration schema from the database
      run-template      Run a template process on a batch of tiles in parallel.
      run-template-db   Run a template process on a batch of database tiles in...


Features
--------

* TODO

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
