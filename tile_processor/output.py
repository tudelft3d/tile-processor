# -*- coding: utf-8 -*-

"""Output handling."""

import os
import shutil
import logging


log = logging.getLogger(__name__)

class DirOutput:
    """Output to a directory. Creates `path` if not exists."""

    def __init__(self, path):
        self.path = path

    @property
    def path(self):
        return self.__path

    @path.setter
    def path(self, value):
        abs_p = os.path.abspath(value)
        try:
            os.makedirs(abs_p, exist_ok=False)
            log.info(f"Created directory {abs_p}")
        except OSError:
            log.debug(f"Directory already exists, {abs_p}")
        self.__path = abs_p

    @path.deleter
    def path(self):
        try:
            os.rmdir(self.path)
        except OSError as e:
            log.exception(e)

    def add(self, path):
        """Join the input with self and return a path."""
        return os.path.join(self.path, path)
