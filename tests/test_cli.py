#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `tile_processor` package."""

import pytest

from click.testing import CliRunner

from tile_processor import worker
from tile_processor import cli


@pytest.fixture
def response():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """
    # import requests
    # return requests.get('https://github.com/audreyr/cookiecutter-pypackage')


def test_content(response):
    """Sample pytest test function with the pytest fixture as an argument."""
    # from bs4 import BeautifulSoup
    # assert 'GitHub' in BeautifulSoup(response.content).title.string


class TestCLI:
    def test_help(self):
        """Test the CLI."""
        runner = CliRunner()
        result = runner.invoke(cli.main)
        assert result.exit_code == 0
        assert "Process data sets in tiles." in result.output
        help_result = runner.invoke(cli.main, ["--help"])
        assert help_result.exit_code == 0
        assert "Usage: main [OPTIONS] COMMAND [ARGS]" in help_result.output
