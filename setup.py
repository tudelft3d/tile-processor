#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('CHANGELOG.rst') as changelog_file:
    changelog = changelog_file.read()

requirements = [
    'Click>=7.1',
    'PyYAML>=5.3',
    'psycopg2>=2.8.5',
    'psutil>=5.8',
    'pandas>=1.2',
    'matplotlib>=3.3'
]

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest', ]

setup(
    author="Balázs Dukai",
    author_email='b.dukai@tudelft.nl',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.8',
    ],
    description="A parallel runner for tile-based spatial data processing",
    entry_points={
        'console_scripts': [
            'tile_processor=tile_processor.cli:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + changelog,
    include_package_data=True,
    keywords='tile_processor',
    name='tile_processor',
    packages=find_packages(include=['tile_processor']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/balazsdukai/tile_processor',
    version='0.3.5',
    zip_safe=False,
)
