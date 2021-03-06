#!/usr/bin/env python3

import io
import re
import os

from setuptools import setup, find_packages
from setuptools.command.develop import develop
from setuptools.command.install import install
from subprocess import check_call, CalledProcessError


def read(*names, **kwargs):
    with io.open(
        os.path.join(os.path.dirname(__file__), *names),
        encoding=kwargs.get("encoding", "utf8"),
    ) as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")

setup(
    name="watchdog",
    version=find_version("watchdog", "__init__.py"),  # +'-dev3',
    description="Watch Beagle",
    url="https://github.com/LinkageIO/Watchdog",
    author="Rob Schaefer",
    author_email="rob@linkage.io",
    license="Copyright Rob Schaefer 2020. Available under the MIT License",
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        "Development Status :: 3 - Alpha",
        # Indicate who your project is intended for
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        # Pick your license as you wish (should match "license" above)
        "License :: OSI Approved :: MIT License",
        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        "Programming Language :: Python :: 3.8",
    ],
    keywords="watch beagle impute",
    project_urls={
        "Source": "https://github.com/LinkageIO/Watchdog",
        "Tracker": "https://github.com/LinkageIO/Watchdog/issues",
    },
    packages=find_packages(),
    scripts=[],
    ext_modules=[],
    cmdclass={},
    package_data={"": [""]},
    install_requires=[
        "click >= 7.0",
        "numpy >= 1.17.3",
        "locuspocus >= 1.0.2"
    ],
    extras_require={"docs": []},
    include_package_data=True,
    entry_points="""
        [console_scripts]
        watchdog=watchdog.cli.watchdog:cli
    """,
)
