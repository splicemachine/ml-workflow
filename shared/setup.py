"""
Python Package for MLManager Lib:
an internal Python package containing
utilities and shared libraries across
the MLManager components
"""
from setuptools import find_packages, setup

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Murray Brown", "Monte Zweben"]

__license__: str = "Commercial"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"

VERSION: str = "0.0.3"

setup(
    name='shared',
    description='Internal Python Package containing shared libraries and utility functions for '
                'the MLManager stack',
    author='Splice Machine',
    version=VERSION,
    url='https://splicemachine.com',
    packages=find_packages(),
    include_package_data=True
)
