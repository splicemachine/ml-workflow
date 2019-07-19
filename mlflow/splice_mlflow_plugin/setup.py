"""
Python Package for Splice Machine MLFlow Plugin.
We need to actually pip install this package because
MLFlow finds third-party plugins through entrypoints
registered upon the execution of this file
"""

from setuptools import setup, find_packages

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2018, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Murray Brown", "Monte Zweben"]

__license__: str = "Commercial"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"

setup(
    name='sm_mlflow',
    description='MLFlow Plugin for Splice Machine RDBMS. It allows Splice Machine to be used'
                'as a tracking backend for SQLAlchemy Store in MLFlow',
    author='Splice Machine',
    url='https://splicemachine.com',
    install_requires=[
        'splicemachinesa==0.0.7.dev0',
        'sqlalchemy',
        'alembic'
        'mlflow>=1.0.0'
    ],
    packages=find_packages(),
    entry_points={
        "mlflow.tracking_store": "splicemachinesa=sm_mlflow:SpliceMachineTrackingStore"
    }
)