# Copyright (C) 19/1/22 RW Bunney

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
from setuptools import setup, find_packages

setup(
    name='skaworkflows',
    version='0.1.0',
    packages=find_packages(
        exclude=["*test*", "*hpconfig*", "examples", "docs", "data"]
    ),
    include_package_data=True,
    install_requires=[
        'numpy>=1.2.0',
        'networkx>=2.0',
        'matplotlib>=3.0',
        'pandas>=0.20',
        'coverage',
        'IPython',
        'pydot',
        'coveralls',
        'sdp-par-model @ git+https://github.com/ska-telescope/sdp-par-model',
        'daliuge-common @ git+https://github.com/ICRAR/daliuge.git#egg'
        '&subdirectory=daliuge-common',
        'daliuge-engine @ git+https://github.com/ICRAR/daliuge.git#egg'
        '&subdirectory=daliuge-engine',
        'daliuge-translator @ git+https://github.com/ICRAR/daliuge.git#egg'
        '&subdirectory=daliuge-translator'
    ],
    # dependency_links=[
    #     'https://github.com/ska-telescope/sdp-par-model.git"'
    # ],

    # package_dir={'': 'shadow'},
    # url='https://github.com/top-sim/topsim',
    license='GNU',
    author='Ryan Bunney',
    author_email='ryan.bunney@icrar.org',
    description='Workflows for experiments with SKA Science Data Processor'
)
