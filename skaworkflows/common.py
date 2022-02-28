# Copyright (C) 12/10/21 RW Bunney

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

"""
Common data and classes for access between diferrent pipeline codes
"""
import pathlib
from enum import Enum
from skaworkflows import __version__


class Baselines(Enum):
    short = 4062.5
    mid = 32500
    long = 65000


class SI:
    kilo = 10 ** 3
    mega = 10 ** 6
    giga = 10 ** 9
    tera = 10 ** 12
    peta = 10 ** 15


# These are 'binned' channels, by dividing the number of real channels by 64.
MAX_BIN_CHANNELS = 256
MAX_TEL_DEMAND = 512
BASE_GRAPH_PATH = 'skaworkflows/data/hpsos/hpso01/dprepa.graph'
COMPONENT_SIZING_LOW = 'skaworkflows/data/pandas_sizing/component_compute_SKA1_Low.csv'
TOTAL_SIZING_LOW = 'skaworkflows/data/pandas_sizing/total_compute_SKA1_Low.csv'

COMPUTE_KEYS = {
    'hpso': "HPSO",
    'time': "Time [%]",
    'stations': "Stations",
    'tobs': "Tobs [h]",
    'ingest': "Ingest [Pflop/s]",
    'rcal': "RCAL [Pflop/s]",
    'fastimg': "FastImg [Pflop/s]",
    'ical': "ICAL [Pflop/s]",
    'dprepa': "DPrepA [Pflop/s]",
    'dprepb': "DPrepB [Pflop/s]",
    'dprepc': "DPrepC [Pflop/s]",
    'dprepd': "DPrepD [Pflop/s]",
    'totalrt': "Total RT [Pflop/s]",
    'totalbatch': "Total Batch [Pflop/s]",
    'total': "Total [Pflop/s]",
    'ingest_rate': 'Ingest Rate [TB/s]'
}

compute_units = {
    'hpso': None,
    'time': "%",
    'stations': None,
    'tobs': "h",
    'ingest': "Pflop/s",
    'rcal': "Pflop/s",
    'fastimg': "Pflop/s",
    'ical': "Pflop/s",
    'dprepa': "Pflop/s",
    'dprepb': "Pflop/s",
    'dprepc': "Pflop/s",
    'dprepd': "Pflop/s",
    'totalrt': "Pflop/s",
    'totalbatch': "Pflop/s",
    'total': "Pflop/s",
    'ingest_rate': "TB/s"
}

WORKFLOW_HEADER = {
    'generator': {
        'name': 'skaworkflows',
        'version': __version__,
    },
    'parameters': {
        'max_arrays': 512,
        'channels': None,
        'arrays': None,
        'baseline': None,
        'duration': None,
    },
    'time': 'False'
}

# Keys for DATA csv
data_keys = {
    'telescope': "Telescope",
    'pipeline': "Pipeline ",
    'datarate': "Data Rate [Gbit/s]",
    'dailygrowth': "Daily Growth [TB/day]",
    'yearlygrowth': "Yearly Growth [PB/year]",
    'five_yeargrowth': "5-year Growth [PB/(5 year)]"
}

hpso_constraints_exclude = {
    'hpso4': ['dprepa', 'dprepb', 'dprepc', 'dprepd'],  # Pulsar
    'hpso5': ['dprepa', 'dprepb', 'dprepc', 'dprepd'],  # Pulsar
    'hpso18': ['dprepa', 'dprepb', 'dprepc', 'dprepd'],  # Transients (FRB)
    'hpso32': ['dprepa', 'dprepc', 'dprepd']  # Cosmology (Gravity)
}

pipeline_constraints = {
    'dprepc': ['dprepa']
}

# Pulsars do not require imaging pipelines
pulsars = ['hpso4a', 'hpso5a']

pipeline_paths = {
    'hpso01': {
        'dprepa': 'skaworkflows/data/hpsos/dprepa.graph'
    }
}

component_paths = {
    'low_short': 'skaworkflows/data/pandas_sizing/component_compute_SKA1_Low_short.csv',
    'low_mid': 'skaworkflows/data/pandas_sizing/component_compute_SKA1_Low_mid.csv',
    'low_long': 'skaworkflows/data/pandas_sizing/component_compute_SKA1_Low_long.csv',
    'mid_short': 'skaworkflows/data/pandas_sizing/component_compute_SKA1_Mid_short.csv',
    'mid_mid': 'skaworkflows/data/pandas_sizing/component_compute_SKA1_Mid_mid.csv',
    'mid_long': 'skaworkflows/data/pandas_sizing/component_compute_SKA1_Mid_long.csv',
}
