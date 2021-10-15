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

from enum import Enum


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
MAX_CHANNELS = 512
MAX_TEL_DEMAND = 256


compute_keys = {
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
        'dprepa': 'data/hpsos/dprepa.graph'
    }
}

component_paths = {
    'low_short': 'data/pandas_sizing/component_compute_SKA1_Low_short.csv',
    'low_mid': 'data/pandas_sizing/component_compute_SKA1_Low_mid.csv',
    'low_long': 'data/pandas_sizing/component_compute_SKA1_Low_long.csv',
    'mid_short': 'data/pandas_sizing/component_compute_SKA1_Mid_short.csv',
    'mid_mid': 'data/pandas_sizing/component_compute_SKA1_Mid_mid.csv',
    'mid_long': 'data/pandas_sizing/component_compute_SKA1_Mid_long.csv',
}
