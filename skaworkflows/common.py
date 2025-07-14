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
Common data and classes for access between different tool codes

Included in this modules are the following class definitions:
    - LowBaselines
    - SI
"""

import numpy as np

import json
from pathlib import Path
from enum import Enum, IntEnum, auto
try:
    import importlib.resources as imp_res
except: 
    import importlib_resources as imp_res

from skaworkflows import __version__


class SI(IntEnum):
    """
    Convenience class for SI units
    """

    kilo = 10 ** 3
    mega = 10 ** 6
    giga = 10 ** 9
    tera = 10 ** 12
    peta = 10 ** 15

class Telescope:

    def __init__(self, telescope: str):
        if not self._contains_low(telescope) and not self._contains_mid(telescope):
            raise ValueError("Telescope %s is not supported! Please try SKALow or SKAMid.")

        self._telescope = SKALow() if self._contains_low(telescope) else SKAMid()

    def _contains_low(self, param):
        return "low" in param.lower()

    def _contains_mid(self, param):
        return "mid" in param.lower()

    def __getattr__(self, item):
        return getattr(self._telescope, item)


    def __instancecheck__(self, instance):
        isinstance(self._telescope, instance)

    def __repr__(self):
        return self._telescope.name


class SKALow:
    """
    Store SKA Low Information

    These are used for limit checking and for use across the observing plans

    Ref: sdp-par-model
    """
    name = "low"

    baselines = [4.0625, 8.125, 16.25, 32.5, 65]

    hpso01 = "hpso01"
    hpso02a = "hpso02a"
    hpso02b = "hpso02b"
    hpso4a = "hpso04a"
    hpso05a = "hpso5b"

    stations = [64, 128, 256, 512]
    max_stations = max(stations)

    max_compute_nodes = 896

    workflow_parallelism = [64, 128, 256]
    channels_multiplier = 128

    default_compute_nodes = 896
    low_realtime_resources = 164

    observation_defaults = imp_res.files("skaworkflows.data.observation") / "low_defaults.toml"

    def __str__(self):
        return self.name

    def __repr__(self):
        return str(self)


class SKAMid:
    """
    Store SKA Mid Information

    Ref: sdp-par-model
    """

    name = 'mid'
    baselines = [5, 10, 15, 25, 75, 110, 150]

    hpso01 = "hpso01"
    hpso02a = "hpso02a"
    hpso02b = "hpso02b"
    hpso4A = "hpso04a"
    hpso05a = "hpso5b"

    stations = [64, 102, 140, 197] # antenna
    max_stations = max(stations)

    max_compute_nodes = 786

    workflow_parallelism = [64, 128, 256, 512]
    channels_multiplier = 128

    default_compute_nodes = 786

    observation_defaults = imp_res.files("skaworkflows.data.observation") / "mid_defaults.toml"

    mid_realtime_resources = 281

    def __str__(self):
        return self.name

    def __repr__(self):
        return str(self)

class Workflows:

    ical = "ICAL"
    drepa = "DPrepA"
    dprepb = "DPrepB"
    dprepc = "DPrepC"
    dprepd = "DPrepD"
    pulsar = "Pulsar"

# Amount of the telescope an observation can request
MAX_TEL_DEMAND_LOW = 512
MAX_TEL_DEMAND_MID = 197
MAX_CHANNELS = 512

# System sizing data paths
DATA_PANDAS_SIZING = imp_res.files("skaworkflows.data.pandas_sizing")
LOW_TOTAL_SIZING = DATA_PANDAS_SIZING.joinpath("total_compute_SKA1_Low_2024-08-20.csv")
LOW_COMPONENT_SIZING = Path(
    DATA_PANDAS_SIZING.joinpath("component_compute_SKA1_Low_2024-08-20.csv")
)
MID_TOTAL_SIZING = Path(DATA_PANDAS_SIZING.joinpath("total_compute_SKA1_Mid_2024-08-20.csv"))
MID_COMPONENT_SIZING = Path(
    DATA_PANDAS_SIZING.joinpath("component_compute_SKA1_Mid_2024-08-20.csv")
)

# Bytes per obseved visibility
BYTES_PER_VIS = 12.0

GRAPH_DIR = imp_res.files("skaworkflows.data.hpsos")
BASIC_PROTOTYPE_GRAPH = GRAPH_DIR.joinpath("dprepa.graph")
CONT_IMG_MVP_GRAPH = GRAPH_DIR.joinpath("cont_img_mvp.graph")
SCATTER_GRAPH = GRAPH_DIR.joinpath("dprepa_parallel_updated.graph")
PULSAR_GRAPH = GRAPH_DIR.joinpath("pulsar.graph")



def create_workflow_header(telescope: str):
    """
    Default JSON "header" elemetn used when generating workflow files
    Parameters
    ----------
    telescope

    Returns
    -------

    """
    return {
        "generator": {
            "name": "skaworkflows",
            "version": __version__,
        },
        "parameters": {
            "max_arrays": Telescope(telescope).max_stations,
            "channels": None,
            "arrays": None,
            "baseline": None,
            "duration": None,
        },
        "time": "False",
    }

class NpEncoder(json.JSONEncoder):
    # My complete laziness
    # https://java2blog.com/object-of-type-int64-is-not-json-serializable/

    def default(self, o):
        if isinstance(o, np.integer):
            return int(o)
        if isinstance(o, np.floating):
            return float(o)
        if isinstance(o, np.ndarray):
            return o.tolist()
        if isinstance(o, np.int64):
            return int(o)
        return super(NpEncoder, self).default(o)
