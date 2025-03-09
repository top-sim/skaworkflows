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

import sdp_par_model

from pathlib import Path
from enum import Enum, IntEnum
try:
    import importlib.resources as imp_res
except: 
    import importlib_resources as imp_res

from skaworkflows import __version__

from sdp_par_model.parameters.definitions import HPSOs, apply_hpso_parameters

hpso = HPSOs.hpso01


class LOWBaselines(Enum):
    """
    Mapping baseline lengths to 'arbitray' length classifications

    These are terms used for the purpose of experimental shorthand
    and do not necessarily represent what the SKA would term a 'short'
    or 'long' baseline.
    """

    short = 4062.5
    mid = 32500
    long = 65000


class SI(IntEnum):
    """
    Convenience class for SI units
    """

    kilo = 10 ** 3
    mega = 10 ** 6
    giga = 10 ** 9
    tera = 10 ** 12
    peta = 10 ** 15


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
CONT_IMG_MVP_GRAPH = GRAPH_DIR.joinpath("cont_img_mvp_skaworkflows_updated.graph")
SCATTER_GRAPH = GRAPH_DIR.joinpath("dprepa_parallel_updated.graph")
PULSAR_GRAPH = GRAPH_DIR.joinpath("pulsar.graph")


# Default JSON "header" elemetn used when generating workflow files
WORKFLOW_HEADER = {
    "generator": {
        "name": "skaworkflows",
        "version": __version__,
    },
    "parameters": {
        "max_arrays": 512,
        "channels": None,
        "arrays": None,
        "baseline": None,
        "duration": None,
    },
    "time": "False",
}
