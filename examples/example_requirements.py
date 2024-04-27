# Copyright (C) 11/3/22 RW Bunney

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

import logging
import numpy as np
import pandas as pd
from pathlib import Path
from skaworkflows.common import SI
from skaworkflows.hpconfig.specs import sdp

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

COMPONENT_DATA = Path(
    'skaworkflows/data/pandas_sizing/component_compute_SKA1_Low.csv'
)
TOTAL_DATA = Path(
    'skaworkflows/data/pandas_sizing/total_compute_SKA1_Low.csv'
)

OBSERVATION_LENGTH = 3600

infrastructure = sdp.SDP_LOW_CDR()

component_sizing = pd.read_csv(COMPONENT_DATA, index_col=0)
total_sizing = pd.read_csv(TOTAL_DATA)

# GRID
dprepa_df = component_sizing[
    component_sizing['hpso'] == 'hpso01'
    ].loc['DPrepA']
dprepa_df_data = component_sizing[
    component_sizing['hpso'] == 'hpso01'
    ].loc['DPrepA_data']

for task in [('Grid', 256), ('Subtract Visibility', 512)]:
    component, num_tasks = task
    total_compute = dprepa_df[[component]] * 3600 * SI.peta
    total_data = dprepa_df_data[[component]] * 3600 * SI.tera

    compute_per_task = total_compute[[component]] / (100 * num_tasks) / SI.tera
    data_per_task = total_data[[component]] / (100 * num_tasks) / SI.tera

    relative_compute = (
            np.array(total_compute[[component]])
            / total_data[[component]]
    )
    LOGGER.info(f"\n{num_tasks*100=}")
    LOGGER.info(f"\n{total_compute=}\n{total_data=}")
    LOGGER.info(f"\n{compute_per_task=}\n{data_per_task=}")
    LOGGER.info(f"\n{relative_compute=}")

LOGGER.info(infrastructure.to_df().T)
LOGGER.info(
    infrastructure.generate_parametric_model_values().T
)
# Subtract
