# Copyright (C) 23/2/22 RW Bunney

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
import pandas as pd
import json
import logging

import skaworkflows.common as common
import skaworkflows.workflow.hpso_to_observation as hto
from skaworkflows.hpconfig.specs.sdp import SDP_LOW_CDR

logging.basicConfig(level="INFO")
LOGGER = logging.getLogger(__name__)


# TODO update to be path and config_name or something
def create_config(observations, telescope_max, path, component_csv,
                  system_csv, sdp, buffer_ratio, timestep,data=True, **kwargs):
    """
    Parameters
    ----------
    observations : list
        Dictionary with the form {'observation_name': observation_count'}
        where observation_count is an int
    path : string
        Path where the 'config' folder will be created

    **kwargs:
        Intended to be for non-standard system sizing directories - not
        currently implemented.

    Returns
    -------
    Path where observation config is stored
    """
    LOGGER.info("Reading system sizing...")
    component_sizing = pd.read_csv(component_csv)
    system_sizing = pd.read_csv(system_csv)
    cluster = sdp.to_topsim_dictionary()

    LOGGER.info("Creating an observation plan")
    observation_plan = hto.create_observation_plan(
        observations, telescope_max
    )
    LOGGER.debug(f"Observation plan: {observation_plan}")

    LOGGER.info(f"Producing the instrument config")
    final_instrument_config = hto.generate_instrument_config(
        observation_plan, 512,
        path,
        component_sizing,
        system_sizing,
        cluster,
        data
    )
    LOGGER.info(f"Producing buffer config")
    final_buffer_config = hto.create_buffer_config(sdp, buffer_ratio)
    final_cluster = cluster

    LOGGER.info(f"Putting it all together...")
    final_config = {
        "instrument": final_instrument_config,
        "cluster": final_cluster,
        "buffer": final_buffer_config,
        "timestep": timestep
    }

    file_path = f'{path}/config.json'
    with open(file_path, 'w') as fp:
        LOGGER.info(f'Writing final config to {file_path}')
        json.dump(final_config, fp, indent=2)

    LOGGER.info(f'Configuration generation complete!')


if __name__ == '__main__':
    TELESCOPE = "LOW"
    LOGGER.info("Starting config generation...")
    hpso01 = hto.create_observation_from_hpso(
        count=1, hpso='hpso01', demand=512, duration=3600,
        workflows=['DPrepA'], channels=256, baseline='long'
    )
    # hpso04a = hto.create_observation_from_hpso(
    #     count=1, hpso='hpso01', demand=512, duration=60,
    #     workflows=['DPrepA'], channels=256, baseline='long'
    # )
    observations = hpso01  # +hpso04a
    LOGGER.debug(f'Observation list is: {observations}')
    output_path = 'output/'
    sdp = SDP_LOW_CDR()
    buffer_ratio = (1, 5)
    timestep = "seconds"
    LOGGER.info(
        f"Creating config with:\n"
        f"Telescope: SKA_{TELESCOPE}\n"
        f"Output: {output_path}\n"
        f"Buffer ratio: {buffer_ratio}\n"
        f"Timestep: {timestep}\n"
    )
    create_config(
        observations,
        common.MAX_TEL_DEMAND,
        output_path,
        common.COMPONENT_SIZING_LOW,
        common.TOTAL_SIZING_LOW,
        sdp,
        buffer_ratio,
        timestep,
        data=False
    )
