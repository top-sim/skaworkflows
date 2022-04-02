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

from pathlib import Path
from skaworkflows.hpconfig.specs.sdp import SDP_LOW_CDR

LOGGER = logging.getLogger(__name__)


# TODO update to be path and config_name or something
def create_config(
        telescope_max,
        hpso_path,
        output_path,
        component_csv,
        system_csv,
        cluster,
        timestep='seconds',
        data=True,
        **kwargs
):
    """
    Parameters
    ----------
    data
    timestep
    cluster
    system_csv
    component_csv
    telescope_max
    observations : list
        Dictionary with the form {'observation_name': observation_count'}
        where observation_count is an int
    output_path : pathlib.Path
        Path where the 'config' folder will be created

    **kwargs:
        Intended to be for non-standard system sizing directories - not
        currently implemented.

    Returns
    -------
    Path where observation config is stored
    """
    LOGGER.info(
        f"\tTelescope: SKA_LOW \n"
        f"\tCreating config with:\n"
        f"\tOutput Directory: {output_path}\n"
        f"\tBuffer ratio: {cluster.buffer_ratio}\n"
        f"\tTimestep: {timestep}\n"
        f"\tData: {data}"
    )

    LOGGER.info("Reading system sizing...")
    component_sizing = pd.read_csv(component_csv)
    system_sizing = pd.read_csv(system_csv)
    cluster_dict = sdp.to_topsim_dictionary()
    observations = hto.process_hpso_from_spec(hpso_path)
    if not observations:
        RuntimeError('Observations do not exist!')
    LOGGER.debug(f"Creating an observation plan with {observations}")
    observation_plan = hto.create_observation_plan(
        observations, telescope_max
    )
    LOGGER.debug(f"Observation plan: {observation_plan}")

    LOGGER.info(f"Producing the instrument config")
    final_instrument_config = hto.generate_instrument_config(
        observation_plan, 512,
        output_path,
        component_sizing,
        system_sizing,
        cluster_dict,
        data
    )
    LOGGER.info(f"Producing buffer config")
    final_buffer_config = hto.create_buffer_config(sdp, cluster.buffer_ratio)
    final_cluster = cluster_dict

    LOGGER.info(f"Putting it all together...")
    final_config = {
        "instrument": final_instrument_config,
        "cluster": final_cluster,
        "buffer": final_buffer_config,
        "timestep": timestep
    }

    file_path = output_path / 'config.json'
    with file_path.open('w') as fp:
        LOGGER.info(f'Writing final config to {file_path}')
        json.dump(final_config, fp, indent=2)

    LOGGER.info(f'Configuration generation complete!')

    return file_path


if __name__ == '__main__':
    logging.basicConfig(level="DEBUG")

    LOGGER.info("Starting config generation...")
    hpso_path = Path('tests/data/single_hpso.json')
    output_path = Path('output/')
    sdp = SDP_LOW_CDR()
    timestep = "seconds"
    data = False

    create_config(
        common.MAX_TEL_DEMAND,
        hpso_path,
        output_path,
        common.COMPONENT_SIZING_LOW,
        common.TOTAL_SIZING_LOW,
        sdp,
        timestep,
        data=data
    )
