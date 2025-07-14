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
import json
import logging
import datetime
import pandas as pd

from pathlib import Path

import skaworkflows.common as common
import skaworkflows.workflow.hpso_to_observation as hto
from skaworkflows.common import SKALow

from skaworkflows.hpconfig.specs.sdp import (
    SDP_LOW_CDR, SDP_MID_CDR, SDP_PAR_MODEL_LOW, SDP_PAR_MODEL_MID
)
LOGGER = logging.getLogger(__name__)

LOGGER.setLevel('DEBUG')

def create_config(
        # TODO define what parameters means!
        parameters: dict,
        output_dir: Path,
        base_graph_paths,
        timestep='seconds',
        data=False,
        overwrite=False,
        data_distribution='standard',
        multiple_plans=False,
        max_num_plans=5,
        **kwargs
):
    """
    Parameters
    ----------
    parameters
    output_dir : pathlib.Path
        Path where the 'config' folder will be created

    **data_distribution:
        Intended to be for non-standard system sizing directories - not
        currently implemented.

    Returns
    -------
    Path where observation config is stored
    """
    dt = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    cfg_name = Path(f"skaworkflows_{dt}")
    LOGGER.info("Generating %s...", cfg_name)

    telescope = None
    try:
        telescope = common.Telescope(parameters["telescope"])
    except ValueError:
        LOGGER.warning("Unable to create observation plan due to unsupported telescope.\n"
                       "Please use either SKALow or SKAMid as your selection.")

    compute_nodes = parameters["nodes"]
    hpc_infrastructure_model = parameters["infrastructure"]

    if 'data_distribution' in data_distribution:
        data_distribution = True

    file_path = output_dir / cfg_name
    if file_path.exists() and not overwrite:
        LOGGER.info("Config %s exists, skipping instruction...", file_path)
        return file_path

    data_rate_multiplier = kwargs.get("data_multiplier", 1)

    if telescope.name == SKALow().name:
        component = common.LOW_COMPONENT_SIZING
        system = common.LOW_TOTAL_SIZING
        if hpc_infrastructure_model == "parametric":
            cluster = SDP_PAR_MODEL_LOW()
        elif hpc_infrastructure_model == "cdr":
            cluster = SDP_LOW_CDR()
        else:
            raise RuntimeError(f"{hpc_infrastructure_model} not supported")
        cluster.data_rate_multiplier = data_rate_multiplier
        cluster.set_nodes(compute_nodes)
    else:
        component = common.MID_COMPONENT_SIZING
        system = common.MID_TOTAL_SIZING
        if hpc_infrastructure_model == "parametric":
            cluster = SDP_PAR_MODEL_MID()
        elif hpc_infrastructure_model == "cdr":
            cluster = SDP_MID_CDR()
        else:
            raise RuntimeError(f"{hpc_infrastructure_model} not supported")
        cluster.data_rate_multiplier = data_rate_multiplier
        cluster.set_nodes(compute_nodes)

    LOGGER.info(
        f"\tTelescope: \n"
        f"\tCreating config with:\n"
        f"\tOutput Directory: {output_dir}\n"
        f"\tBuffer ratio: {cluster.buffer_ratio}\n"
        f"\tTimestep: {timestep}\n"
        f"\tData: {data}"
    )

    LOGGER.info("Reading system sizing...")
    component_sizing = pd.read_csv(component)
    system_sizing = pd.read_csv(system)
    cluster_dict = cluster.to_topsim_dictionary()
    observations = hto.process_hpso_from_spec(parameters)

    if not observations:
        RuntimeError('Observations do not exist!')
    LOGGER.debug(f"Creating an observation plan with {observations}")
    all_plans = hto.create_basic_plan(
        observations, telescope.max_stations, with_concurrent=False
    )
    LOGGER.debug(f"Observation plan: {all_plans}")

    # all_plans = hto.alternate_plan_composition(all_plans.pop(), telescope_max)
    import random
    random.shuffle(all_plans)
    # for i, plan in enumerate(all_plans):
    #     print(f"Plan {i} of {len(all_plans)}: {all_plans}")
    # if not multiple_plans:
    #     # Take the middle plan
    #     all_plans = [all_plans[int(len(all_plans)/2)]]
    #     LOGGER.info("Selected plan: %s", all_plans)
    LOGGER.debug("Plans: %s", all_plans)
    LOGGER.info("Final number of plan permutations is: %d", len(all_plans))
    LOGGER.info("Producing the instrument config")
    final_instrument_config = []
    all_plans = [all_plans]
    for observation_plan in all_plans:
        final_instrument_config.append(hto.generate_instrument_config(
            telescope.name,
            telescope.max_stations,
            observation_plan,
            output_dir,
            component_sizing,
            system_sizing,
            cluster_dict,
            base_graph_paths,
            data,
            data_distribution
        ))

    LOGGER.info(f"Producing buffer config")
    final_buffer_config = hto.create_buffer_config(
        cluster
    )
    final_cluster = cluster_dict

    file_paths = []
    LOGGER.info(f"Putting it all together...")
    for i, cfg in enumerate(final_instrument_config):
        final_config = {
            "instrument": cfg,
            "cluster": final_cluster,
            "buffer": final_buffer_config,
            "timestep": timestep
        }

        if not file_path.parent.exists():
            file_path.parent.mkdir(parents=True)
        file_path_cfg = file_path.parent / (file_path.name + f"_{i}" + ".json")
        with file_path_cfg.open('w') as fp:
            LOGGER.info(f'Writing final config to {file_path}')
            json.dump(final_config, fp, indent=2)
            file_paths.append(file_path_cfg)


    LOGGER.info(f'Configuration generation complete!')

    return file_paths


def config_to_shadow(cfg_path: Path) -> dict:
    """
    Convert the SDP system configuration to SHADOW format
    See: https://github.com/myxie/shadow
    Parameters
    ----------
    cfg_path :

    Returns
    -------
    cluster : dictionary of the cluster machines as nodes
    """
    with cfg_path.open() as fp:
        cfg = json.load(fp)
    cluster = {'system': cfg["cluster"]["system"]}
    return cluster
