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
import uuid

import pandas as pd
import random

from string import ascii_letters

from pathlib import Path

import skaworkflows.common as common
import skaworkflows.workflow.observations_to_workflows as hto
from skaworkflows.common import SKALow, Telescope
from skaworkflows.observation.statistics import observation_weighting

from skaworkflows.observation.observation import (
    process_hpso_from_spec, create_basic_plan, create_concurrent_plan)

from skaworkflows.hpconfig.specs.sdp import (
    SDP_LOW_CDR, SDP_MID_CDR, SDP_PAR_MODEL_LOW, SDP_PAR_MODEL_MID
)
from skaworkflows.observation.permutations import generate_multiple_plans

LOGGER = logging.getLogger(__name__)
MAX_SHUFFLED_PLANS = 100

LOGGER.setLevel('DEBUG')

def create_observing_plans(
        days,
        telescope: Telescope,
        percent_experiments=0.5,
        num_of_plans=0,
        num_shuffled_plans=1,
        concurrent_demand=0
):
    """
    Produce the observing plans based on the observation parameters.
    Returns
    -------

    """
    all_plans = []
    LOGGER.info("Generating observing plan permutations for %d days of observations", days)
    LOGGER.info("Using concurrent demand: %d", concurrent_demand)
    LOGGER.info("Generating %d plans", num_shuffled_plans)
    plans = generate_multiple_plans('low',
                                    days,
                                    percent_experiments=percent_experiments)
    print(f"Number of observation permutations generated: {len(plans)}")
    if num_of_plans > 0:
        plans = random.sample(plans, min(num_of_plans, len(plans)))
        # plans = plans[:num_of_plans]

    print(f"Number of observation permutations selected for this run: {len(plans)}")
    for i, tup in enumerate(plans):
        plan, _ = tup
        current_permutation = []

        weighted_plans = []
        # Generate MAX_SHUFFLED_PLANS that have different variations of the same Observation specs
        # This helps us select an even range of plan weights.
        random.seed(i)
        concurrent_observation_probability = random.random()
        for j in range(MAX_SHUFFLED_PLANS):
            observations = process_hpso_from_spec(plan)
            if concurrent_demand > 0:
                LOGGER.debug("Creating concurrent observing plan with concurrent demand: %d", concurrent_demand)
                weighted_plans.append(create_concurrent_plan(observations, telescope.max_stations,
                                                             64,
                                                             concurrent_observation_probability=concurrent_observation_probability,
                                                             seed=j))
                print(f"Number of observations in permutation {len(weighted_plans[0])}")
            else:
                LOGGER.debug("Creating non-concurrent observing plan")
                weighted_plans.append(create_basic_plan(observations))
                print(f"Number of observations in permutation {len(weighted_plans[0])}")

        plan_weights = [(p, observation_weighting(p)) for p in weighted_plans]
        plan_weights.sort(key=lambda x: x[1])
        # Get equally spaced indices between 0 and MAX_SHUFFLED_PLANS
        indices = [round(i * (MAX_SHUFFLED_PLANS - 1) / (num_shuffled_plans - 1)) for i in range(num_shuffled_plans)]
        selected_plans = [plan_weights[i][0] for i in indices]

        all_plans.append({'obs_plan_id':uuid.uuid4().hex, 'plan_permutation':selected_plans})

    LOGGER.info("Finished producing %d plans",  len(all_plans))
    return all_plans


def create_telescope_infrastructure(telescope):
    """
    Create the HPCConfig dictionary based on the specified infrastructure.

    Supports:
    - SKALow CDR, SKALow Parameteric Model
    - SKAMid CDR, SKALow Parameteric Model

    Parameters
    ----------
    telescope, infrastructure

    Returns
    -------
    dict, computing machine dictionary
    """

    pass

def get_base_graph_paths(workflow_graph_map: str = "prototype"):
        return   {"ICAL":  workflow_graph_map,
                  "DPrepA": workflow_graph_map,
                  "DPrepB": workflow_graph_map,
                  "DPrepC": workflow_graph_map,
                  "DPrepD": workflow_graph_map,
                  "Pulsar": "pulsar"}

def create_maximal_config(
        infrastructure='parametric',
        output_dir: Path=Path.cwd(),
        imaging_graph_base='prototype',
        timestep='seconds',
        overwrite=False,
        **kwargs
):
    pass


def create_config(
        days=1,
        telescope='low',
        infrastructure='parametric',
        output_dir: Path=Path.cwd(),
        imaging_graph_base='prototype',
        timestep='seconds',
        overwrite=False,
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

    try:
        telescope = common.Telescope(telescope)
    except ValueError:
        LOGGER.warning("Unable to create observation plan due to unsupported telescope.\n"
                       "Please use either SKALow or SKAMid as your selection.")

    num_nodes =  kwargs.get("compute_nodes")

    compute_nodes = num_nodes if num_nodes else telescope.default_compute_nodes
    hpc_infrastructure_model = infrastructure

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
    )

    LOGGER.info("Reading system sizing...")
    component_sizing = pd.read_csv(component)
    system_sizing = pd.read_csv(system)
    cluster_dict = cluster.to_topsim_dictionary()

    all_plans = create_observing_plans(
        days,
        telescope,
        num_shuffled_plans=kwargs.get('num_shuffled_plans', 1),
        concurrent_demand=kwargs.get('concurrent_demand', 0),
        num_of_plans=kwargs.get('num_of_plans',1)
    )

    LOGGER.debug("Plans: %s", all_plans)
    LOGGER.info("Final number of plan permutations is: %d", len(all_plans))
    LOGGER.info("Producing the instrument config")
    final_instrument_config = []
    # all_plans = [all_plans]
    if not file_path.parent.exists():
        file_path.parent.mkdir(parents=True)
    base_workflow_graphs = get_base_graph_paths(imaging_graph_base)
    for i, shuffled_plans in enumerate(all_plans):
        j = 0
        obs_plan_id = shuffled_plans.get('obs_plan_id')
        plans = shuffled_plans.get('plan_permutation')
        for j, plan in enumerate(plans):
            cfg_file_path = file_path.parent / (file_path.name + f"_{i}-{ascii_letters[j]}" + ".json")
            LOGGER.info("Config: %d/%d",i+1, len(all_plans))
            final_instrument_config.append((
                cfg_file_path,
                hto.generate_instrument_config(
                    telescope.name,
                    telescope.max_stations,
                    plan,
                    output_dir,
                    component_sizing,
                    system_sizing,
                    cluster_dict,
                    base_workflow_graphs,
                    obs_plan_id=obs_plan_id
            )))
            j+=1

    LOGGER.info(f"Producing buffer config")
    final_buffer_config = hto.create_buffer_config(
        cluster
    )
    final_cluster = cluster_dict

    file_paths = []
    LOGGER.info(f"Putting it all together...")
    for cfg_file_path, cfg in final_instrument_config:
        final_config = {
            "instrument": cfg,
            "cluster": final_cluster,
            "buffer": final_buffer_config,
            "timestep": timestep
        }
        
        with cfg_file_path.open('w') as fp:
            LOGGER.info(f'Writing final config to {cfg_file_path}')
            json.dump(final_config, fp, indent=2)
            file_paths.append(cfg_file_path)

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
    cluster = cfg["cluster"]["system"]
    machines_types = cluster['resources']
    example_key = list(machines_types.keys())[0]
    if not machines_types[example_key].get("count"):
        return {"system": cluster}

    # If we are using the new style, change it to the shadow-compatible style.
    resources = {}
    for machine, spec in machines_types.items():
        for i in range(spec["count"]):
            resources[f"{machine}_{i}"] = spec
    cfg['cluster']['system']['resources'] = resources
    return {'system': cfg["cluster"]["system"]}

