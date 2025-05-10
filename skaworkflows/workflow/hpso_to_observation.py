# Copyright (C) 3/9/20 RW Bunney

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
HPSO observation information is stored in a csv file.

Use pandas to read in the information

This is a test change to determine some PyCharm Functinality.

Important to make disctincctions between Pipeline information and HPSO
information
    - HPSO information is useful for Ingest/Real-time data and compute
    - Pipeline information is how we get the workflow runtime`
        - Multiple-pipelines per HPSO
"""

import datetime
import json
import logging
import math
import os
import random
import sys

import pandas as pd
import networkx as nx

from typing import List, Dict
from enum import Enum
from pathlib import Path

import skaworkflows.workflow.eagle_daliuge_translation as edt

from skaworkflows.common import (
    SI,
    create_workflow_header,
    CONT_IMG_MVP_GRAPH,
    BASIC_PROTOTYPE_GRAPH,
    SCATTER_GRAPH,
    PULSAR_GRAPH,
    BYTES_PER_VIS,
    Telescope
)

LOGGER = logging.getLogger(__name__)


def process_hpso_from_spec(hpsos: dict):
    """
    Pass a JSON dictionary of observations we want to process

    Easier to edit and cleaner to generate multiple observations (doesn't
    rely on

    Parameters
    ----------
    path

    Returns
    -------

    """
    final_obs = []
    # with path.open() as fp:
    #     hpsos = json.load(fp)

    offset = 0
    for h in hpsos["hpsos"]:
        LOGGER.debug(f"{h=}")
        obslist = create_observation_from_hpso(**h, offset=offset)
        offset += len(obslist)
        final_obs += obslist
    return final_obs


def create_observation_from_hpso(
        count,
        hpso,
        workflows,
        demand,
        duration,
        channels,
        workflow_parallelism,
        baseline,
        telescope,
        offset):
    """
     objects store the number of observations that willappear
    in the mid-term plan


    Parameters
    -----------

    offset : int
        id offset used for when unrolling multiple observations of same hpso
        with different specs (e.g. duration or channels).

    Returns
    -------

    """
    obslist = []
    for i in range(count):
        obs = Observation(
            f"{hpso}_{i + offset}",
            hpso,
            workflows,
            demand,
            duration,
            channels,
            workflow_parallelism,
            baseline,
            telescope,
        )
        obslist.append(obs)
    return obslist


class Observation:
    """
    Helper-class to store information for when generating observation schedule
    """

    def __init__(
            self,
            name,
            hpso,
            workflows,
            demand,
            duration,
            channels,
            workflow_parallelism,
            baseline,
            telescope,
    ):
        """
        Parameters
        -----------
        name : str
            The name of the observation
        hpso : str
            The high-priority science project the observation is associated with
        duration : int
            The duration of the observation in minutes
        workflows : list()
            List of paths to imaging pipelines for process the observation data
        channels : int
            Number of channels that are being observed. This is to search the
            'database' of channels
        workflow_parallelism : int
            The nunber of averaged channels expected to make up a workflow.
            This is used as a proxy for the parallelism of the workflow
        baseline: float
            The length of the baseline used in observation.
        """
        self.name = name
        self.telescope = telescope
        self.hpso = hpso
        self.demand = demand
        self.start = 0
        self.duration = duration
        self.workflows = workflows
        self.channels = channels
        self.workflow_parallelism = workflow_parallelism
        self.baseline = baseline
        self.workflow_path = None
        self.planned = False
        self.ingest_compute_demand = None
        self.ingest_flop_rate = None
        self.ingest_data_rate = None

    def __hash__(self):
        """
        Construct a hash of observation parameters to determine if one is
        equivalent to another

        If an observation has the same:
        * HPSO
        * Demand
        * Duration
        * Baseline

        It is the same workflow

        Returns
        -------

        """
        return hash(
            self.name + (str(self.demand + self.workflow_parallelism + int(self.baseline)))
        )

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def add_start_time(self, start):
        self.start = start

    def to_json(self):
        """
        Produce TOPSIM compatible JSON dictionary

        Returns
        -------
        final_dict : dict
            Dictionary of components
        """

        return {
            "name": self.name,
            "start": self.start,
            "duration": self.duration,
            "instrument_demand": self.demand,
            "type": self.hpso,
            "data_product_rate": self.ingest_data_rate,
        }


def create_observation_plan(hpsos, max_telescope_usage):
    """
    Given a sequence of HPSOs that are present in the system sizing
    dictionary, generate a plan. of observations from which we can create
    telescope config.


    Parameters
    ----------
    hpsos : list
        List of :py:obj:`~pipelines.hpso_to_observations.Observations`

    max_telescope_usage: float
        The maximum percentage of the telescope to be occupied at any given
        time. For some simulations, it may be necessary to only 'simulate' a
        smaller demand on the telescope.

    Notes
    -----
    Observation scheduling is normally a challenging process and quite
    bespoke. The observation schedules we generate are therefore going to be
    made according to the following heuristic:

        * Start with the largest observation (size) in the list
            * This is tie broken on duration
        * If there are any more observations that fit on the telescope at the
        the same time, we add these to the plan too.
        * The longest observation should be followed by at least one small
        observation
        * observations that are small are selected until they reach the limit
        * at least 2 smaller observed until the next larger observations are
        selected


    Returns
    -------
    plan : list()
        A list of strings that details the order of HPSOs that will be running
        for a given plan. These HPSOs will be derived from what is in the
        provided system-sizing dictionary.

        These strings are HPSOs - we need to link them to a pipeline as well
        (RCAL/Ingest we can consume together as 'real-time' pipelines,
        and so promote these as the range of compute required for real-time
        execution).
    """

    plan = []

    current_tel_usage = 0
    loop_count = 0
    start = 0
    finish = -1
    LOGGER.debug(f"{hpsos=}")
    observations = [o for o in hpsos]
    while observations:
        LOGGER.debug("Planning observations")
        observations = sorted(
            observations, key=lambda obs: (obs.baseline, obs.duration)
        )
        if (len(observations) > 1) and (loop_count % len(observations) == 0):
            largest_observation = observations[-1]
            LOGGER.debug(f"{largest_observation=}")
            if finish == -1:  # Then we are the first with this time
                # plan.pop()
                largest_observation.add_start_time(start)
                largest_observation.planned = True
                plan.append(largest_observation)
                current_tel_usage += largest_observation.demand
                # observations.remove(largest_observation)
                loop_count += 1
                finish = start + largest_observation.duration
            else:  # We have to check the telescope capacity
                if current_tel_usage + largest_observation.demand > max_telescope_usage:
                    loop_count += 1
                else:
                    largest_observation.add_start_time(start)
                    largest_observation.planned = True
                    plan.append(largest_observation)
                    current_tel_usage += largest_observation.demand
                    # observations.remove(largest_observation)
                    loop_count += 1
                    finish = start + largest_observation.duration

        else:  # we are not looking to add the largest:
            # See if we can squeeze in a few observations
            for observation in observations:
                LOGGER.debug(f"{observation=}")
                if observation.planned:
                    continue
                if current_tel_usage + observation.demand <= max_telescope_usage:
                    observation.add_start_time(start)
                    observation.planned = True
                    plan.append(observation)
                    LOGGER.debug(f"{plan=}")
                    current_tel_usage += observation.demand
                    # observations.remove(observation)
                    loop_count += 1
                    if finish < start + observation.duration:
                        finish = start + observation.duration
            start = finish
            finish = -1
            current_tel_usage = 0
        observations = [
            observation for observation in observations if not observation.planned
        ]

    LOGGER.debug(f"{plan=}")
    return plan


def create_basic_plan(hpsos, max_telescope_usage, with_concurrent=False,
                      existing_plan=None):
    plan = []

    current_tel_usage = 0
    loop_count = 0
    start = 0
    finish = -1
    LOGGER.debug(f"{hpsos=}")
    if existing_plan:
        observations = [o for o in existing_plan]
    else:
        observations = [o for o in hpsos]
    random.shuffle(observations)
    while observations:
        if with_concurrent:
            for observation in observations:
                if observation.demand > max_telescope_usage:
                    LOGGER.warning("Observation demand exceeds telescope; review config.")
                    sys.exit(1)
                LOGGER.debug(f"{observation=}")
                if observation.planned:
                    continue
                if current_tel_usage + observation.demand <= max_telescope_usage:
                    observation.add_start_time(start)
                    observation.planned = True
                    plan.append(observation)
                    LOGGER.debug(f"{plan=}")
                    current_tel_usage += observation.demand
                    if finish < start + observation.duration:
                        finish = start + observation.duration
            start = finish
            finish = -1
            current_tel_usage = 0
        else:
            for observation in observations:
                observation.add_start_time(start)
                observation.planned = True
                plan.append(observation)
                LOGGER.debug(f"{plan=}")
                current_tel_usage += max_telescope_usage
                if finish < start + observation.duration:
                    finish = start + observation.duration
                start = finish
                finish = -1
            # current_tel_usage = 0
        observations = [
            observation for observation in observations if not observation.planned
        ]

    return plan


# def reset_observation_plan_times(observation_plan: list, with_concurrent=False):


import copy


def alternate_plan_composition(observation_plan: list, max_telescope_usage,
                               with_concurrent=False):
    """
    Pick the largest ¨n" observations, where n is passed as a parameter
    create two lists, one without the observation, and one with only the observation
    iterate through the list, inserting the observation at each index throughout the
    plan.

    Parameters
    ----------
    observation_plan

    Returns
    -------

    """
    # TODO use the shuffle function
    lol = []
    lol.append(copy.deepcopy(observation_plan))
    largest = sorted(observation_plan, key=lambda x: (x.demand, x.channels))[-1]
    observation_plan = [o for o in observation_plan if o != largest]
    for i in range(1, len(observation_plan) + 1):
        new_plan = copy.deepcopy(observation_plan)
        large_copy = copy.deepcopy(largest)
        new_plan.insert(i, large_copy)
        # reset plan
        for o in new_plan:
            o.planned = False
            o.start = 0
        new_plan = create_basic_plan(
            hpsos=None,
            max_telescope_usage=max_telescope_usage,
            with_concurrent=with_concurrent,
            existing_plan=new_plan)
        if new_plan not in lol and i%3 == 0:
            lol.append(new_plan)

    with open("/tmp/plans.txt", "w") as fp:
        for l in lol:
            fp.write(f"{str(l)}\n")

    return lol


def create_buffer_config(itemised_spec):
    """
    Generate the buffer configuration from spec, given the provided ratio of
    HotBuffer:ColdBuffer

    Parameters
    ----------
    itemised_spec : hpconfig.ARCHITECTURE
    ratio : tuple
        ratio of buffer size (HotBuffer:ColdBuffer)

    Return  s
    -------
    spec : dict
        Dictionary

    """
    spec = {
        "hot": {"capacity": -1, "max_ingest_rate": -1},
        "cold": {"capacity": -1, "max_data_rate": -1},
    }

    if itemised_spec.buffer_ratio:  # Calculate capacities based on buffer
        hot, cold = itemised_spec.buffer_ratio
        spec["hot"]["capacity"] = int(itemised_spec.total_storage * (hot / cold))
        spec["cold"]["capacity"] = int(itemised_spec.total_storage * (1 - (hot / cold)))
        spec["hot"]["max_ingest_rate"] = int(itemised_spec.ingest_rate)
        spec["cold"]["max_data_rate"] = int(
            itemised_spec.input_transfer_rate / itemised_spec.total_nodes
        )
    else:
        spec["hot"]["capacity"] = int(itemised_spec.total_input_buffer)
        spec["cold"]["capacity"] = int(itemised_spec.total_compute_buffer)
        spec["hot"]["max_ingest_rate"] = int(itemised_spec.ingest_rate)
        spec["cold"]["max_data_rate"] = int(itemised_spec.input_transfer_rate)

    return spec


def telescope_max(system_sizing, observation):
    """

    Parameters
    ----------
    observation
    system_sizing : pd.DataFrame
        Dataframe using our translated system sizing data produced by
        `data.pandas_system_sizing`a
    baseline

    Returns
    -------

    """

    bl = observation.baseline
    tmax = max(system_sizing[system_sizing["Baseline"] == bl]["Stations"])

    return tmax


def assign_observation_ingest_demands(
        observation_plan, cluster, system_sizing
):
    """

    Parameters
    ----------
    observation_plan : list
        List of `Observation` objects
    cluster : `hpconfic.spec`
        This should ideally be an hpconfig spec object
    system_sizing : pandas.DataFrame

    maximum_telescope

    Returns
    -------

    """

    for o in observation_plan:
        (
            o.ingest_compute_demand,
            o.ingest_flops_rate,
            o.ingest_data_rate,
        ) = calc_ingest_demand(o, system_sizing, cluster)
        LOGGER.debug(f"{o.ingest_compute_demand=},{o.ingest_data_rate=}")

    return observation_plan


def generate_instrument_config(
        telescope: str,
        maximum_telescope,
        observation_plan: List[Observation],
        config_dir_path,
        component_sizing,
        system_sizing,
        cluster,
        base_graph_paths,
        data=True,
        data_distribution: str = "standard",
        **kwargs,
) -> dict:
    """
    Produce the `instrument level configuration for a TopSim compatible
    simulation configuration file.

    The instrument config describes within it:

    Notes
    -----
    MAX channels and MAX telescope stations are both used to update the
    compute of an observation. Based on the parametric model, both
    ingest and FLOPs are functions of frequency channels and the number of
    stations. For an :py:object:`skaworkflows.workflow.hpso_to_workflow
    .Observation`, the stations used is observation.demand.

    Parameters
    ----------
    observation_plan  of Observation objects
    observation_plan
    telescope
    maximum_telescope
    config_dir_path
    component_sizing
    system_sizing
    cluster
    base_graph_paths
    data
    data_distribution: str
        Describes where data is allocated on the workflow.
        "standard" will apply data to tasks only.
        "edges" will apply data to both tasks and esges.


    Returns 
    -------
    dict: dictionary of relevant information
    """

    pipeline_dict = {}
    telescope_observations = []
    max_ingest_resources = -1
    observation_plan = assign_observation_ingest_demands(
        observation_plan=observation_plan,
        cluster=cluster,
        system_sizing=system_sizing,
    )
    LOGGER.debug(f"{observation_plan=}")

    for o in observation_plan:
        use_existing_file = False
        if not o.planned:
            raise RuntimeError(
                "Please ensure you run 'create_observation_plan' "
                "prior to generating instrument config."
            )
        # create workflow
        if o.ingest_compute_demand > max_ingest_resources:
            max_ingest_resources = o.ingest_compute_demand
        # cfg_dir_path = Path(config_dir)
        wf_file_name = Path(_create_workflow_path_name(o))

        wf_file_path = config_dir_path / "workflows" / wf_file_name
        if not wf_file_path.exists():
            wf_file_path.parent.mkdir(parents=True, exist_ok=True)

        possible_file_name = _find_existing_workflow(config_dir_path / "workflows",
                                                     o, data, data_distribution)
        if possible_file_name:
            use_existing_file = True
            wf_file_path = config_dir_path / "workflows" / possible_file_name

        if not os.path.exists(wf_file_path) or not use_existing_file:
            wf_file_path = generate_workflow_from_observation(
                o,
                maximum_telescope,
                config_dir_path,
                component_sizing,
                system_sizing,
                wf_file_name,
                base_graph_paths,
                data=data,
                data_distribution=data_distribution,
            )
        else:
            wf_file_path = wf_file_path
        if not o.ingest_data_rate:
            raise RuntimeError(
                "Please ensure you run 'assign_observation_ingest_demands' "
                "prior to generating instrument config."
            )

        pipeline_dict[o.name] = {
            "workflow": wf_file_path.relative_to(config_dir_path).as_posix(),
            "ingest_demand": o.ingest_compute_demand,
            "duration": o.duration,
            "channels": o.channels,
            "workflow_parallelism": o.workflow_parallelism,
            "demand": o.demand,
            "baseline": o.baseline,
            "workflow_type": list(set(base_graph_paths.values())), # TODO convert to set of strings?
            "graph_type": list(set(base_graph_paths.keys())), # TODO As above
            "data": data,
            "data_distribution": data_distribution
        }
        telescope_observations.append(o.to_json())

    telescope_observations.sort(key=lambda d: d["start"])
    telescope_dict = {
        "telescope": {
            "observatory": telescope,
            "max_ingest_resources": max_ingest_resources,
            "total_arrays": maximum_telescope,
            "pipelines": pipeline_dict,
            "observations": telescope_observations,
        }
    }
    return telescope_dict


def _find_existing_workflow(dirname, observation, data, data_distribution):
    """
    "parameters": {
        "max_arrays": 512,
        "channels": 512,
        "arrays": 256,
        "baseline": 65000.0,
        "duration": 18000
    },

    Parameters
    ----------

    Returns
    -------

    """
    pathname = ""
    header = {"parameters": {}}
    header["parameters"]["workflow_parallelism"] = observation.workflow_parallelism
    header["parameters"]["channels"] = observation.channels
    header["parameters"]["arrays"] = observation.demand
    header["parameters"]["baseline"] = observation.baseline
    header["parameters"]["duration"] = observation.duration
    header["parameters"]["workflows"] = observation.workflows
    header["parameters"]["hpso"] = observation.hpso
    # TODO Fix this so it is based on telescope
    header["parameters"]["max_arrays"] = Telescope(observation.telescope).max_stations
    header["parameters"]["data"] = data
    header["parameters"]["data_distribution"] = data_distribution

    # TODO consider caching this information
    for wf in os.listdir(dirname):
        if ".csv" not in wf:
            with open(dirname / wf) as fp:
                wf_dict = json.load(fp)
                if header["parameters"] == wf_dict["header"]["parameters"]:
                    pathname = wf
                    break

    return pathname


def _create_workflow_path_name(
        observation
):
    str_date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return f"{hash(observation)}_{str_date}"


def create_single_observation_for_instrument(observation, workflow_path):
    """
    Given an observation, generate the following two components for the
    telescope configuration:

    * pipeline:

    >>>   {{
    >>>         "observation": {
    >>>             "workflow": "path/to/workflow"
    >>>             "ingest_demand": number_of_machines_needed_for_ingest
    >>>         }
    >>> }

    * Observation:
    >>> {{
    >>>     "name" : observation.hpso + count
    >>>     "start": observation.start
    >>>     "duration" : length_of_observation
    >>>     "demand" :
    >>>     "data_product_rate": ingest_rate
    >>> }


    Returns
    -------

    """


def generate_workflow_from_observation(
        observation,
        telescope_max,
        config_dir,
        component_sizing,
        system_sizing,
        workflow_path_name,
        base_graph_paths,
        concat=True,
        data=True,
        data_distribution="standard",
):
    """
    Given a pipeline and observation specification, generate a workflow file
    in the provided directory according to observation specifications,
    including telescope usage and frequency channels.

    Parameters
    ----------
    observation : :py:obj:`~hpso_to_observation.Observation`.
        Observation descriptor object
    telescope_max: int
        The maximum number of arrays used on the telescope
    component_sizing : pd.DataFrame
        Data frame that stores information of system sizing. See
        common.SIZING for a dictionary mapping saved column names
        to human readable names.
    base_graph_path : Path
        Path to the EAGLE graph that forms the base logical structure for the HPSO

    config_dir: The directory in which the workflow will be produced. This
    should be generated by a previous function.
    concat : True
        True if we want to pipeline the workflows together into one 'SuperDAG'
    data : bool
        Flag for writing data costs to edges. Default to True as it makes
        more sense from a workflow perspective. False if we want it 0 for
        testing/experimental purposes.

    Returns
    -------
    path to JSON file for associated workflow

    """

    workflow_dir = f"{config_dir}/workflows"
    if not os.path.exists(config_dir):
        raise FileNotFoundError(f"{config_dir} does not exist")
    if not os.path.exists(f"{config_dir}/workflows"):
        os.mkdir(f"{config_dir}/workflows")

    telescope_frac = observation.demand / telescope_max

    channels = observation.workflow_parallelism
    # Unroll the graph
    final_graphs = {}
    cached_base_graph = {}
    workflow_stats = {}
    for workflow in observation.workflows:
        base_graph_type = base_graph_paths[workflow]
        base_graph = _match_graph_options(base_graph_type)
        if base_graph not in cached_base_graph:
            cached_base_graph[base_graph] = None
        LOGGER.debug(f"Using {base_graph} as base workflow.")
        channel_lgt = edt.update_graph_parallelism(
            base_graph, channels, observation.demand
        )
        intermed_graph, task_dict, cached_base_graph[base_graph] = (
            edt.eagle_to_nx(
                channel_lgt,
                workflow,
                file_in=False,
                cached_workflow=cached_base_graph[base_graph]
            )
        )

        final_path = f"{workflow_dir}/" + f"{workflow_path_name}"
        if base_graph_type == "pulsar":
            intermed_graph, task_dict = generate_cost_per_total_workflow(intermed_graph,
                                                                         observation,
                                                                         system_sizing)
            final_graphs[workflow] = intermed_graph
        else:
            intermed_graph, task_dict = generate_cost_per_product(
                intermed_graph,
                task_dict,
                observation,
                workflow,
                component_sizing,
                data=data,
                data_distribution=data_distribution,
            )
            final_graphs[workflow] = intermed_graph
        workflow_stats[workflow] = task_dict

    write_workflow_stats_to_csv(workflow_stats, final_path)
    final_workflow = edt.concatenate_workflows(final_graphs, observation.workflows)
    final_json = produce_final_workflow_structure(
        final_workflow, observation, data, data_distribution, time=False
    )

    with open(final_path, "w") as fp:
        json.dump(final_json, fp, indent=2)

    return Path(final_path)


def _match_graph_options(graph_type: str):
    """
    Given the path
    Parameters
    ----------
    graph_type: str
        Which base graph options we have

    Returns
    -------

    """

    if graph_type == "prototype":
        return BASIC_PROTOTYPE_GRAPH
    elif graph_type == "cont_img_mvp_graph":
        return CONT_IMG_MVP_GRAPH
    elif graph_type == "scatter":
        return SCATTER_GRAPH
    elif graph_type == "pulsar":
        return PULSAR_GRAPH
    else:
        raise RuntimeError(
            f"graph_type {graph_type} unsupported\n"
            f"Currently support prototype or scatter."
        )


def generate_cost_per_product(
        nx_graph,
        task_dict,
        observation,
        workflow,
        component_sizing,
        data=True,
        data_distribution="standard",
        final_path=None,
):
    """
    Produce a cost value per node within the workflow graph for the given
    product.

    For a given workflow, there will be a product from the HPSO (e.g. Grid,
    Subtract Image etc.) which, based on the SDP Parametric Model,
    has a value which is the total expected PFLOP/s expected for that
    component over the lifetime of the workflow.

    As per sdp-par-model.parameters.equations., we know
    the PFlop/s is generated by dividing by o.Tobs (the observation time in
    seconds). From this we can back-calculate total FLOPS/product for the
    entire workflow, and then divide this based on the number of
    product-tasks we have within the workflow.

    Parameters
    ----------
    nx_graph : :py:object:`networkx.DiGraph`
        Topsim-compliant that forms the basis of the workflow

    observation : :py:object:`hpso_to_observation.Observation`
        the HPSO we are generating.

    component_sizing : pd.DataFrame
        Pandas dataframe containing the components


    Returns
    -------

    """
    # TODO lets re-engineer this to directly allocate costs to the nodes

    # Ignore components that feature in either:
    # - The EAGLE logical graph, that does not feature in the parametric model
    # - The parametric model, that is encapsulated by a more generic node in the
    #   EAGLE logical graph.
    #
    # For example:
    # BeginMajorCycle: This is a logical construct to produce a loop in the LGT
    # Visibility Weighting: We group this into the larger "Grid" node in
    # `identify_component_cost`.

    ignore_components = [
        "UpdateGSM",
        "BeginMajorCycle",
        "FinishMajorCycle",
        "FinishMinorCycle",
        "BeginMinorCycle",
        "Gather",
        "Scatter",
        "FrequencySplit",
        "End",
        "CalSourceFinding",
        "SelfCalConverge",
        "ExtractLSM",
        "Raw-Vis-Copy",
        "lstnr",
        "Phase Rotation Predict",
        "Visibility Weighting",
        "Gridding Kernel Update",
        "Phase Rotation",
    ]

    for component in task_dict:
        if component in ignore_components:
            task_dict[component]["total_compute"] = 0
            task_dict[component]["fraction_compute_cost"] = 0
            task_dict[component]["total_data"] = 0
            task_dict[component]["fraction_data_cost"] = 0
        else:
            total_compute, total_data = identify_component_cost(
                observation,
                workflow,
                component,
                component_sizing,
            )

            task_dict[component]["total_compute"] = total_compute
            task_dict[component]["fraction_compute_cost"] = (
                    total_compute / task_dict[component]["node"]
            )
            task_dict[component]["total_data"] = total_data
            task_dict[component]["fraction_data_cost"] = (
                    total_data / task_dict[component]["node"]
            )

    for node in nx_graph.nodes:
        workflow, component, index = node.split("_")
        if component in ignore_components:
            nx_graph.nodes[node]["comp"] = observation.duration
        else:
            compute = (
                    observation.duration
                    * task_dict[component]["fraction_compute_cost"]
                    * SI.peta
            )
            data_cost = (
                    observation.duration
                    * task_dict[component]["fraction_data_cost"]
                    * SI.mega
                    * BYTES_PER_VIS
            )

            if compute > 0:
                nx_graph.nodes[node]["comp"] = compute
            else:
                nx_graph.nodes[node]["comp"] = observation.duration
            if data_cost > 0 and data:
                nx_graph.nodes[node]["task_data"] = data_cost
            else:
                nx_graph.nodes[node]["task_data"] = 0

        num_edges = len(list(nx_graph.predecessors(node)))
        for producer in nx_graph.predecessors(node):
            pworkflow, pcomponent, pindex = producer.split("_")

            if component in ignore_components:
                nx_graph[producer][node]["transfer_data"] = 0
            else:
                data_cost = (
                        observation.duration
                        * task_dict[component]["fraction_data_cost"]
                        * SI.mega
                        * BYTES_PER_VIS
                )
                if data_distribution == "edges":
                    nx_graph[producer][node]["transfer_data"] = data_cost / num_edges
                else:
                    nx_graph[producer][node]["transfer_data"] = 0

    return nx_graph, task_dict


def generate_cost_per_total_workflow(
        nx_graph,
        observation,
        system_sizing,
):
    # Retrieve workflow cost for system sizing
    # Use real time pipeline cost and evaluation mechanism for number of machines
    # allocated

    cost = calc_pulsar_demand(observation,  system_sizing)
    cost_per_task = cost / len(nx_graph.nodes)
    task_dict = {"workflow":["pulsar"], "total_cost": [cost], "cost_per_task": [cost_per_task]}
    # Translate this to all
    for node in nx_graph.nodes:
        nx_graph.nodes[node]["comp"] = cost_per_task * observation.duration * (10**15)
        nx_graph.nodes[node]["task_data"] = 0

        for producer in nx_graph.predecessors(node):
            nx_graph[producer][node]["transfer_data"] = 0

    return nx_graph, task_dict


def _process_task_cost(task_dict, graph):
    return task_dict, graph


def identify_component_cost(
        observation: Observation,
        workflow: Path,
        component,
        component_sizing,
):
    """
    Use HPSO and pipeline information to generate the correct workflow
    information

    Parameters
    ----------
    observation : :py:obj:`Observation`
        The Observation object to which this component is associated
    workflow : str
    component : str
    component_sizing : :py:obj:`pd.DataFrame`

    Notes
    ------
    * The component 'UpdateLSM' subsumes the 'reproject and reproject predict'
    components

    * The Gridding component subsumes Phase Rotation Predict, in addition to
    its own costs.

    * The component 'Predict', which appears in a couple of places in the
    workflow, sources it's cost from DFT and IFFT

    * Subtract is 'subtract visibilities'

    Returns
    -------

    """

    # TODO Consider grouping and iterating over dictionary instead
    total_cost = 0
    total_data = 0
    if component == "UpdateLSM":
        for compnt in ["Reprojection Predict", "Reprojection"]:
            cost, data = retrieve_component_cost(
                observation, workflow, compnt, component_sizing
            )
            total_cost += cost
            total_data += data

    elif component == "Grid":
        for compnt in [
            "Grid",
            "Phase Rotation Predict",
            "Visibility Weighting",
            "Gridding Kernel Update",
            "Phase Rotation",
        ]:
            cost, data = retrieve_component_cost(
                observation, workflow, compnt, component_sizing
            )
            total_cost += cost
            total_data += data

    elif component == "Degrid":
        for compnt in ["Degrid", "Degridding Kernel Update"]:
            cost, data = retrieve_component_cost(
                observation, workflow, compnt, component_sizing
            )
            total_cost += cost
            total_data += data

    elif component == "Predict":
        for compnt in ["DFT", "IFFT"]:
            cost, data = retrieve_component_cost(
                observation, workflow, compnt, component_sizing
            )
            total_cost += cost
            total_data += data

    elif component == "Subtract":
        for compnt in ["Subtract Visibility"]:
            cost, data = retrieve_component_cost(
                observation, workflow, compnt, component_sizing
            )
            total_cost += cost
            total_data += data

    elif component == "Correct":
        for compnt in ["Correct"]:
            cost, data = retrieve_component_cost(
                observation, workflow, compnt, component_sizing
            )
            total_cost += cost
            total_data += data

    else:
        cost, data = retrieve_component_cost(
            observation, workflow, component, component_sizing
        )
        total_cost += cost
        total_data += data

    return total_cost, total_data


def calculate_major_loop_data(
        task_dict, component_sizing, hpso, baseline, workflow, data_distribution
):
    """
    If we want to take data into account in the way that the parametric model does,
    we use their visibility read rate (Rio) value in their model.

    This is supposed to 'be read' only once per major cycle, but there is
    ambiguity around whether that is allocated to a single task, once per
    'set' of major cycle tasks, or once per major cycle & minor cycle tasks.

    The following tasks are in the major cycle (but not in the minor cycle):

        * DeGrid
        * Subtract
        * Flag
        * Grid

    By default we assign the data to the DeGrid as part of its compute task,
    and then to the edges of each subsequent node as a potential 'transfer'
    cost. If `distribute` is `True`, then we average the data across each
    major-loop task above, to determine if it has any impact on the runtime.

    Parameters
    ----------

    Returns
    -------
    task_dict : `dict`
        Modified task dictionary - keeps the functionality pure.
    """
    data = 0
    data_dict = {"Degrid": {}, "Subtract": {}, "Flag": {}, "Grid": {}}
    obs_frame = component_sizing[
        (component_sizing["hpso"] == hpso) & (component_sizing["Baseline"] == baseline)
        ]

    if data_distribution == "distribute":
        raise NotImplementedError(
            "Current functionality does not support distributed data costs"
        )
    elif data_distribution == "standard":
        data = float(
            obs_frame[obs_frame["Pipeline"] == f"{workflow}"]["Visibility read rate"]
        )
        data_dict["Degrid"]["total_io_cost"] = data
        data_dict["Degrid"]["fraction_io_cost"] = data / task_dict["Degrid"]["node"]
        for component in data_dict:
            if component == "Degrid":
                data_dict[component]["total_io_cost"] = data
                data_dict[component]["fraction_io_cost"] = (
                        data / task_dict["Degrid"]["node"]
                )
            else:
                data_dict[component]["total_io_cost"] = 0
                data_dict[component]["fraction_io_cost"] = 0

            data_dict[component]["total_data_cost"] = data
            data_dict[component]["fraction_data_cost"] = (
                    data / task_dict[component]["node"]
            )
    else:
        raise RuntimeError("Passing incorrect data distribution method")

    return data_dict


def retrieve_component_cost(observation, workflow, component, component_sizing):
    """

    Parameters
    ----------
    observation
    workflow : Path
    component : str
    component_sizing : :py:obj:`pd.DataFrame`
    Returns
    -------

    """
    # Santiy check for components:
    if workflow not in component_sizing["Pipeline"].values:
        LOGGER.warning(
            "Workflow string %s not present in DataFrame. Double check spelling in "
            "Observation.", workflow)
        return

    hpso_sizing = component_sizing[component_sizing["hpso"] == observation.hpso]
    # Find closest baseline to the one that is specified
    baseline = min(list(hpso_sizing["Baseline"]),
                   key=lambda x: abs(x - observation.baseline))

    obs_frame = component_sizing[
        (component_sizing["hpso"] == observation.hpso)
        & (component_sizing["Baseline"] == baseline)
        & (component_sizing["Channels"] == observation.channels)
        & (component_sizing["Antenna stations"] == observation.demand)
        ]

    if obs_frame.empty:
        raise ValueError(
            f"Data does not contain the union of "
            f"{observation.hpso} and {observation.baseline};"
            f"please review for errors in user input. "
        )

    if workflow not in component_sizing["Pipeline"].values:
        raise RuntimeError(f"HPSO does not require {workflow} - check HPSO config.")

    compute = float(obs_frame[obs_frame["Pipeline"] == workflow][component].iloc[0])

    data = float(obs_frame[obs_frame["Pipeline"] == f"{workflow}_data"][component].iloc[0])

    return compute, data


def retrieve_workflow_cost(observation, workflow, system_sizing):
    """
    For HPSO (e.g. HPSO01a) retrieve total ingest FLOPS for a specific baseline

    Parameters
    ----------
    observation : Observation
    workflow : str
    system_sizing : pd.Dataframe

    Returns
    -------

    """
    if observation.hpso not in system_sizing["HPSO"].values:
        raise RuntimeError(f"HPSO: {observation.hpso} not present")

    hpso_sizing = system_sizing[system_sizing["HPSO"] == observation.hpso]
    # Find closest baseline to the one that is specified
    baseline = min(list(hpso_sizing["Baseline"]),
                   key=lambda x: abs(x - observation.baseline))

    obs_frame = system_sizing[
        (system_sizing["HPSO"] == observation.hpso)
        & (system_sizing["Baseline"] == baseline)
        & (system_sizing["Channels"] == observation.channels)
        & (system_sizing["Stations"] == observation.demand)
        ]
    flops = float(obs_frame[workflow].iloc[0])

    return flops


def produce_final_workflow_structure(nx_final, observation, data, data_distribution,
                                     time=False):
    """
    For a given logical graph template, produce a workflow with the specific
    number of channels and return it as a JSON serialisable dictionary.

    Parameters
    ----------
    nx_final : :py:obj:`networkx.DiGraph`

    time: bool, default=False
        The unit in which computation 'cost'. Historically, task DAG
        scheduling has used the total seconds it takes to run a task.

    Returns
    -------

    """

    header = create_workflow_header(observation.telescope)
    header["time"] = time
    header["parameters"]["workflow_parallelism"] = observation.workflow_parallelism
    header["parameters"]["channels"] = observation.channels
    header["parameters"]["arrays"] = observation.demand
    header["parameters"]["baseline"] = observation.baseline
    header["parameters"]["duration"] = observation.duration
    header["parameters"]["workflows"] = observation.workflows
    header["parameters"]["hpso"] = observation.hpso
    header["parameters"]["data"] = data
    header["parameters"]["data_distribution"] = data_distribution
    jgraph = {"header": header, "graph": nx.readwrite.node_link_data(nx_final, edges="links")}
    return jgraph


def write_workflow_stats_to_csv(
        workflow_stats: Dict,
        workflow_path_name
):
    """
    Create a and save a CSV table of information for each product and graph
    type used in the final workflow. Intended to facilitate analysis of
    the different system and workflow sizings.

    Parameters
    ----------
    workflow_stats :
        list of dictionaries, each of which describes useful information about
        the task characteristics for a given graph. E.g.
        "ICAL" : {total_compute: 0.5879, fraction_compute_cost: 0.0025...}
    workflow_path_name:
        The path we are putting the workflow; we use this path for our .csv file

    Returns
    -------
    None:
        This produces a csv file.
    """

    if 'Pulsar' in workflow_stats:
        pd.DataFrame(workflow_stats["Pulsar"]).to_csv(f"{workflow_path_name}.csv")
        return

    columns = ['workflow_type', 'product', "total_compute",
               "fraction_compute_cost", 'node',
               'total_data', 'fraction_data_cost']

    d = {c: [] for c in columns}
    for graph in workflow_stats:
        for key, value in workflow_stats[graph].items():
            d["workflow_type"].append(graph)
            d["product"].append(key)
            for k, v in value.items():
                if k in columns:
                    d[k].append(v)

    df = pd.DataFrame(d)
    df = df.rename(columns={"node": "num_tasks"})
    workflow_data_path = f"{workflow_path_name}.csv"
    df.to_csv(workflow_data_path, index=False)


def calc_ingest_demand(observation: Observation, system_sizing: pd.DataFrame, cluster: dict):
    """
    Get the average compute over teh CPUs in the cluster and determine the
    number of resources necessary for the current ingest_flops

    """
    ingest_flops = (
            retrieve_workflow_cost(
                observation, "Ingest [Pflop/s]", system_sizing
            ) * SI.peta
    )
    for name, machine_data in cluster["system"]["resources"].items():
        flops_per_machine = machine_data["flops"]
        num_machines = math.ceil(ingest_flops / flops_per_machine)

    ingest_bytes = (
            retrieve_workflow_cost(
                observation, "Ingest Rate [TB/s]", system_sizing
            ) * SI.tera
    )

    return num_machines, ingest_flops, ingest_bytes

def calc_pulsar_demand(observation, system_sizing):
    """
    Get the average compute over teh CPUs in the cluster and determine the
    number of resources necessary for the current ingest_flops

    """

    pulsar_workflows = ["RCAL [Pflop/s]", "FastImg [Pflop/s]"]

    pulsar_flops =0
    for pw in pulsar_workflows:
        pulsar_flops += (
                retrieve_workflow_cost(observation, pw, system_sizing)
            )

    return pulsar_flops

