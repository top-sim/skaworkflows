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

import pandas as pd
import networkx as nx

from typing import List, Dict
from pathlib import Path

import skaworkflows.workflow.eagle_daliuge_translation as edt
from skaworkflows.observation.observation import Observation

from skaworkflows.common import (
    SI,
    create_workflow_header,
    CONT_IMG_MVP_GRAPH,
    BASIC_PROTOTYPE_GRAPH,
    PARALLEL_GRAPH,
    PULSAR_GRAPH,
    BYTES_PER_VIS,
    Telescope
)

LOGGER = logging.getLogger(__name__)


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
    .Observation`, the stations used is observation.stations.

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
                                                     o)
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
            "demand": o.stations,
            "baseline": o.baseline,
            "workflow_type": list(set(base_graph_paths.values())), # TODO convert to set of strings?
            "graph_type": list(set(base_graph_paths.keys())), # TODO As above
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

    if kwargs.get("obs_plan_id", None):
        telescope_dict["telescope"]["obs_plan_id"] = kwargs.get("obs_plan_id")

    return telescope_dict


def _find_existing_workflow(dirname, observation):
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
    header["parameters"]["arrays"] = observation.stations
    header["parameters"]["baseline"] = observation.baseline
    header["parameters"]["duration"] = observation.duration
    header["parameters"]["workflows"] = observation.workflows
    header["parameters"]["hpso"] = observation.hpso
    # TODO Fix this so it is based on telescope
    header["parameters"]["max_arrays"] = Telescope(observation.telescope).max_stations

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



def generate_workflow_from_observation(
        observation,
        telescope_max,
        config_dir,
        component_sizing,
        system_sizing,
        workflow_path_name,
        base_graph_paths,
        concat=True,
):
    """
    Given a pipeline and observation specification, generate a workflow file
    in the provided directory according to observation specifications,
    including telescope usage and frequency channels.

    Parameters
    ----------
    observation : :py:obj:`~observations_to_workflows.Observation`.
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

    telescope_frac = observation.stations / telescope_max

    channels = observation.workflow_parallelism
    # Unroll the graph
    final_graphs = {}
    cached_base_graph = {}
    workflow_stats = {}
    for workflow in observation.workflows:
        base_graph_type = base_graph_paths[workflow]
        base_graph = _match_graph_options(base_graph_type)
        LOGGER.info("Using Base Graph: %s", base_graph)
        if base_graph not in cached_base_graph:
            cached_base_graph[base_graph] = None
        LOGGER.debug(f"Using {base_graph} as base workflow.")
        channel_lgt = edt.update_graph_parallelism(
            base_graph, channels, observation.stations
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
            )
            final_graphs[workflow] = intermed_graph
        workflow_stats[workflow] = task_dict

    write_workflow_stats_to_csv(workflow_stats, final_path)
    final_workflow = edt.concatenate_workflows(final_graphs, observation.workflows)
    final_json = produce_final_workflow_structure(
        final_workflow, observation, time=False
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
    -------:


    """

    if graph_type == "prototype":
        return BASIC_PROTOTYPE_GRAPH
    elif graph_type == "complex_img_mvp":
        return CONT_IMG_MVP_GRAPH
    elif graph_type == "parallel":
        return PARALLEL_GRAPH
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

    observation : :py:object:`observations_to_workflows.Observation`
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
            if data_cost > 0:
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
                nx_graph[producer][node]["transfer_data"] = data_cost / num_edges

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
        & (component_sizing["Antenna stations"] == observation.stations)
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
        & (system_sizing["Stations"] == observation.stations)
        ]
    flops = float(obs_frame[workflow].iloc[0])

    return flops


def produce_final_workflow_structure(nx_final, observation, time=False):
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
    header["parameters"]["arrays"] = observation.stations
    header["parameters"]["baseline"] = observation.baseline
    header["parameters"]["duration"] = observation.duration
    header["parameters"]["workflows"] = observation.workflows
    header["parameters"]["hpso"] = observation.hpso
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

