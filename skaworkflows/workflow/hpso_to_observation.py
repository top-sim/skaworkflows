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

import json
import os
import math
import pandas as pd
import networkx as nx

from enum import Enum

import skaworkflows.workflow.eagle_daliuge_translation as edt
from skaworkflows.common import (
    Baselines, MAX_BIN_CHANNELS, pipeline_paths, SI,
    WORKFLOW_HEADER, BASE_GRAPH_PATH,
    MAX_TEL_DEMAND, TOTAL_SIZING_LOW)


class HPSO(Enum):
    ingest = ['ingest, rcal, ']
    hpso1 = ['ingest', 'dprepa, dprepb,dprepc,dprepd']
    hpso2a = ['ingest']


def create_observation_from_hpso(
        count, hpso, workflows, demand, duration, channels, baseline
):
    """
    Observation objects store the number of observations that will appear
    in the mid-term plan

    Returns
    -------

    """
    obslist = []
    for i in range(count):
        obs = Observation(
            f'{hpso}_{i}', hpso, workflows, demand, duration, channels,
            baseline
        )
        obslist.append(obs)
    return obslist


class Observation:
    """
    Helper-class to store information for when generating observation schedule

    Parameters
    -----------
    id : str
        The number of the observations wanted in the schedule
    hpso : str
        The high-priority science project the observation is associated with
    duration : int
        The duration of the observation in minutes
    workflows : list()
        The imaging pipeline to process the observation data
    channels : int
        The nunber of averaged channels expected to make up a workflow
    baseline: str
        The length of the baseline used in observation. See Baselines Enum
        class.
    """

    def __init__(
            self,
            name, hpso, workflows, demand, duration,
            channels, baseline
    ):
        self.name = name
        self.telescope = "low"
        self.hpso = hpso
        self.demand = demand
        self.start = 0
        self.duration = duration
        self.workflows = workflows
        self.channels = channels
        self.baseline = Baselines[baseline]
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
        return hash(self.hpso + (
            str(self.demand + self.channels + self.baseline.value)))

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def add_workflow_path(self, path):
        """
        Add the workflow path to observation
        Parameters
        ----------
        path : str
            Path to the workflow in JSON format

        Returns
        -------

        """
        if path:
            raise RuntimeError('Path already defined')
        else:
            self.workflow_path = path
            return True

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
            'name': self.name,
            'start': self.start,
            'duration': self.duration,
            'demand': self.demand,
            'type': self.hpso,
            'data_product_rate': self.ingest_data_rate
        }


def create_observation_plan(observations, max_telescope_usage):
    """
    Given a sequence of HPSOs that are present in the system sizing
    dictionary, generate a plan. of observations from which we can create
    telescope config.


    Parameters
    ----------
    observations : list
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
    # current_end = 0
    # Looping to fill up telescope resources as much as possible.
    # Want plan =[(0,60,32,'hpso01','dprepa'),(60,-1,0)]
    current_start = 0
    current_tel_usage = 0
    loop_count = 0
    # start, finish, demand, hpso, pipeline, channels, baseline
    # plan = [(0, -1, 0, '', '', 0, '')]
    start = 0
    finish = -1
    while observations:
        observations = sorted(
            observations, key=lambda obs: (obs.baseline.value, obs.duration)
        )
        if (len(observations) > 1) and (loop_count % len(observations) == 0):
            largest_observation = observations[-1]
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
                if (
                        current_tel_usage + largest_observation.demand
                        > max_telescope_usage
                ):
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
                if observation.planned:
                    continue
                if (current_tel_usage + observation.demand
                        <= max_telescope_usage):
                    observation.add_start_time(start)
                    observation.planned = True
                    plan.append(observation)
                    current_tel_usage += observation.demand
                    # observations.remove(observation)
                    loop_count += 1
                    if finish < start + observation.duration:
                        finish = start + observation.duration
            start = finish
            finish = -1
            current_tel_usage = 0
        observations = [
            observation for observation in
            observations if not observation.planned
        ]

    return plan


def create_buffer_config(itemised_spec, ratio):
    """
    Generate the buffer configuration from spec, given the provided ratio of
    HotBuffer:ColdBuffer

    Parameters
    ----------
    itemised_spec : hpconfig.ARCHITECTURE
    ratio : tuple
        ratio of buffer size (HotBuffer:ColdBuffer)

    Returns
    -------
    spec : dict
        Dictionary

    """
    spec = {
        "buffer": {
            "hot": {
                "capacity": -1,
                "max_ingest_rate": -1
            },
            "cold": {
                "capacity": -1,
                "max_data_rate": -1
            }
        }
    }
    hot, cold = ratio
    spec['buffer']['hot']['capacity'] = int(
        itemised_spec.total_storage * (hot / cold)
    )
    spec['buffer']['cold']['capacity'] = int(
        itemised_spec.total_storage * (1 - (hot / cold))
    )
    spec['buffer']['hot']['max_ingest_rate'] = int(
        itemised_spec.total_bandwidth * (hot / cold)
    )
    spec['buffer']['cold']['max_data_rate'] = itemised_spec.ethernet
    return spec


def telescope_max(system_sizing, observation):
    """

    Parameters
    ----------
    system_sizing : pd.DataFrame
        Dataframe using our translated system sizing data produced by
        `data.pandas_system_sizing`a
    baseline

    Returns
    -------

    """

    bl = observation.baseline.value
    tmax = max(system_sizing[system_sizing['Baseline'] == bl]['Stations'])

    return tmax


def assign_observation_ingest_demands(
        observation_plan, cluster, system_sizing,
        maximum_telescope=MAX_TEL_DEMAND
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
        o.ingest_compute_demand, o.ingest_flops_rate, o.ingest_data_rate = (
            calc_ingest_demand(o, maximum_telescope, system_sizing, cluster)
        )

    return observation_plan


def generate_instrument_config(
        observation_plan,
        maximum_telescope,
        config_dir,
        component_sizing,
        system_sizing,
        cluster
):
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

    Returns
    -------

    """

    # telescope_max,
    # config_dir,
    # component_sizing,

    pipeline_dict = {}
    telescope_observations = []
    observation_plan = assign_observation_ingest_demands(
        observation_plan=observation_plan,
        cluster=cluster,
        system_sizing=system_sizing,
        maximum_telescope=512
    )

    for observation in observation_plan:
        if not observation.planned:
            raise RuntimeError(
                "Please ensure you run 'create_observation_plan' "
                "prior to generating instrument config."
            )
        # create workflow
        final_workflow_path = None
        workflow_path_name = _create_workflow_path_name(observation)
        if not os.path.exists(config_dir):
            os.mkdir(config_dir)
        if not os.path.exists(f'{config_dir}/workflows/{workflow_path_name}'):
            final_workflow_path = generate_workflow_from_observation(
                observation, maximum_telescope, config_dir,
                component_sizing, workflow_path_name
            )
        else:
            final_workflow_path = f'{config_dir}/workflows/{workflow_path_name}'
        if not observation.ingest_data_rate:
            raise RuntimeError(
                "Please ensure you run 'assign_observation_ingest_demands' "
                "prior to generating instrument config."
            )

        pipeline_dict[observation.name] = {
            "ingest_demand": observation.ingest_compute_demand,
            "workflow": final_workflow_path
        }
        telescope_observations.append(observation.to_json())

    telescope_observations.sort(key=lambda d: d['start'])
    telescope_dict = {
        'total_arrays': maximum_telescope,
        'pipelines': pipeline_dict,
        'observations': telescope_observations
    }
    return telescope_dict


def _create_workflow_path_name(observation):
    return (
            f'{observation.hpso}_time-{observation.duration}'
            + f'_channels-{observation.channels}_tel-'
            + f'{observation.demand}.json'
    )


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
        workflow_path_name,
        concat=True
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

    config_dir: The directory in which the workflow will be produced. This
    should be generated by a previous function.

    Returns
    -------
    path to JSON file for associated workflow

    """

    workflow_dir = f'{config_dir}/workflows'
    if not os.path.exists(config_dir):
        raise FileNotFoundError(f'{config_dir} does not exist')
    elif not os.path.exists(f'{config_dir}/workflows'):
        os.mkdir(f'{config_dir}/workflows')

    telescope_frac = observation.demand / telescope_max

    # base_graph = pipeline_paths[observation.hpso][observation.hpso]
    channels = observation.channels
    # Unroll the graph
    final_graphs = {}
    for workflow in observation.workflows:
        channel_lgt = edt.update_number_of_channels(BASE_GRAPH_PATH, channels)
        intermed_graph, task_dict = edt.eagle_to_nx(
            channel_lgt, workflow, file_in=False
        )
        intermed_graph, task_dict = generate_cost_per_product(
            intermed_graph, task_dict, observation, workflow,
            component_sizing
        )
        final_graphs[workflow] = intermed_graph

    final_workflow = edt.concatenate_workflows(final_graphs,
                                               observation.workflows)
    final_json = produce_final_workflow_structure(final_workflow,
                                                  observation, time=False)

    final_path = (
            f'{workflow_dir}/'
            + f'{workflow_path_name}'
    )
    with open(final_path, 'w') as fp:
        json.dump(final_json, fp, indent=2)

    return final_path


def generate_cost_per_product(
        nx_graph,
        task_dict,
        observation,
        workflow,
        component_sizing,
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

    ignore_components = [
        'UpdateGSM', 'BeginMajorCycle', 'FinishMajorCycle',
        'FinishMinorCycle', 'BeginMinorCycle', 'Gather', 'Scatter',
        'FrequencySplit'
    ]
    # EAGLE : System Sizing
    # total_product_costs = {}
    # Precompute per-component costs, assuming equal split accross component
    for component in task_dict:
        if component in ignore_components:
            continue
        else:
            total_cost, total_data = identify_component_cost(
                observation.hpso, observation.baseline, workflow,
                component, component_sizing
            )
        task_dict[component]['total_cost'] = total_cost
        task_dict[component]['fraction_cost'] = (
                total_cost / task_dict[component]['node']
        )
        task_dict[component]['total_data'] = total_data
        task_dict[component]['fraction_data'] = (
                total_data / task_dict[component]['out_edge']
        )

    # DO Compute
    for node in nx_graph.nodes:
        workflow, component, index = node.split('_')
        if component in ignore_components:
            continue

        nx_graph.nodes[node]['comp'] = (
                observation.duration
                * task_dict[component]['fraction_cost']
                * SI.peta
        )

    # TODO potential refactoring exercise
    # # Do Edges
    # for node
    for edge in nx_graph.edges:
        producer = edge[0]
        workflow, component, index = producer.split('_')
        if component in ignore_components:
            continue
        nx_graph[edge[0]][edge[1]]['data_size'] = (
                observation.duration
                * task_dict[component]['fraction_data']
                * SI.peta
        )

    return nx_graph, task_dict


def identify_component_cost(hpso, baseline, workflow, component,
                            component_sizing):
    """
    Use HPSO and pipeline information to generate the correct workflow
    information

    Parameters
    ----------
    hpso : str
        'Name' of the observation, which doubles as the HPSO
    baseline: :py:object:`common.Baseline`
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

    total_cost = 0
    total_data = 0
    if component == 'UpdateLSM':
        for compnt in ['Reprojection Predict', 'Reprojection']:
            cost, data = retrieve_component_cost(
                hpso, baseline, workflow, compnt, component_sizing
            )
            total_cost += cost
            total_data += data

    elif component == 'Grid':
        for compnt in [
            'Grid', 'Phase Rotation Predict', 'Visibility Weighting',
            'Gridding Kernel Update'
        ]:
            cost, data = retrieve_component_cost(
                hpso, baseline, workflow, compnt, component_sizing
            )
            total_cost += cost
            total_data += data

    elif component == 'Degrid':
        for compnt in [
            'Degrid', 'Degridding Kernel Update'
        ]:
            cost, data = retrieve_component_cost(
                hpso, baseline, workflow, compnt, component_sizing
            )
            total_cost += cost
            total_data += data

    elif component == 'Predict':
        for compnt in ['DFT', 'IFFT']:
            cost, data = retrieve_component_cost(
                hpso, baseline, workflow, compnt, component_sizing
            )
            total_cost += cost
            total_data += data

    elif component == 'Subtract':
        for compnt in ['Subtract Visibility']:
            cost, data = retrieve_component_cost(
                hpso, baseline, workflow, compnt, component_sizing
            )
            total_cost += cost
            total_data += data

    elif component == 'Correct':
        for compnt in ['Correct', 'Average']:
            cost, data = retrieve_component_cost(
                hpso, baseline, workflow, compnt, component_sizing
            )
            total_cost += cost
            total_data += data

    else:
        cost, data = retrieve_component_cost(
            hpso, baseline, workflow, component, component_sizing
        )
        total_cost += cost
        total_data += data

    return total_cost, total_data


def retrieve_component_cost(hpso, baseline, workflow, component,
                            component_sizing):
    """

    Parameters
    ----------
    hpso : str
        'Name' of the observation, which doubles as the HPSO
    baseline: :py:object:`common.Baseline`
    workflow : str
    component : str
    component_sizing : :py:obj:`pd.DataFrame`

    Returns
    -------

    """
    # Santiy check for components:
    if workflow not in component_sizing['Pipeline'].values:
        raise RuntimeError(
            f"Workflow string '{workflow}' not present in DataFrame."
            + f"Double check spelling in Observation."
        )

    obs_frame = component_sizing[
        (component_sizing['hpso'] == hpso) &
        (component_sizing['Baseline'] == baseline.value)
        ]
    compute = float(obs_frame[obs_frame['Pipeline'] == workflow][component])
    data = float(
        obs_frame[obs_frame['Pipeline'] == f'{workflow}_data'][component]
    )

    return compute, data


def retrive_workflow_cost(hpso, baseline, workflow, system_sizing):
    """
    For HPSO (e.g. HPSO01a) retrieve total ingest FLOPS for a specific baseline

    Parameters
    ----------
    hpso
    baseline
    workflow
    system_sizing

    Returns
    -------

    """

    obs_frame = system_sizing[
        (system_sizing['HPSO'] == hpso) &
        (system_sizing['Baseline'] == baseline.value)
        ]
    flops = float(obs_frame[workflow])

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

    header = WORKFLOW_HEADER
    header['time'] = time
    header['parameters']['channels'] = observation.channels
    header['parameters']['arrays'] = observation.demand
    header['parameters']['baseline'] = observation.baseline.value
    header['parameters']['duration'] = observation.duration
    jgraph = {
        "header": header,
        "graph": nx.readwrite.node_link_data(nx_final)
    }
    return jgraph


#
# def assign_costs_to_workflow(workflow, costs, observation, system_sizing):
#     """
#     For a given set of costs, calculate the amount per-task in the workflow
#     is necessary.
#     Parameters
#     ----------
#     workflow : dictionary
#         Dictionary representation of JSON object that is converted from EAGLE
#         LGT through DALiuGE
#     costs : dict
#         product-compute costs (petaflops/s) pairs for each component in the
#         workflow
#     observation : dict
#         This is a list of requirements associated with the observation,
#         which we use to determine the amount of compute associated with it
#         e.g. length (max 5 hours), number of baselines (max 512) etc.
#
#     Notes
#     -----
#     The idea is that for a given component (e.g. Grid) there is a set compute
#
#     Returns
#     -------
#
#     """
#     pipelines = {
#         "pipelines": {}
#     }
#
#     # generate pipeline total ingest costs:
#     # max_ingest = system_sizing[
#     #     system_sizing['HPSO'] == 'hpso01'
#     #     ]['Total Compute Requirement [PetaFLOP/s]']
#
#     observation_ingest = tel_pecentage * (float(max_ingest) * SI.peta)
#     tel_pecentage = channels / float(MAX_CHANNELS)
#
#     ingest_cluster_demand = _find_ingest_demand(cluster, observation_ingest)
#
#     final_workflow = []
#     ecounter = {}
#
#     # count prevalence of each component in the workflow
#     for element in workflow:
#         if 'outputs' in element.keys():
#             if element['nm'] in ecounter:
#                 ecounter[element['nm']] += 1
#             else:
#                 ecounter[element['nm']] = 1
#
#     for element in workflow:
#         if 'outputs' in element.keys():
#             name = element['nm']
#             if name in ecounter:
#                 if costs[name] == -1:
#                     continue
#                 else:
#                     individual_cost = costs[name] / ecounter[name] * SI.peta
#                     element['tw'] = individual_cost
#                     final_workflow.append(element)
#         else:
#             final_workflow.append(element)
#
#     return final_workflow


def calc_ingest_demand(observation, telescope_max, system_sizing, cluster):
    """
    Get the average compute over teh CPUs in the cluster and determine the
    number of resources necessary for the current ingest_flops

    """
    #
    # arch = cluster['cluster']['header']['architecture']
    # cpus = arch['cpu'].keys()
    # m = 0

    # TODO To get ingest flops, sum all the flops for the ingest and then
    # divide based on the machine numbers. This is a partial solution,
    # as we don't want to also simulate scheduling the ingest (this is too
    # much overhead for the simulation - especially when we have to assume
    # ingest pipelines will run on time, as they are necessary for the
    # observation to occur.

    telescope_frac = observation.demand / telescope_max
    ingest_flops = retrive_workflow_cost(
        observation.hpso,
        observation.baseline,
        'Ingest [Pflop/s]',
        system_sizing
    ) * SI.peta * telescope_frac
    resources = cluster['system']['resources']
    m = 0
    num_machines = 0
    for machine in resources:
        m += resources[machine]['flops']
        num_machines += 1
        if m > ingest_flops:
            break

    ingest_bytes = retrive_workflow_cost(
        observation.hpso,
        observation.baseline,
        'Ingest Rate [TB/s]',
        system_sizing
    ) * SI.peta * telescope_frac

    return num_machines, ingest_flops, ingest_bytes


def compile_observations_and_workflows(
        input_dir='./',
        output_dir='out/',
        itemised_spec=None,
        buffer_ratio=None

):
    """
    Generate a configuration file given a set of observation descriptions and
    the workflow file paths, as well as the cluster.

    Parameters
    ----------
    input_dir : str
    output_dir : str
        Destination directory for the config and workflow files

    Notes
    ------
    The telescope system sizing is generated by
    :py:obj:`construct_telescope_config_from_observation_plan`,
    which builds the following:

    >>> {
    >>>    "telescope": {
    >>>       "total_arrays": 512,
    >>>       "pipelines": {
    >>>           "DPrepA": {
    >>>               "ingest_demand": 128,
    >>>                "workflow": "final/directory/for/workflow",
    >>>           },
    >>>           "DPrepC": {
    >>>               "ingest_demand": 84,
    >>>                "workflow": "final/directory/for/workflow",
    >>>       },

    :py:obj: `total_arrays` are specified at the beginning of generation.
    Pipelines are constructed based on the observation plan, and their
    :py:obj: `ingest_demand` is calculated based on the Ingest rate of the
    observation in conjunction with the system sizing that is provided.


    >>>    {"observations": [
    >>>            {
    >>>                "name": "hpso01_1",
    >>>                "start": 0,
    >>>                "duration": 10,
    >>>                "demand": 256,
    >>>                "type": "continuum",
    >>>                "data_product_rate": 4
    >>>            }
    >>>        ]
    >>>    },

zx
    >>>    {"cluster": {
    >>>        "header": {
    >>>            "time": "false",
    >>>        },
    >>>        "system": {
    >>>            "resources": {
    >>>                    "cat0_m0": {
    >>>                        "flops": 35000,
    >>>                        "rates": 10
    >>>                    },
    >>>            },
    >>>            "bandwidth": 1.0
    >>>        }
    >>>    },

    >>>     {"buffer": {
    >>>         "hot": {
    >>>             "capacity": 2000000,
    >>>             "max_ingest_rate": 1000
    >>>         },
    >>>         "cold": {
    >>>             "capacity": 5000000,
    >>>             "max_data_rate": 500
    >>>         }
    >>>     }
    >>> }



    Returns
    -------
    Path to a JSON config file.
    """

    # construct_telescope_config_from_observation_plan()
    # for observation in observations:
    #     generate_workflow_from_observation()
    #
    # pass

    # Generate buffer spec
    buffer_spec = create_buffer_config(itemised_spec, buffer_ratio)
