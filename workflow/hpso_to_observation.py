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

import workflow.eagle_daliuge_translation as edt
from workflow.common import Baselines, MAX_CHANNELS, MAX_TEL_DEMAND, \
    pipeline_paths, component_paths


class HPSO(Enum):
    ingest = ['ingest, rcal, ']
    hpso1 = ['ingest', 'dprepa, dprepb,dprepc,dprepd']
    hpso2a = ['ingest']


class Observation:
    """
    Helper-class to store information for when generating observation schedule

    Parameters
    -----------
    count : int
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
            count, hpso, workflows, demand, duration,
            channels, baseline
    ):
        self.telescope = "low"
        self.count = count
        self.hpso = hpso
        self.demand = demand
        self.duration = duration
        self.workflows = workflows
        self.channels = channels
        self.baseline = Baselines[baseline]
        self.workflow_path = None

    def unroll_observations(self):
        """
        Observation objects store the number of observations that will appear
        in the mid-term plan

        Returns
        -------

        """
        obslist = []
        for i in range(self.count):
            obslist.append(self)
        return obslist

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
    plan = [(0, -1, 0, '', '', 0, '')]
    while observations:
        observations = sorted(
            observations, key=lambda obs: (obs.baseline.value, obs.channels)
        )
        if loop_count % len(observations) == 0:
            start, finish, demand, hpso, pipeline, channels, baseline = plan[-1]
            largest = observations[-1]
            if finish == -1:  # Then we are the first with this time
                plan.pop()
                plan.append(create_observation_plan_tuple(largest, start))
                current_tel_usage += largest.demand
                observations.remove(largest)
                loop_count += 1
            else:  # We have to check the telescope capacity
                if current_tel_usage + largest.demand > max_telescope_usage:
                    loop_count += 1
                    continue
                else:
                    plan.append((create_observation_plan_tuple(largest, start)))
                    current_tel_usage += largest.demand
                    observations.remove(largest)
                    loop_count += 1
        else:  # we are not looking to add the largest:
            start, finish, demand, hpso, pipeline, channels, baseline = plan[-1]
            if finish == -1:
                plan.pop()
            # See if we can squeeze in a few observations
            for observation in observations:
                if (current_tel_usage + observation.demand
                        <= max_telescope_usage):
                    plan.append(create_observation_plan_tuple(observation,
                                                              start))
                    current_tel_usage += observation.demand
                    observations.remove(observation)
                    loop_count += 1
            start, finish, demand, hpso, pipeline, channels, baseline = plan[-1]
            next_time_frame = (finish, -1, 0, '', '', 0, '')
            current_tel_usage = 0
            plan.append(next_time_frame)

    return plan


def create_observation_plan_tuple(observation, start):
    """
    An observation can be represented as a tuple with a given start time,
    for the purpose of transforming into JSON

    Parameters
    ----------
    observation
    start

    Returns
    -------

    """
    start = start
    finish = start + observation.duration
    hpso = observation.hpso
    pipeline = observation.pipeline
    demand = observation.demand
    channels = observation.channels
    baseline = observation.baseline
    tup = (start, finish, demand, hpso, pipeline, channels, baseline)
    return tup


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


def construct_telescope_config_from_observation_plan(
        plan, total_system_sizing, component_system_sizing, itemised_spec
):
    """
    Based on a simplified dictionaries built from the System Sizing for the
    SDP, this function builds a mid-term plan based on the HPSO sizes.


    Paremeters
    ----------
    plan: list
        list of observation tuples, in the form
            (start, finish, telescope demand, hpso, pipeline, channels).
    total_system_sizing : :py:obj:`pd.DataFrame`
        This is the system sizing that contains total system costs. It is
        from this that we get the maximum number of arrays available,
        and the baseline-dependent ingest rate for the selected HPSO.
    component_system_sizing: :py:obj:`pd.DataFrame`
        Component system sizing contains the predicted FLOPS/GBs-1 output of
        each task within a workflow.

    itemised_spec: :py:obj:
        This is the itemised system sizing that provides node-based
        descriptions for system specifications. This allows us to determine
        how many nodes are required for ingest, given an observations
        requirements.

    Notes
    -----
    Currently, the relevant `hpconfig` objects will be the specs.sdp. In the
    future, it may be possible to use any system specification to determine
    what different SDP candidate specifications look like.

    Returns
    -------
    config : dict()
        A dictionary that is in the form required of the TOpSim simulator to
        configure a Telescope (see topsim.core.telescope.Telescope).
    """

    # Calculate expected ingest rate and demand for the observation
    observation_list = []
    system_sizing = pd.read_csv(total_system_sizing)
    for i, observation in enumerate(plan):
        start, finish, demand, hpso, pipeline, channels, baseline = observation

        # TODO Find correct system sizing from observation baseline
        # total_system.lookup(baseline).max_telescope
        # percentage = channels/max_channels

        tel_pecentage = channels / float(MAX_CHANNELS)
        # Calculate the ingest for this size of observation, where ingest is
        # in Petaflops
        # TODO use CDR class for maximum system sizing values
        # total_system.lookup(baseline).lookup(observation).ingest
        max_buffer_ingest = float(system_sizing[
                                      system_sizing['HPSO'] == observation.hpso
                                      ]['Ingest [Pflop/s]'])
        ingest_buffer_rate = tel_pecentage * max_buffer_ingest

        tmp_dict = {
            'name': f"{hpso}-{i}",
            'start': start,
            'demand': demand,
            'data_product_rate': ingest_buffer_rate
        }
        observation_list.append(tmp_dict)

    return observation_list


def telescope_max(system_sizing, baseline):
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

    bl = Baselines[baseline].value
    tmax = max(system_sizing[system_sizing['Baseline'] == bl]['Stations'])

    return tmax


def generate_workflow_from_observation(
        observation,
        telescope_max,
        base_dir,
        component_sizing):
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

    base_dir: The directory in which the workflow will be produced

    Returns
    -------
    path to JSON file for associated workflow

    """
    if not os.path.exists(base_dir):
        raise FileNotFoundError(f'{base_dir} does not exist')
    elif not os.path.exists(f'{base_dir}/config'):
        os.mkdir(f'{base_dir}/config')
        os.path.exists(f'{base_dir}/config/worklows')
    elif not os.path.exists(f'{base_dir}config/worklows'):
        os.path.exists(f'{base_dir}/config/worklows')

    telescope_frac = observation.demand / telescope_max

    base_graph = pipeline_paths[observation.hpso][observation.pipeline]
    channels = observation.channels
    channel_lgt = edt.update_number_of_channels(base_graph, channels)
    # Unroll the graph
    final_graphs = {}
    for workflow in observation.workflows:
        intermed_graph, task_dict = edt.eagle_to_nx(
            channel_lgt, workflow, file_in=False
        )
        intermed_graph = generate_cost_per_product(
            channel_lgt, task_dict, workflow, observation.hpso,
            component_sizing
        )
        final_graphs[workflow] = intermed_graph

    final_workflow = edt.concatenate_workflows(final_graphs)
    final_json = produce_final_workflow_structure(final_workflow, time=False)

    final_path = (
        f'{observation.name}_channels-{channels}_tel-{telescope_frac}.json'
    )
    with open(final_path) as fp:
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
        'UpdateGSM', 'UpdateLSM', 'BeginMajorCycle', 'FinishMajorCycle',
        'FinishMinorCycle', 'BeginMinorCycle', 'Gather', 'Scatter',
        'FrequencySplit'
    ]
    # EAGLE : System Sizing
    component_translate = {'Subtract': 'Subtract Visibility'}

    # total_product_costs = {}
    # Precompute per-component costs, assuming equal split accross component
    for component in task_dict:
        if component in ignore_components:
            continue
        if component in component_translate:
            total = isolate_component_cost(
                observation.hpso, observation.baseline, workflow,
                component_translate[component], component_sizing
            )
        else:
            total = isolate_component_cost(
                observation.hpso, observation.baseline, workflow,
                component, component_sizing
            )
        task_dict[component]['total'] = total
        task_dict[component]['fraction'] = total / task_dict[component]['node']

    for node in nx_graph.nodes:
        workflow, component, index = node.split('_')
        if component in ignore_components:
            continue

        nx_graph.nodes[node]['comp'] = task_dict[component]['fraction']

    return nx_graph


def generate_computation_cost_per_component():
    """
    For the workflow type, generate comp cost for the component
    """
    pass


def generate_data_cost_per_component():
    """
    for the workflow type, generate data cost for the component
    (this is absorbed as an edge cost).
    """
    pass


def isolate_component_cost(hpso, baseline, workflow, component,
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

    Returns
    -------

    """
    obs_frame = component_sizing[
        (component_sizing['hpso'] == hpso) &
        (component_sizing['Baseline'] == baseline.value)
        ]

    cost = float(obs_frame[obs_frame['Pipeline'] == workflow][component])

    return cost


def produce_final_workflow_structure(nx_final, time=False):
    """
    For a given logical graph template, produce a workflow with the specific
    number of channels and return it as a JSON serialisable dictionary.

    Parameters
    ----------
    nx_final : :py:obj:`networkx.DiGraph`
    time: bool, default=False
        The unit in which computation 'cost'. Historically, task DAG
        scheduling has used the total seconds it takes to run a task

    Returns
    -------

    """
    jgraph = {
        "header": {
            "time": time,
        },
        "graph": nx.readwrite.node_link_data(nx_final)
    }
    return jgraph


def assign_costs_to_workflow(workflow, costs, observation, system_sizing):
    """
    For a given set of costs, calculate the amount per-task in the workflow
    is necessary.
    Parameters
    ----------
    workflow : dictionary
        Dictionary representation of JSON object that is converted from EAGLE
        LGT through DALiuGE
    costs : dict
        product-compute costs (petaflops/s) pairs for each component in the
        workflow
    observation : dict
        This is a list of requirements associated with the observation,
        which we use to determine the amount of compute associated with it
        e.g. length (max 5 hours), number of baselines (max 512) etc.

    Notes
    -----
    The idea is that for a given component (e.g. Grid) there is a set compute

    Returns
    -------

    """
    pipelines = {
        "pipelines": {}
    }

    # generate pipeline total ingest costs:
    # max_ingest = system_sizing[
    #     system_sizing['HPSO'] == 'hpso01'
    #     ]['Total Compute Requirement [PetaFLOP/s]']

    observation_ingest = tel_pecentage * (float(max_ingest) * SI.peta)
    tel_pecentage = channels / float(MAX_CHANNELS)

    ingest_cluster_demand = _find_ingest_demand(cluster, observation_ingest)

    final_workflow = []
    ecounter = {}

    # count prevalence of each component in the workflow
    for element in workflow:
        if 'outputs' in element.keys():
            if element['nm'] in ecounter:
                ecounter[element['nm']] += 1
            else:
                ecounter[element['nm']] = 1

    for element in workflow:
        if 'outputs' in element.keys():
            name = element['nm']
            if name in ecounter:
                if costs[name] == -1:
                    continue
                else:
                    individual_cost = costs[name] / ecounter[name] * SI.peta
                    element['tw'] = individual_cost
                    final_workflow.append(element)
        else:
            final_workflow.append(element)

    return final_workflow


def _find_ingest_demand(cluster, ingest_flops):
    """
    Get the average compute over teh CPUs in the cluster and determine the
    number of resources necessary for the current ingest_flops

    "cluster": {
        "header": {
            "time": "false",
            "generator": "hpconfig",
            "architecture": {
                "cpu": {
                    "XeonIvyBridge": 50,
                    "XeonSandyBridge": 100
                },
                "gpu": {
                    "NvidiaKepler": 64
                }
            }
        },

    """
    arch = cluster['cluster']['header']['architecture']
    cpus = arch['cpu'].keys()
    m = 0

    for cpu in cpus:
        m += cluster['cluster']['system']['resources'][f'{cpu}_m0'][
            'flops']
    sys_average = m / len(cpus)
    num_machines = math.ceil(ingest_flops / sys_average)
    return num_machines


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
