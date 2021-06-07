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
import math
import pandas as pd
from enum import Enum


class Baselines(Enum):
    short = 4062.5
    mid = 32500
    long = 65000


class SI:
    kilo = 10 ** 3
    mega = 10 ** 6
    giga = 10 ** 9
    tera = 10 ** 12
    peta = 10 ** 15


# These are 'binned' channels, by dividing the number of real channels by 64.
MAX_CHANNELS = 512
MAX_TEL_DEMAND = 256


class Observation:
    """
    Helper-class to store information for when generating observation schedule

    Parameters:
    -----------
    count : int
        The number of the observations wanted in the schedule
    hpso : str
        The high-priority science project the observation is associated with
    duration : int
        The duration of the observation in minutes
    pipeline : str
        The imaging pipeline to process the observation data
    channels : int
        The nunber of averaged channels expected to make up a workflow
    baseline: str
        The length of the baseline used in observation. See Baselines Enum
        class.
    """

    def __init__(
            self,
            count, hpso, demand, duration,
            pipeline, channels, baseline
    ):
        self.count = count
        self.hpso = hpso
        self.demand = demand
        self.duration = duration
        self.pipeline = pipeline
        self.channels = channels
        self.baseline = Baselines[baseline]

    def unroll_observations(self):
        obslist = []
        for i in range(self.count):
            obslist.append(self)
        return obslist


compute_keys = {
    'hpso': "HPSO",
    'time': "Time [%]",
    'stations': "Stations",
    'tobs': "Tobs [h]",
    'ingest': "Ingest [Pflop/s]",
    'rcal': "RCAL [Pflop/s]",
    'fastimg': "FastImg [Pflop/s]",
    'ical': "ICAL [Pflop/s]",
    'dprepa': "DPrepA [Pflop/s]",
    'dprepb': "DPrepB [Pflop/s]",
    'dprepc': "DPrepC [Pflop/s]",
    'dprepd': "DPrepD [Pflop/s]",
    'totalrt': "Total RT [Pflop/s]",
    'totalbatch': "Total Batch [Pflop/s]",
    'total': "Total [Pflop/s]",
    'ingest_rate': 'Ingest Rate [TB/s]'
}

compute_units = {
    'hpso': None,
    'time': "%",
    'stations': None,
    'tobs': "h",
    'ingest': "Pflop/s",
    'rcal': "Pflop/s",
    'fastimg': "Pflop/s",
    'ical': "Pflop/s",
    'dprepa': "Pflop/s",
    'dprepb': "Pflop/s",
    'dprepc': "Pflop/s",
    'dprepd': "Pflop/s",
    'totalrt': "Pflop/s",
    'totalbatch': "Pflop/s",
    'total': "Pflop/s",
    'ingest_rate': "TB/s"
}

# Keys for DATA csv
data_keys = {
    'telescope': "Telescope",
    'pipeline': "Pipeline ",
    'datarate': "Data Rate [Gbit/s]",
    'dailygrowth': "Daily Growth [TB/day]",
    'yearlygrowth': "Yearly Growth [PB/year]",
    'five_yeargrowth': "5-year Growth [PB/(5 year)]"
}

hpso_constraints_exclude = {
    'hpso4': ['dprepa', 'dprepb', 'dprepc', 'dprepd'],  # Pulsar
    'hpso5': ['dprepa', 'dprepb', 'dprepc', 'dprepd'],  # Pulsar
    'hpso18': ['dprepa', 'dprepb', 'dprepc', 'dprepd'],  # Transients (FRB)
    'hpso32': ['dprepa', 'dprepc', 'dprepd']  # Cosmology (Gravity)
}

pipeline_constraints = {
    'dprepc': ['dprepa']
}

# Pulsars do not require imaging pipelines
pulsars = ['hpso4a', 'hpso5a']


def convert_systemsizing_csv_to_dict(csv_file):
    csv = pd.read_csv(csv_file)

    # Keys for COMPUTE csv
    reverse_keys = {v: k for k, v in compute_keys.items()}

    df_updated_keys = csv.rename(columns=reverse_keys)

    return df_updated_keys


def create_observation_plan(observations, max_telescope_usage):
    """
    Given a sequence of HPSOs that are present in the system sizing
    dictionary, generate a plan. of observations from which we can create
    telescope config.


    Parameters
    ----------
    dataframe : pandas.DataFrame
        A dataframe describing the potential HPSOs that are to be placed in
        the plan

    per_hpso_count : list()
        A list of Integers that describes how many of each HPSO is in the plan

    max_telescope_usage: float
        The maximum percentage of the telescope to be occupied at any given
        time. For some simulations, it may be necessary to only 'simulate' a
        smaller demand on the telescope.

    Notes
    -----
    Observation scheduling is normally a challenging process and quite
    bespoke. The observation schedules we generate are therefore going to be
    made according to the following heuristic:

        * Start with the largest observation in the list
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
    start = start
    finish = start + observation.duration
    hpso = observation.hpso
    pipeline = observation.pipeline
    demand = observation.demand
    channels = observation.channels
    baseline = observation.baseline
    tup = (start, finish, demand, hpso, pipeline, channels, baseline)
    return tup


def construct_telescope_config_from_observation_plan(
        plan, total_system_sizing
):
    """
    Based on a simplified dictionaries built from the System Sizing for the
    SDP, this function builds a mid-term plan based on the HPSO sizes.


    Paremeters
    ----------
    plan: list
        list of observation tuples, in the form
            (start, finish, telescope demand, hpso, pipeline, channels).
    total_system_sizing : csv
        This is the system sizing that contains total system costs. It is
        from this that we get the ingest rate for the selected HPSO.

    Notes
    -----

    The structure of an observation.json file takes the following:
    {
      "telescope": {
        "total_arrays": # This is adapted from the maximum number of
        'stations' in the system_sizing csv.

        }
        "observations": [
            # a list of observations in the order of the 'plan'
            "name": # this is a place holder
            "start": This is relative to the first, in hours from first
            observation start time
            "duration": Tobs, from system sizing
            "demand": this is the number of stations it requires
            "data_product_rate": This is
        ]
      }
    }

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
        tel_pecentage = channels / float(MAX_CHANNELS)
        # Calculate the ingest for this size of observation, where ingest is
        # in Petaflops
        max_buffer_ingest = float(system_sizing[
                                      system_sizing['HPSO'] == 'hpso01'
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


def generate_workflow_from_observation(observation):
    """
    Given a pipeline and observation specification, generate a workflow file
    and return the path name
    Parameters
    ----------
    observation : dict
        Dictionary of observation data, including duration

    Returns
    -------
            'type':pipeline,

    """
    return None


def generate_cost_per_product(workflow, product_table, hpso, pipeline):
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
    workflow : dictionary of the JSON graph we are focusing on

    product_table : pd.DataFrame
        Pandas dataframe containing the components

    hpso : str
        the HPSO we are generating.

    cluster: str
        path to the cluster specification required of the observation.


    Returns
    -------

    """

    ignore_components = [
        'UpdateGSM', 'UpdateLSM', 'FinishMinorCycle', 'BeginMinorCycle'
    ]

    total_product_costs = {}
    for element in workflow:
        # Name of task in DALiuGE workflow is 'nm'
        if 'outputs' in element.keys():
            name = element['nm']
            if name not in total_product_costs:
                if name in ignore_components:
                    # These are not 'products' that take compute time
                    total_product_costs[name] = 0
                else:
                    df = product_table[['Pipeline', 'hpso', name]]
                    df = df[(df['Pipeline'] == pipeline) & (df['hpso'] == hpso)]
                    value = float(df[name])
                    total_product_costs[name] = value

    return total_product_costs


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
    max_ingest = system_sizing[
        system_sizing['HPSO'] == 'hpso01'
        ]['Total Compute Requirement [PetaFLOP/s]']
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

def compile_observations_and_workflows():
    pass