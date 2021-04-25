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
import random
import itertools
import pandas as pd


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
        The duration of the observation
    pipeline : str
        The imaging pipeline to process the observation data
    """

    def __init__(self, count, hpso, demand, duration, pipeline):
        self.count = count
        self.hpso = hpso
        self.demand = demand
        self.duration = duration
        self.pipeline = pipeline

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
    # TODO Store a list of tuples; tuple is start, end, usage segment.
    # Looping to fill up telescope resources as much as possible.
    # Want plan =[(0,60,32,'hpso01','dprepa'),(60,-1,0)]
    current_start = 0
    current_tel_usage = 0
    loop_count = 0
    plan = [(0, -1, 0, '', '')]
    while observations:
        observations = sorted(
            observations, key=lambda obs: (obs.demand, obs.duration)
        )
        if loop_count % len(observations) == 0:
            start, finish, demand, hpso, pipeline = plan[-1]
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
            start, finish, demand, hpso, pipeline = plan[-1]
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
            start, finish, demand, hpso, pipeline = plan[-1]
            next_time_frame = (finish, -1, 0, '', '')
            current_tel_usage = 0
            plan.append(next_time_frame)

    return plan


def create_observation_plan_tuple(observation, start):
    start = start
    finish = start + observation.duration
    hpso = observation.hpso
    pipeline = observation.pipeline
    demand = observation.demand
    tup = (start, finish, demand, hpso, pipeline)
    return tup


def construct_telescope_config_from_observation_plan(observations, plan):
    """
    Based on a simplified dictionaries built from the System Sizing for the
    SDP, this function builds a mid-term plan based on the HPSO sizes.

    The structure of an observation.json file takes the following:

    {
      "telescope": {
        "total_arrays": # This is adapted from the maximum number of
        'stations' in the system_sizing csv.

        "pipelines": {
            # Pipelines are taken from the HPSOs that are outlined in the plan;
            "pipeline_name": {
               "demand": demand is taken from 'totalrt', which is the total
               Real-time compute required for the observation (in PFlops/s).
            }
        }
        "observations": [
            # a list of observations in the order of the 'plan'
            "name": # this is a place holder
            "start": This is relative to the first, in hours from first
            observation start time
            "duration": Tobs, from system sizing
            "demand": this is the number of stations it requires
            "workflow": where the workflow file is stored
            "type": what pipeline it is
            "data_product_rate": This is
        ]
      }
    }

    Paremeters
    ----------
    observations: list
        list of dictionaries, each dictionary storing a HPSO

    Returns
    -------
    config : dict()
        A dictionary that is in the form required of the TOpSim simulator to
        configure a Telescope (see topsim.core.telescope.Telescope).

    """
    # Find max number of arrays:
    max_stations, unit = max([x['stations'] for x in observations])
    pipelines = [x['hpso'] for x in observations]
    config = {
        'telescope': {
            'total_arrays': max_stations
        },
        'observations': [

        ]
    }
    return config


if __name__ == '__main__':
    df_system_sizing = convert_systemsizing_csv_to_dict(
        'csv/SKA1_Low_hpso_pandas.csv'
    )
    df_pipeline_products = pd.read_csv('SKA1_Low_PipelineProducts.csv')
    hpso_frequency = [2, 1, 3, 0, 0]
    final_plan = create_observation_plan(df_system_sizing, hpso_frequency)

    config = construct_telescope_config_from_observation_plan(
        df_system_sizing, final_plan
    )
    filename = 'json/config.json'
    with open(filename, 'w') as jfile:
        json.dump(config, jfile, indent=4)
