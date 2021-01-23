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
    'hpso4': ['dprepa','dprepb','dprepc', 'dprepd'],  # Pulsar
    'hpso5': ['dprepa','dprepb','dprepc', 'dprepd'],  # Pulsar
    'hpso18': ['dprepa','dprepb','dprepc', 'dprepd'],  # Transients (FRB)
    'hpso32': ['dprepa','dprepc', 'dprepd']  # Cosmology (Gravity)
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

    df = csv.rename(columns=reverse_keys)

    observations = [dict(x._asdict()) for x in df.itertuples(index=False)]
    for observation in observations:
        for key in observation:
            v = observation.get(key)
            tmp = (v, compute_units[key])
            observation[key] = tmp

    return observations


def create_observation_plan(observations, per_hpso_count):
    """
    Given a sequence of HPSOs that are present in the system sizing
    dictionary, generate a plan. of observations from which we can create
    telescope config.

    #TODO Re-read this section below
    The idea here is that we have our hpso_count, which is the frequency of
    that HPSOs within the mid-term schedule. From this, we determine the
    pipeline that we associate with that observation; there will be ingest
    pipelines and then processing pipelines. The mid-term plan is (
    eventually) only going to represent observations + pipelines.
    Observations that have multiple pipelines associated with them will be
    represented as two observations, one with a length >0, and the rest
    without. These will then reference the same data that's been produced.

    Parameters
    ----------
    hpso_list : list()
        A list of strings describing the HPSOs that are to be placed in the plan

    per_hpso_count : list()
        A list of Integers that describes how many of each HPSO is in the plan


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



    hpso_list = [obs['hpso'][0] for obs in observations]

    pipelines = ['dprepa', 'dprepb', 'dprepc', 'dprepd']
    plan = []

    constraints = {

    }

    # TODO use this approach to generate the pipeline plan as it is more
    #  efficient
    # final = []
    # for i, count in enumerate(plan):
    #     final.extend([observations[i]['hpso']] * count)

    for hpso in hpso_list:
        for count in per_hpso_count:
            tmp = (hpso, random.choice(pipelines))
            plan.append(tmp)
    return plan


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
    observations = convert_systemsizing_csv_to_dict('csv/SKA1_Low_COMPUTE.csv')
    hpso_list = [obs['hpso'][0] for obs in observations]
    hpso_frequency = [2, 1, 3, 0, 2]
    plan = create_observation_plan(hpso_list, hpso_frequency)

    config = construct_telescope_config_from_observation_plan(observations,
                                                              plan)
    filename = 'json/config.json'
    with open(filename, 'w') as jfile:
        json.dump(config, jfile, indent=4)
