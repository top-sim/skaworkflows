# Copyright (C) 2024 RW Bunney
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

import copy
import logging
import random
import sys

from collections import Counter
from dataclasses import dataclass, asdict
from pprint import pformat
from string import ascii_letters

from skaworkflows.common import Telescope, FIXED_LOW_CHANNELS_DEMAND, MAX_LOW_CHANNELS, MAX_MID_CHANNELS
from skaworkflows.observation.parameters import load_observation_defaults

LOGGER = logging.getLogger(__name__)
CONCURRENT_PROBABILITY = 0.5


@dataclass
class HPSOParameter:
    telescope: str
    count: int
    hpso: str
    duration: int
    workflows: list
    stations: int
    channels: int
    workflow_parallelism: int
    baseline: float

    def to_dict(self):
        return asdict(self)


class ObservationPlan:

    def __init__(self, telescope: str):
        self.telescope = Telescope(telescope)
        self._plan = {"nodes": self.telescope.default_compute_nodes,
                      "infrastructure": "parametric",
                      "telescope": self.telescope.name,
                      "hpsos": []
                      }

    def add_observation(self, hpso: HPSOParameter):
        self._plan["hpsos"].append(hpso.to_dict())

    def to_json(self):
        return self._plan

# TODO focus on this
# plan.add_observation(HPSOParameter(count=count, hpso=hpso,
#                                    duration=
#                                    low_observation_defaults[
#                                        "hpsos"][hpso][
#                                        "duration"], workflows=
#                                    low_observation_defaults[
#                                        "hpsos"][hpso][
#                                        "workflows"],
#                                    stations=stations,
#                                    channels=FIXED_LOW_CHANNELS_DEMAND * plan.telescope.channels_multiplier,
#                                    workflow_parallelism=stations,
#                                    baseline=baseline,
#                                    # *1000, # convert to meters
#                                    telescope=str(
#                                        plan.telescope)))


class Observation:
    """
    Helper-class to store information for when generating observation schedule
    """

    @classmethod
    def from_dict(cls, spec_dict, telescope='low'):
        """
        Create an Observation instance from a dictionary specification.

        Parameters
        ----------
        name : str
            The observation name (typically the HPSO identifier)
        spec_dict : dict
            Dictionary containing observation parameters with keys:
            - workflow: str
            - ingest_demand: int
            - duration: int
            - channels: int
            - workflow_parallelism: int
            - demand: int
            - baseline: float
            - workflow_type: list
            - graph_type: list
        telescope : str
            Telescope name (default: 'low')

        Returns
        -------
        Observation
            New Observation instance
        """
        # Extract HPSO name from the observation name (before underscore if present)
        name = spec_dict.get('name')
        hpso = name.split('_')[0] if '_' in name else name

        return cls(
            name=name,
            hpso=hpso,
            workflows=spec_dict.get('workflow_type', []),
            demand=spec_dict.get('demand', spec_dict.get('ingest_demand')),
            duration=spec_dict.get('duration'),
            channels=spec_dict.get('channels'),
            workflow_parallelism=spec_dict.get('workflow_parallelism'),
            baseline=spec_dict.get('baseline'),
            telescope=telescope,
            start=spec_dict.get('start', 0)
        )

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
            start=0
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
        self.stations = demand
        self.start = start
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
            self.name + (str(self.stations + self.workflow_parallelism + int(self.baseline)))
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
            "instrument_demand": self.stations,
            "type": self.hpso,
            "data_product_rate": self.ingest_data_rate,
        }


     

def process_hpso_from_spec(hpsos: dict, telescope='low', maximal=True)->list:
    """
    Pass a JSON dictionary of observations we want to process

    Easier to edit and cleaner to generate multiple observations (doesn't
    rely on

    Parameters
    ----------
    hpsos: dict
    telescope: str

    Returns
    -------
    final_obs: list
    """
    final_obs = []
    telescope = Telescope(telescope)
    LOGGER.info("Translating HPSO permutations to observing plan")
    if telescope.name == "low":
        observation_defaults = load_observation_defaults("skalow")
        channel_demand = MAX_LOW_CHANNELS if maximal else FIXED_LOW_CHANNELS_DEMAND
    else:
        observation_defaults = load_observation_defaults("skamid")
        channel_demand = MAX_MID_CHANNELS if maximal else FIXED_LOW_CHANNELS_DEMAND
    offset = 0
    for hpso, items in hpsos.items():
        counter = dict(Counter(items))
        for pair, count in counter.items():
            baseline, stations = pair
            obslist = create_observation_from_hpso(
                count=count,
                hpso=hpso,
                duration=observation_defaults["hpsos"][hpso]["duration"],
                workflows=observation_defaults["hpsos"][hpso]["workflows"],
                demand=stations,
                channels=channel_demand * telescope.channels_multiplier,
                workflow_parallelism=stations,
                baseline=baseline,
                # *1000, # convert to meters
                telescope=str(telescope),
                offset=offset
            )
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

def create_basic_plan(hpsos: list, shuffle=True,
                      existing_plan=None, seed=1):
    """
    Randomly shuffle the observations to create a sequence of HPSOS of different
    sizes.


    hpsos : list
        List of :py:obj:`~pipelines.observations_to_workflows.Observations`

    max_stations: int
        The maximum percentage of the telescope to be occupied at any given
        time. For some simulations, it may be necessary to only 'simulate' a
        smaller demand on the telescope.

    return: plan : list()
        A list of strings that details the order of HPSOs that will be running
        for a given plan. These HPSOs will be derived from what is in the
        provided system-sizing dictionary.

        These strings are HPSOs - we need to link them to a pipeline as well
        (RCAL/Ingest we can consume together as 'real-time' pipelines,
        and so promote these as the range of compute required for real-time
        execution).
    """

    plan = []

    start = 0
    finish = -1
    LOGGER.debug("%s", pformat(hpsos, indent=4, depth=1))
    if existing_plan:
        observations = [o for o in existing_plan]
    else:
        observations = [o for o in hpsos]
    if shuffle:
        random.seed(seed)
        random.shuffle(observations)

    while observations:
        for observation in observations:
            observation.add_start_time(start)
            observation.planned = True
            plan.append(observation)
            LOGGER.debug(f"{plan=}")
            if finish < start + observation.duration:
                finish = start + observation.duration
            start = finish
            finish = -1
        observations = [
            observation for observation in observations if not observation.planned
        ]
    return plan


def create_concurrent_plan(hpsos: list, max_stations: int, concurrent_demand_limit: int, concurrent_observation_probability=1, seed=1):
    """
    Randomly shuffle the observations to create a sequence of HPSOS of different
    sizes, _with_ concurrent observations.

    hpsos : list
        List of :py:obj:`~pipelines.observations_to_workflows.Observations`
[
    max_stations: int
        The maximum percentage of the telescope to be occupied at any given
        time. For some simulations, it may be necessary to only 'simulate' a
        smaller demand on the telescope.

    return: plan : list()
        A list of strings that details the order of HPSOs that will be running
        for a given plan. These HPSOs will be derived from what is in the
        provided system-sizing dictionary.

        These strings are HPSOs - we need to link them to a pipeline as well
        (RCAL/Ingest we can consume together as 'real-time' pipelines,
        and so promote these as the range of compute required for real-time
        execution).
    """
    plan = []

    LOGGER.info("Creating a concurrent plan...")
    start = 0
    finish = -1
    LOGGER.debug("%s", pformat(hpsos, indent=4, depth=1))
    observations = [o for o in hpsos]
    random.seed(seed)
    random.shuffle(observations)
    # We want the shuffle to be random but the number of random observations to be the same
    while observations:
        for observation in observations:
            stations = observation.stations
            num_concurrent = 1
            if (stations <= concurrent_demand_limit
                    and concurrent_observation_probability < CONCURRENT_PROBABILITY):
                num_concurrent = int(max_stations / stations) // 2

            concurrent_observations = [copy.deepcopy(observation) for i in range(num_concurrent)]
            for i, co in enumerate(concurrent_observations):
                co.name = f"{co.name}{ascii_letters[i]}"
            for obs in concurrent_observations:
                obs.add_start_time(start)
                obs.planned = True
                plan.append(obs)
                if finish < start + obs.duration:
                    finish = start + obs.duration
            start = finish
            finish = -1
        observations = [
            observation for observation in observations if not obs.planned
        ]
    return plan


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
            max_stations=max_telescope_usage,
            with_concurrent=with_concurrent,
            existing_plan=new_plan)
        if new_plan not in lol and i%3 == 0:
            lol.append(new_plan)

    with open("/tmp/plans.txt", "w") as fp:
        for l in lol:
            fp.write(f"{str(l)}\n")

    return lol
