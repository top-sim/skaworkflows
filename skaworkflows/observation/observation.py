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

from skaworkflows.common import Telescope, FIXED_LOW_CHANNELS_DEMAND
from skaworkflows.observation.parameters import load_observation_defaults

LOGGER = logging.getLogger(__name__)


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
        self.stations = demand
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


def process_hpso_from_spec(hpsos: dict, telescope='low'):
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
    low_observation_defaults = load_observation_defaults("skalow")
    offset = 0
    for hpso, items in hpsos.items():
        counter = dict(Counter(items))
        for pair, count in counter.items():
            baseline, stations = pair
            obslist = create_observation_from_hpso(
                count=count,
                hpso=hpso,
                duration=low_observation_defaults["hpsos"][hpso]["duration"],
                workflows=low_observation_defaults["hpsos"][hpso]["workflows"],
                demand=stations,
                channels=FIXED_LOW_CHANNELS_DEMAND * telescope.channels_multiplier,
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


def create_observation_plan(hpsos, max_telescope_usage):
    """
    Given a sequence of HPSOs that are present in the system sizing
    dictionary, generate a plan. of observations from which we can create
    telescope config.


    Parameters
    ----------


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
    LOGGER.debug("%s" ,{pformat(hpsos, indent=4, depth=1)})
    observations = [o for o in hpsos]
    while observations:
        LOGGER.info("Generating observing plan")
        # observations = sorted(
        #     observations, key=lambda obs: (obs.baseline, obs.duration)
        # )
        if (len(observations) > 1) and (loop_count % len(observations) == 0):
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


def create_basic_plan(hpsos: list, max_stations: int,
                      existing_plan=None):
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


def create_concurrent_plan(hpsos: list, max_stations: int, concurrent_demand_limit: int):
    """
    Randomly shuffle the observations to create a sequence of HPSOS of different
    sizes, _with_ concurrent observations.


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
    observations = [o for o in hpsos]
    random.shuffle(observations)
    while observations:
        for observation in observations:
            stations = observation.stations
            num_concurrent = 1
            if stations <= concurrent_demand_limit:
                num_concurrent = int(max_stations / stations)

            concurrent_observations = [observation] * num_concurrent
            for i, co in enumerate(concurrent_observations):
                co.name = f"{co.name}{ascii_letters[i]}"
            for obs in concurrent_observations:
                observation.add_start_time(start)
                observation.planned = True
                plan.append(observation)
                if finish < start + observation.duration:
                    finish = start + observation.duration
            start = finish
            finish = -1
        observations = [
            observation for observation in observations if not observation.planned
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
