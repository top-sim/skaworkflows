# Copyright (C) 1/4/22 RW Bunney
import copy

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


import pytest
import unittest
import pandas as pd
from pathlib import Path

from skaworkflows import observation
import skaworkflows.workflow.observations_to_workflows as hto
from skaworkflows.common import Telescope
from skaworkflows.observation.observation import Observation, process_hpso_from_spec, create_basic_plan, \
    create_concurrent_plan
from skaworkflows.observation.permutations import allocate_observations, create_hpso_counts_from_ratios
from skaworkflows.observation.statistics import observation_weighting


def create_one_day_plan():
    """
    Do the setup required to get a plan

    Returns
    -------
    dict, set of observations
    """
    observation_amounts, total_obs = create_hpso_counts_from_ratios(1)
    observation_sizes = pd.Series(
        {"small": 0, "medium": 15, "large": 5})
    return allocate_observations(observation_amounts, total_obs,
                                 observation_sizes)

class CreateObservationFromPermutations(unittest.TestCase):

    def test_process_hpso_from_spec(self):
        plan = create_one_day_plan()

        observation_plan = process_hpso_from_spec(plan)
        self.assertEqual(20, len(observation_plan))

class CreateObservationPlanTimeAllocations(unittest.TestCase):

    def test_non_overlapping_plans(self):
        plan = create_one_day_plan()
        observation_plan = process_hpso_from_spec(plan)
        telescope = Telescope("low")
        odp = create_basic_plan(observation_plan, telescope.max_stations)
        self.assertEqual(20,len(odp))
        start = 0
        so = odp.pop(0)
        self.assertEqual(start, so.start)
        for o in odp:
            self.assertEqual(start + so.duration, o.start)
            start = start + so.duration
            so = o


    def test_overlapping_plans(self):
        plan = create_one_day_plan()
        observation_plan = process_hpso_from_spec(plan)
        telescope = Telescope("low")
        odp = create_concurrent_plan(observation_plan, telescope.max_stations,
                                64)
        start = 0
        so = odp.pop(0)
        self.assertEqual(start, so.start)
        for o in odp:
            if so.stations == 64 and o.stations == 64:
                self.assertEqual(so.start, o.start)
            else:
                self.assertEqual(start + so.duration, o.start)
                start = start + so.duration
                so = o

    def test_multiple_plans_factor(self):
        plan = create_one_day_plan()
        telescope = Telescope("low")
        plans = []
        for i in range(50):
            observation_plan = process_hpso_from_spec(plan)
            plans.append(create_concurrent_plan(observation_plan, telescope.max_stations,
                                64, seed=i))
        weights = [observation_weighting(plan) for plan in plans]
        print(f"{min(weights)=}")
        print(f"{max(weights)=}")


@unittest.skip("Legacy test cases")
class OldTests(unittest.TestCase):
    def read_hpso_spec(self):
        """
        Create path object for process_hpso_from_spec

        Returns
        -------
        path: pathlib.Path
            A Path object for `'tests/data/hpso_spec.json'`
        """

        path = Path('tests/data/hpso_spec.json')
        return path

    def test_obs_list_length_from_spec(self, spec):
        obslist = observation.observation.process_hpso_from_spec(spec)
        assert len(obslist) == 5

    def test_obs_list_hpso_attributes(self, spec):
        obslist = observation.observation.process_hpso_from_spec(spec)
        o = obslist[0]
        assert o.name == 'hpso01_0'
        assert o.duration == 18000
        assert o.stations == 512
        o = obslist[2]
        assert o.name == 'hpso01_2'
        assert o.stations == 256
        assert o.baseline == 65000.0


SMALL_OBS_LIST = [
    Observation('A', 'hpso01', 'ICAL', 64, 18000,
                16384, 64, 65000, "low"),
    Observation('B', 'hpso01', 'ICAL', 256, 18000,
                16384, 64, 65000, "low"),
    Observation('C', 'hpso02a', 'ICAL', 64, 18000,
                16384, 64, 65000, "low"),
    Observation('D', 'hpso02a', 'ICAL', 64, 18000,
                16384, 64, 65000, "low"),
]


# class TestObservationPlan(unittest.TestCase):
#
#     def setUp(self):
#         pass
#
#     import copy
#     def testBasicPlan(self):
#         """
#         This confirms the basic functionality of generating a basic plan, with and without
#         concurrent observations enables.
#
#         This is confirmed by checking the start times in the plan:
#             * For with_concurrent=False, we would expect the set of start times to
#             contain all different start times from the observation list
#             * For with_concurrent=True, we would expect that as may observations as can
#             be run concurrently will be (based on telescope demand), so we would expect
#             the set to be smaller.
#
#         """
#         plan = observation.observation.create_basic_plan(copy.deepcopy(SMALL_OBS_LIST), max_stations=512,
#                                                          with_concurrent=False)
#
#         plan_obs = [o.start for o in plan]
#         self.assertEqual(4, len(set(plan_obs)))
#         # Confirm that concurrent plan has A, C, D all scheduled together
#         plan = observation.observation.create_basic_plan(copy.deepcopy(SMALL_OBS_LIST),
#                                                          max_stations=256, with_concurrent=True)
#         plan_obs = [o.start for o in plan if o.name != 'B']
#         self.assertEqual(1, len(set(plan_obs)))
#
#     def testAlternatePlans(self):
#         plan = observation.observation.create_basic_plan(copy.deepcopy(SMALL_OBS_LIST),
#                                                          max_stations=256, with_concurrent=False)
#         alternates = observation.observation.alternate_plan_composition(plan, 512)
#         print(alternates)
#         # self.assertListEqual(['A', 'C', 'D', 'B'], plan_obs)

