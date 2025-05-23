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
from pathlib import Path

import skaworkflows.workflow.hpso_to_observation as hto
from skaworkflows.workflow.hpso_to_observation import Observation


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
        obslist = hto.process_hpso_from_spec(spec)
        assert len(obslist) == 5

    def test_obs_list_hpso_attributes(self, spec):
        obslist = hto.process_hpso_from_spec(spec)
        o = obslist[0]
        assert o.name == 'hpso01_0'
        assert o.duration == 18000
        assert o.demand == 512
        o = obslist[2]
        assert o.name == 'hpso01_2'
        assert o.demand == 256
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


class TestObservationPlan(unittest.TestCase):

    def setUp(self):
        pass

    import copy
    def testBasicPlan(self):
        """
        This confirms the basic functionality of generating a basic plan, with and without
        concurrent observations enables.

        This is confirmed by checking the start times in the plan:
            * For with_concurrent=False, we would expect the set of start times to
            contain all different start times from the observation list
            * For with_concurrent=True, we would expect that as may observations as can
            be run concurrently will be (based on telescope demand), so we would expect
            the set to be smaller.

        """
        plan = hto.create_basic_plan(copy.deepcopy(SMALL_OBS_LIST), max_telescope_usage=512,
                                     with_concurrent=False)

        plan_obs = [o.start for o in plan]
        self.assertEqual(4, len(set(plan_obs)))
        # Confirm that concurrent plan has A, C, D all scheduled together
        plan = hto.create_basic_plan(copy.deepcopy(SMALL_OBS_LIST),
                                           max_telescope_usage=256, with_concurrent=True)
        plan_obs = [o.start for o in plan if o.name != 'B']
        self.assertEqual(1, len(set(plan_obs)))

    def testAlternatePlans(self):
        plan = hto.create_basic_plan(copy.deepcopy(SMALL_OBS_LIST),
                                     max_telescope_usage=256, with_concurrent=False)
        alternates = hto.alternate_plan_composition(plan, 512)
        print(alternates)
        # self.assertListEqual(['A', 'C', 'D', 'B'], plan_obs)

