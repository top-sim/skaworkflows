# Copyright (C) 8/1/21 RW Bunney

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

import unittest
import random
import json

import pandas as pd

from hpso_to_observation import Observation
from hpso_to_observation import SI
from hpso_to_observation import \
    convert_systemsizing_csv_to_dict, create_observation_plan, \
    construct_telescope_config_from_observation_plan, \
    _find_ingest_demand

SKA_LOW_SYSTEM = 'csv/SKA1_Low_compute_pandas.csv'
CLUSTER = 'tests/PawseyGalaxy_nd_1619058732.json'

class TestObservationUnroll(unittest.TestCase):

    def setUp(self):
        self.obs1 = Observation(2, 'hpso01', 32, 60, 'dprepa',256)
        self.obs2 = Observation(4, 'hpso04a', 16, 30, 'dprepa',256)

    def test_unroll_observations(self):
        unroll_list = [self.obs1 for x in range(self.obs1.count)]
        self.assertListEqual(unroll_list, self.obs1.unroll_observations())


class TestObservationPlanGeneration(unittest.TestCase):

    def setUp(self):
        # low_compute_data = "csv/SKA1_Low_COMPUTE.csv"
        # self.observations = convert_systemsizing_csv_to_dict(low_compute_data)
        self.obs1 = Observation(2, 'hpso01', 32, 60, 'dprepa', 256)
        self.obs2 = Observation(4, 'hpso04a', 16, 30, 'dprepa', 256)
        self.obs3 = Observation(3, 'hpso1', 32, 30, 'drepa', 256)
        self.system_sizing = pd.read_csv(SKA_LOW_SYSTEM)
        self.max_telescope_usage = 32  # 1/16th of the telescope

    def test_create_observation_plan_notiebreaks(self):
        """
        create_observation_plan takes the set of observations and generates a sequence
        and produces a dictionary from this.
        (0, 60, 32, 'hpso01', 'dprepa', 256)
        (60, 90, 16, 'hpso04a', 'dprepa',256)
        (60, 90, 16, 'hpso04a', 'dprepa',256)
        (90, 150, 32, 'hpso01', 'dprepa',256)
        (150, 180, 16, 'hpso04a', 'dprepa',256)
        (180, 210, 16, 'hpso04a', 'dprepa',256)

        There are a number of constraints on observations plans
        Returns
        -------
        """
        random.seed(0)
        obslist = (self.obs1.unroll_observations()
                   + self.obs2.unroll_observations())
        plan = create_observation_plan(obslist, self.max_telescope_usage)
        self.assertTrue(plan[0], [(0, 30, self.obs1)])
        self.assertTrue(plan[1], self.obs2)

    def test_create_observation_plan_tiebreaker(self):
        """
        Make sure the hpos with the longest time goes first regardless of
        their order in the list
        Returns
        -------

        """
        self.assertTrue(False)


class TestObservationTopSimTranslation(unittest.TestCase):

    def setUp(self):
        self.obs1 = Observation(2, 'hpso01', 32, 60, 'dprepa', 256)
        self.system_sizing = pd.read_csv(SKA_LOW_SYSTEM)
        with open(CLUSTER) as jf:
            self.cluster = json.load(jf)
        self.max_telescope_usage = 32  # 1/16th of telescope
        self.plan = [
            (0, 60, 32, 'hpso01', 'dprepa', 256),
            (60, 120, 32, 'hpso01', 'dprepa', 256)
        ]

    def test_sizing_calculations(self):
        obslist = construct_telescope_config_from_observation_plan(
            self.plan,self.cluster, self.max_telescope_usage,
            self.system_sizing)

        self.assertAlmostEqual(0.028689, obslist[0]['data_product_rate'], 6)

    def test_ingest_demand_calc(self):
        max_ingest = self.system_sizing[
            self.system_sizing['HPSO'] == 'hpso01'
            ]['Ingest [Pflop/s]']
        ingest_flops = 32/512 * (float(max_ingest) * SI.peta)
        self.assertEqual(123, _find_ingest_demand(self.cluster, ingest_flops))