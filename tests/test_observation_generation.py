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

from pipelines.hpso_to_observation import Observation
from pipelines.hpso_to_observation import create_observation_plan, \
    construct_telescope_config_from_observation_plan

DATA_DIR = 'data/parametric_model'
LONG = f'{DATA_DIR}/2021-06-02_long_HPSOs.csv'


SYSTEM_SIZING_DIR = 'data/pandas_sizing'
TOTAL_SYSTEM_SIZING = 'data/pandas_sizing/total_compute_SKA1_Low'

COMPONENT_SYSTEM_SIZING = None
CLUSTER = 'tests/PawseyGalaxy_nd_1619058732.json'

EAGLE_LGT = 'tests/data/eagle_lgt.graph'

PATH_NOT_EXISTS = 'path/doesnt/exist'

class TestObservationClass(unittest.TestCase):

    def setUp(self):
        self.obs1 = Observation(2, 'hpso01', 32, 60, 'dprepa', 256, 'long')
        self.obs2 = Observation(4, 'hpso04a', 16, 30, 'dprepa', 256, 'long')

    def test_unroll_observations(self):
        unroll_list = [self.obs1 for x in range(self.obs1.count)]
        self.assertListEqual(unroll_list, self.obs1.unroll_observations())

    def test_add_workflow_path(self):
        self.assertRaises(
            RuntimeError, self.obs1.add_workflow_path, PATH_NOT_EXISTS
            )
        self.assertTrue()


class TestObservationPlanGeneration(unittest.TestCase):

    def setUp(self):
        # low_compute_data = "csv/SKA1_Low_COMPUTE.csv"
        # self.observations = convert_systemsizing_csv_to_dict(low_compute_data)
        self.obs1 = Observation(2, 'hpso01', 32, 60, 'dprepa', 256, 'long')
        self.obs2 = Observation(4, 'hpso04a', 16, 30, 'dprepa', 128, 'long')
        self.obs3 = Observation(3, 'hpso01', 32, 30, 'drepa', 256, 'long')
        self.system_sizing = pd.read_csv(LONG)
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


class TestObservationTopSimTranslation(unittest.TestCase):

    def setUp(self):
        self.obs1 = Observation(
            count=2, hpso='hpso01', demand=32,
            duration=60, pipeline='dprepa', channels=256,
            baseline='long'
        )
        with open(CLUSTER) as jf:
            self.cluster = json.load(jf)
        self.max_telescope_usage = 32  # 1/16th of telescope
        self.plan = [
            (0, 60, 32, 'hpso01', 'dprepa', 256, 'long'),
            (60, 120, 32, 'hpso01', 'dprepa', 256, 'long')
        ]
        self.obs2 = Observation(1, 'hpso01', 32, 60, 'dprepa', 256, 'long')
        self.obs3 = Observation(2, 'hpso04a', 16, 30, 'dprepa', 256, 'long')

    def test_telescope_config_sizing(self):
        """
        Gvien an observation plan, we want to calculate the expected output
        for that plan to ensure the expected ingest rates etc. are correct.
        These are used in TopSim to count how much data is being produced in
        the system during an observation on the telescope.
        Returns
        -------

        """

        obslist = construct_telescope_config_from_observation_plan(
            self.plan, TOTAL_SYSTEM_SIZING
        )
        # The number of channels is 256; this is half the number of max
        # channels, so we would expect the size to be half of the stored data
        # rate in the SDP report.
        self.assertAlmostEqual(0.316214, obslist[0]['data_product_rate'], 6)

    def test_buffer_config_sizing(self):
        """
        Call the generate_buffer_config, which is a wrapper for hpconfig
        buffer_to_topsim config

        Returns
        -------

        """




    def test_ingest_machine_provisioning(self):
        """
        When creating system config, we want to find the largest ingest
        pipeline based on expected FLOPS to set up the Telescope boundaries.
        Returns
        -------

        """


    def test_observation_output(self):
        """
        Given a plan, produce an JSON-serialisable dictionary for configuration

        Returns
        -------

        """

        # Count the number of shared items between two dictionaries -
        # this will help us test the JSON files produced during translation.
        # shared_items = {k: x[k] for k in x if k in y and x[k] == y[k]}

    def test_final_config_creation(self):
        """
        Produce a total simulation config for at TopSim simulator

        Requires:

        * Final output directory for the data and workflow
        * Underlying compute infrastructure (using HPConfig)
        * Path to component-based costs (parametric output)
        * Path or dictionary to logical graph files for workflow translation
        * List of observations (to be unrolled).

        These are compiled and will produce a JSON-compatible dictionary:


        Returns
        -------

        """
        final_dir = 'test/data/out/'
        final_workflow_dir = 'test/data/out/workflows'
        observations = self.obs1.unroll_observations()

        system_sizing = None
        pandas_component_sizing = None

        path = compile_observations_and_workflows()
