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

from workflow.hpso_to_observation import Observation
from workflow.hpso_to_observation import create_observation_plan, \
    construct_telescope_config_from_observation_plan, \
    create_buffer_config, compile_observations_and_workflows

from workflow.common import SI

from hpconfig.specs.sdp import SDP_LOW_CDR

DATA_DIR = 'data/parametric_model'
LONG = f'{DATA_DIR}/2021-06-02_long_HPSOs.csv'

SYSTEM_SIZING_DIR = 'data/pandas_sizing'
TOTAL_SYSTEM_SIZING = 'data/pandas_sizing/total_compute_SKA1_Low.csv'

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
        # self.assert()


class TestObservationPlanGeneration(unittest.TestCase):

    def setUp(self):
        # low_compute_data = "csv/SKA1_Low_COMPUTE.csv"
        # self.observations = convert_systemsizing_csv_to_dict(low_compute_data)
        self.obs1 = Observation(2, 'hpso01', 32, 60, 'dprepa', 256, 'long')
        self.obs2 = Observation(4, 'hpso04a', 16, 30, 'dprepa', 128, 'long')
        self.obs3 = Observation(3, 'hpso01', 32, 30, 'drepa', 256, 'long')
        self.system_sizing = pd.read_csv(TOTAL_SYSTEM_SIZING)
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
            duration=60, workflows=['dprepa'], channels=256,
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

    def test_ingest_demand_calc(self):
        # Start with a spec, then determine how many machines we will need
        # based on the ingest size
        max_ingest = self.system_sizing[
            self.system_sizing['HPSO'] == 'hpso01'
            ]['Ingest [Pflop/s]']
        ingest_flops = 32 / 512 * (float(max_ingest) * SI.peta)
        self.assertEqual(123, hpo._find_ingest_demand(self.cluster,
                                                      ingest_flops))

    def test_telescope_config_sizing(self):
        """
        Gvien an observation plan, we want to calculate the expected output
        for that plan to ensure the expected ingest rates etc. are correct.
        These are used in TopSim to count how much data is being produced in
        the system during an observation on the telescope.

        >>>    {..."telescope": {
        >>>       "total_arrays": 512,
        >>>       "pipelines": {
        >>>           "DPrepA": {
        >>>               "ingest_demand": 128,
        >>>                "workflow": "final/directory/for/workflow",
        >>>           },
        >>>     ...}

        This is testing the combination of

        generate_workflow_from_observation
        generate_cost_per_product
        find_ingest_deman

        Returns
        -------

        """
        itemised_spec = SDP_LOW_CDR()
        total_system_sizing = pd.read_csv(TOTAL_SYSTEM_SIZING)
        component_system_sizing = pd.read_csv(COMPONENT_SYSTEM_SIZING)
        obslist = construct_telescope_config_from_observation_plan(
            self.plan, total_system_sizing, component_system_sizing,
            itemised_spec
        )
        # The number of channels is 256; this is half the number of max
        # channels, so we would expect the size to be half of the stored data
        # rate in the SDP report.
        self.assertAlmostEqual(0.316214, obslist[0]['data_product_rate'], 6)

    def test_buffer_config_sizing(self):
        """
        Call the generate_buffer_config, which is a wrapper for hpconfig
        buffer_to_topsim config

        Buffer information is in hpconfig or the Total System sizing. At the
        beginning of the configuration, the only necessary information for
        the buffer is how we split the Hot and Cold, as this is not currently
        set in stone.

        Returns
        -------

        """
        buffer_ratio = (1, 5)
        sdp = SDP_LOW_CDR()
        # For SDP_LOW_CDR, total_storage is 67.2 PetaBytes
        hot_buffer = 13.44  # 20% of buffer
        cold_buffer = 53.76  # 80% of buffer
        spec = create_buffer_config(sdp, buffer_ratio)
        self.assertEqual(spec['buffer']['hot']['capacity'] / SI.peta,
                         hot_buffer)
        self.assertEqual(
            spec['buffer']['cold']['capacity'] / SI.peta,
            cold_buffer
        )
        self.assertEqual(
            spec['buffer']['hot']['max_ingest_rate'] / SI.giga,
            1254.4
        )

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

        buffer_ratio = (1, 5)
        sdp = SDP_LOW_CDR()

        path = compile_observations_and_workflows()
