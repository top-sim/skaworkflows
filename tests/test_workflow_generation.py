# Copyright (C) 6/6/21 RW Bunney

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
import os

import pandas as pd
import workflow.hpso_to_observation as hpo
import workflow.eagle_daliuge_translation as edt
from workflow.common import SI

BASE_DATA_DIR = "data/pandas_sizing/"
TEST_DATA_DIR = 'tests/data/'

TOTAL_SYSTEM_SIZING = f'{BASE_DATA_DIR}total_compute_SKA1_Low.csv'
LGT_PATH = 'tests/data/eagle_lgt_scatter.graph'
PGT_PATH = 'tests/data/daliuge_pgt_scatter.json'
PGT_PATH_GENERATED = f'{TEST_DATA_DIR}/daliuge_pgt_scatter_generated.json'
LGT_CHANNEL_UPDATE = f'{TEST_DATA_DIR}/eagle_lgt_scatter_channel-update.graph'
PGT_CHANNEL_UPDATE = f'{TEST_DATA_DIR}/daliuge_pgt_scatter_channel-update.json'
TOPSIM_PGT_GRAPH = 'tests/data/topsim_compliant_pgt.json'
NO_PATH = "dodgy.path"
COMPONENT_SYSTEM_SIZING = f'{BASE_DATA_DIR}/component_compute_SKA1_Low.csv'


# SKA Low Pipelines
#
#         # hpso01:  (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
#         #           Pipelines.ICAL, Pipelines.DPrepA,
#         #           Pipelines.DPrepB, Pipelines.DPrepC, Pipelines.DPrepD),
#         # hpso02a: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
#         #           Pipelines.ICAL, Pipelines.DPrepA,
#         #           Pipelines.DPrepB, Pipelines.DPrepC, Pipelines.DPrepD),
#         # hpso02b: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
#         #           Pipelines.ICAL, Pipelines.DPrepA,
#         #           Pipelines.DPrepB, Pipelines.DPrepC, Pipelines.DPrepD),
#         # hpso04a: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
#         #           Pipelines.PSS),
#


class TestPipelineStructureTranslation(unittest.TestCase):

    def setUp(self) -> None:
        pass

    def test_lgt_to_pgt_file(self):
        edt.unroll_logical_graph(
            LGT_PATH,
            PGT_PATH_GENERATED
        )
        self.assertTrue(os.path.exists(
            PGT_PATH_GENERATED))

    def test_lgt_to_pgt_nofileoutput(self):
        """
        Test that we generate a json-compatible str result instead of a file.

        Returns
        -------

        """

        result = edt.unroll_logical_graph(
            'tests/data/eagle_lgt_scatter_minimal.graph')
        jdict = json.loads(result)
        with open('tests/data/daliuge_pgt_scatter_minimal.json') as f:
            test_dic = json.load(f)
        self.assertListEqual(jdict, test_dic)

    def test_lgt_to_pgt_with_channel_update(self):
        number_channels = 4
        # First convert channels from JSON string/dict
        lgt_dict = edt.update_number_of_channels(
            LGT_PATH, number_channels
        )
        self.assertTrue(isinstance(lgt_dict, dict))
        self.assertTrue('linkDataArray' in lgt_dict)
        pgt_list = json.loads(edt.unroll_logical_graph(lgt_dict, file_in=False))
        with open(PGT_CHANNEL_UPDATE) as f:
            test_pgt = json.load(f)
        self.assertEqual(test_pgt, pgt_list)
        self.assertEqual(283, len(pgt_list))

        # Get returned string and confirm it is the same as a previously
        # converted logical graph

    def test_daliuge_nx_conversion(self):
        """
        Once we unroll to a daliuge JSON file, we want to keep this as a NX
        object until we convert it to JSON, which will happen at the point we
        want to allocate computational and data loads to tasks.


        Returns
        -------

        """
        # Make sure the data file is consistent
        # We set the seed to for edge assertion below, as the numbers are
        # randomly generated.
        random.seed(0)
        minimal_pgt = 'tests/data/daliuge_pgt_scatter_minimal.json'
        with open(minimal_pgt) as f:
            jdict = json.load(f)
        nx_graph = edt._daliuge_to_nx(jdict, 'DPrepA')
        self.assertEqual(21, len(nx_graph.nodes))
        self.assertEqual(21, len(nx_graph.edges))

        # Expected grid is 1
        # Expected degrid is 1
        # We want to double check that our translated JSON matches the
        # original relationships in the previous JSON graph.
        # Chck the following:

        with open(PGT_PATH) as f:
            jdict = json.load(f)
        # jdict['oid']
        nx_graph = edt._daliuge_to_nx(jdict, 'DPrepA')
        example_edge_attr = {
            "data_size": 0,
            "u": "1_-4_0/0",
            "v": "1_-13_0/0/1/0",
            "data_drop_oid": "1_-26_0/0",
        }

        self.assertDictEqual(
            nx_graph.edges['DPrepA_Flag_3', 'DPrepA_FFT_3'],
            example_edge_attr
        )
        # now test for multi-loop example
        # Expected grid is 2
        # expected subtract image is 4

    def test_json_to_topsim(self):
        """
        We use the converted daliuge_to_nx and then add additional
        information required by TopSim.
        """
        self.assertRaises(FileExistsError, edt.eagle_to_topsim, NO_PATH,
                          'DPrepA')
        final_graph = edt.eagle_to_topsim(LGT_PATH, 'DPrepA')

        example_edge = {
            "data_size": 0,
            "u": "1_-4_0/0",
            "v": "1_-13_0/0/1/0",
            "source": "DPrepA_Flag_3",
            "target": "DPrepA_FFT_3",
            "data_drop_oid": "1_-26_0/0",
        }
        nodes = final_graph['graph']['nodes']
        self.assertEqual(36, len(nodes))
        edges = final_graph['graph']['links']
        self.assertTrue(example_edge in edges)

    def test_use_in_other_functions(self):
        pass

        # with open()

    def tearDown(self) -> None:
        """
        Remove files generated during various test cases
        Returns
        -------

        """
        if os.path.exists(PGT_PATH_GENERATED):
            os.remove(PGT_PATH_GENERATED)
        self.assertFalse(os.path.exists(PGT_PATH_GENERATED))


class TestWorkflowFromObservation(unittest.TestCase):

    def setUp(self) -> None:
        pass

    def testConcatWorkflows(self):
        """
        Generate 2 workflows, and ensure that they have been correctly
        concatenated

        Returns
        -------

        """
        minimal_pgt = 'tests/data/daliuge_pgt_scatter_minimal.json'
        with open(minimal_pgt) as f:
            jdict = json.load(f)
        # We know this has 21 nodes and edges
        nx_graph = edt._daliuge_to_nx(jdict, 'DPrepA')

        pass


class TestCostGenerationAndAssignment(unittest.TestCase):

    def setUp(self) -> None:
        self.channels = 256
        self.system_sizing = pd.read_csv(TOTAL_SYSTEM_SIZING)
        demand = 32
        duration = 3600
        channels = 64
        self.obs1 = hpo.Observation(
            1, 'hpso01', ['DPrepA'], demand, duration, channels, 'long'
        )
        self.component_system_sizing = pd.read_csv(COMPONENT_SYSTEM_SIZING)

        self.observations = []

    def testIsolateComponentCost(self):
        """
        isolate_component_cost takes observation, workflow, the component, and
        the component-based system sizing as a arguments to find the total
        FLOPS cost for that product
        Returns
        -------

        """

        # long baseline, DPrepA - component is Degrid
        component_cost = hpo.isolate_component_cost(
            self.obs1.hpso, self.obs1.baseline, self.obs1.workflows[0],
            'Degrid', self.component_system_sizing
        )
        print(component_cost)
        self.assertAlmostEquals(0.06615325754321748, component_cost, places=5)

        component_cost = hpo.isolate_component_cost(
            self.obs1.hpso, self.obs1.baseline, self.obs1.workflows[0],
            'Degrid', self.component_system_sizing
        )
        component_cost = hpo.isolate_component_cost(
            self.obs1.hpso, self.obs1.baseline, self.obs1.workflows[0],
            'Degrid', self.component_system_sizing
        )


    def generate_cost_per_product(self):
        """
        Based on the number of channels and the observation specs, we divide
        the cost across those tasks. Here, we test that the division occurs,
        and that the total of all the divisions adds up to the total FLOPs
        for the provided workflow.

        workflow
        product_table
        hpso

        Returns
        -------

        """

        final_workflow = hpo.generate_cost_per_product(

        )

        self.assertTrue(False)

    def test_generate_workflow_from_observation(self):
        """
        From a single observation, produce a workflow, get the file path,
        and return the location and the 'pipelines' dictionary entry.

        * If the telescope demand is not complete, this will affect the
        calculations
        * The number of frequency channels will affect the size of the workflow

        This is reliant on the hpo.generate_cost_per_product_workflow

        Returns
        -------

        """
        # Get an observation object and create a file for the associated HPSO
        total_system_sizing = pd.read_csv(TOTAL_SYSTEM_SIZING)
        telescope_max = hpo.telescope_max(
            total_system_sizing, self.obs1.baseline
        )
        self.assertEqual(512, telescope_max)

        base_dir = 'test/data/'
        hpo.generate_workflow_from_observation(
            self.obs1, telescope_max, base_dir, self.component_system_sizing
        )
        self.assertTrue(False)

    def test_workflow_generated_from_observation(self):
        """
        Given an observation, create a workflow file from the observation
        specifications based on the system sizing details provided.


        Returns
        -------

        """

        telescope_max = 512.0  # Taken from total system sizing
        base_dir = "test/data/tmp"
        component_system_sizing = pd.read_csv(TOTAL_SYSTEM_SIZING)
        self.assertTrue(False)