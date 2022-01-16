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
        with open('tests/data/daliuge_scatter_minimal.json') as f:
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
        minimal_pgt = 'tests/data/daliuge_scatter_minimal.json'
        with open(minimal_pgt) as f:
            jdict = json.load(f)
        nx_graph, task_dict = edt.daliuge_to_nx(jdict, 'DPrepA')
        self.assertEqual(21, len(nx_graph.nodes))
        self.assertEqual(22, len(nx_graph.edges))
        # Make sure the component exists and is properly initialised

        # Expected grid is 1
        # Expected degrid is 1
        self.assertEqual(0, nx_graph.nodes['DPrepA_Flag_0']['comp'])
        self.assertEqual(0, nx_graph.nodes['DPrepA_Grid_0']['comp'])
        # There are two instances of 'Flag' application in the graph, so we
        # expect two edges from this.
        self.assertEqual(2, task_dict['Flag']['node'])
        self.assertEqual(2, task_dict['Flag']['out_edge'])

        self.assertEqual(1, task_dict['FFT']['out_edge'])
        # We want to double check that our translated JSON matches the
        # original relationships in the previous JSON graph.
        # Chck the following:

        with open(PGT_PATH) as f:
            jdict = json.load(f)
        # jdict['oid']
        nx_graph, task_dict = edt.daliuge_to_nx(jdict, 'DPrepA')
        # check our string counter is right, should have 0-2
        self.assertFalse('DPrepA_Flag_3' in nx_graph.nodes)
        # make sure the data is consistent
        example_edge_attr = {
            "data_size": 0,
            "u": "1_-4_0/0",
            "v": "1_-13_0/0/1/0",
            "data_drop_oid": "1_-26_0/0",
        }
        self.assertDictEqual(
            nx_graph.edges['DPrepA_Flag_2', 'DPrepA_FFT_2'],
            example_edge_attr
        )
        # now test for multi-loop example
        # Expected grid is 2
        # expected subtract image is 4
        self.assertEqual(2, task_dict['Grid']['node'])
        self.assertEqual(4, task_dict['Subtract Image Component']['node'])

    def test_json_to_nx(self):
        """
        We use the converted daliuge_to_nx and then add additional
        information required by TopSim.
        """
        self.assertRaises(FileExistsError, edt.eagle_to_nx, NO_PATH,
                          'DPrepA')
        final_graph, task_dict = edt.eagle_to_nx(LGT_PATH, 'DPrepA')

        example_edge_data = {
            "data_size": 0,
            "u": "1_-4_0/0",
            "v": "1_-13_0/0/1/0",
            "data_drop_oid": "1_-26_0/0",
        }
        # nodes = final_graph['graph']['nodes']
        self.assertEqual(36, len(final_graph.nodes))
        # edges = final_graph['graph']['links']
        self.assertEqual(
            example_edge_data,
            final_graph.edges['DPrepA_Flag_2', 'DPrepA_FFT_2']
        )
        # self.assertTrue('task_dict' in final_graph['header'])
        self.assertTrue(6, task_dict['Flag']['node'])

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
        demand = 32
        duration = 3600
        channels = 1
        self.component_sizing = pd.read_csv(COMPONENT_SYSTEM_SIZING)
        self.obs1 = hpo.Observation(
            1, 'hpso01', ['DPrepA', 'DPrepB'], demand, duration, channels,
            'long'
        )

    def testConcatWorkflows(self):
        """
        Generate 2 workflows, and ensure that they have been correctly
        concatenated

        Returns
        -------

        """
        workflows = ['DPrepA', 'DPrepB']
        # minimal_pgt = 'tests/data/daliuge_pgt_scatter_minimal.json'
        # with open(minimal_pgt) as f:
        #     jdict = json.load(f)
        # # We know this has 21 nodes and edges
        final_graphs = {}
        base_graph = LGT_PATH
        # channel_lgt = edt.update_number_of_channels(
        #     base_graph, self.obs1.channels
        # )
        for workflow in self.obs1.workflows:
            nx_graph, task_dict = edt.eagle_to_nx(
                base_graph, workflow, file_in=True
            )
            intermed_graph = hpo.generate_cost_per_product(
                nx_graph, task_dict, self.obs1, workflow,
                self.component_sizing
            )
            final_graphs[workflow] = intermed_graph
        final_graph = edt.concatenate_workflows(final_graphs,
                                                self.obs1.workflows)
        self.assertEqual(72, len(final_graph.nodes))

        # Need to ensure that the DPrepA costs are Different to DPrepB
        self.assertAlmostEqual(
            0.0330766,
            final_graphs['DPrepA'].nodes['DPrepA_Grid_0']['comp'],
            places=5
        )
        self.assertAlmostEqual(
            0.015635,
            final_graphs['DPrepB'].nodes['DPrepB_Grid_0']['comp'],
            places=5
        )
        pass

    def testConcatWorkflowsCost(self):
        """
        We want to be comparable to the SDP parametric model, so we want the
        costs to be the same. Hence, we need to check against the compute that
        is presented in the parametric model, which should be ~5x a single
        pipeline (using their nomenclature).


        Returns
        -------

        """
        # 'ICAL + DPrepA + DPrepB + DPrepC + DPrepD'
        self.obs1.demand = 512
        sdp_par_model_batchcompute = 7861834424122161
        workflows = ['ICAL', 'DPrepA', 'DPrepB', 'DPrepC', 'DPrepD']
        final_graphs = {}
        base_graph = LGT_PATH
        self.obs1.workflows = workflows
        for workflow in self.obs1.workflows:
            nx_graph, task_dict = edt.eagle_to_nx(
                base_graph, workflow, file_in=True
            )
            intermed_graph = hpo.generate_cost_per_product(
                nx_graph, task_dict, self.obs1, workflow,
                self.component_sizing
            )
            final_graphs[workflow] = intermed_graph
        final_graph = edt.concatenate_workflows(final_graphs,
                                                self.obs1.workflows)
        sum = 0
        for node in final_graph:
            sum += final_graph.nodes[node]['comp']

        # ICAL 6.878779923892501
        # DPrepA 2.354166012951267
        # DPrepB 2.503983537367972
        # DPrepC 5.119784348908921
        # DPrepD 0.300741837640721
        self.assertEqual(sdp_par_model_batchcompute/(10**15), sum)



class TestCostGenerationAndAssignment(unittest.TestCase):

    def setUp(self) -> None:
        self.system_sizing = pd.read_csv(TOTAL_SYSTEM_SIZING)
        demand = 512
        duration = 3600
        channels = 64
        self.obs1 = hpo.Observation(
            1, 'hpso01', ['DPrepA', 'DPrepB'], demand, duration, channels,
            'long'
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
        component_cost = hpo.identify_component_cost(
            self.obs1.hpso, self.obs1.baseline, self.obs1.workflows[0],
            'Degrid', self.component_system_sizing
        )
        print(component_cost)
        self.assertAlmostEqual(0.06615325754321748, component_cost, places=5)

        component_cost = hpo.identify_component_cost(
            self.obs1.hpso, self.obs1.baseline, self.obs1.workflows[1],
            'Degrid', self.component_system_sizing
        )
        self.assertAlmostEqual(0.031270291820095455, component_cost, places=5)

    def test_generate_cost_per_product(self):
        """
        Based on the number of channels and the observation specs, we divide
        the cost across those tasks. Here, we test that the division occurs,
        and that the total of all the divisions adds up to the total FLOPs
        for the provided workflow.

        Notes
        ------
        We have standard eagle, which has 2 minor cycles. For an observation
        that is running on

        Returns
        -------

        """
        # generate a workflow based on number of channels in the observation
        wf = self.obs1.workflows[0]
        channels = self.obs1.channels
        nx_graph, task_dict = edt.eagle_to_nx(LGT_PATH, wf)
        self.assertTrue('DPrepA_Degrid_0' in nx_graph.nodes)
        final_workflow, task_dict = hpo.generate_cost_per_product(
            nx_graph, task_dict, self.obs1, wf, self.component_system_sizing
        )
        # We want to make sure the comp cost has been updated

        # 2022 UPDATE - includes more than just degrid (see function docs).

        self.assertAlmostEqual(
            0.045599966584772,
            final_workflow.nodes['DPrepA_Degrid_0']['comp'],
            places=5
        )
        self.assertAlmostEqual(
            15.683419877615755,
            final_workflow['DPrepA_Degrid_0']['DPrepA_Subtract_0'][
                'data_size'
            ],
            places=5
        )

        # UPDATE CHANNELS TO A MUCH LARGER NUMBER
        # self.assertTrue(False)
        channel_lgt = edt.update_number_of_channels(LGT_PATH, channels)
        nx_graph, task_dict = edt.eagle_to_nx(channel_lgt, wf, file_in=False)
        self.assertEqual(128, task_dict['Degrid']['node'])
        self.assertTrue('DPrepA_Degrid_0' in nx_graph.nodes)
        final_workflow, task_dict = hpo.generate_cost_per_product(
            nx_graph, task_dict, self.obs1, wf, self.component_system_sizing
        )

        self.assertAlmostEqual(
            0.0007130320969410361,
            final_workflow.nodes['DPrepA_Degrid_0']['comp'],
            places=5
        )
        self.assertAlmostEqual(
            0.24505343558774617,
            final_workflow['DPrepA_Degrid_0']['DPrepA_Subtract_0'][
                'data_size'
            ],
            places=5
        )

    def testTotalComponentSum(self):
        """
        The sum of each node in the graph should equal the total compute for
        the workflow

        Returns
        -------
        """
        wf = self.obs1.workflows[0]

        nx_graph, task_dict = edt.eagle_to_nx(LGT_PATH, wf)
        self.assertTrue('DPrepA_Degrid_0' in nx_graph.nodes)
        final_workflow, task_dict = hpo.generate_cost_per_product(
            nx_graph, task_dict, self.obs1, wf, self.component_system_sizing
        )

        total = sum(
            [final_workflow.nodes[node]['comp'] for node in final_workflow]
        )
        self.assertAlmostEqual(2.3541660129512665, total, places=5)


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
        base_dir = 'tests/data/'
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
