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
import pipelines.hpso_to_observation as hpo
import pipelines.eagle_daliuge_translation as edt
from pipelines.hpso_to_observation import SI


TOTAL_SYSTEM_SIZING = 'data/pandas_sizing/total_compute_SKA1_Low_long.csv'
LGT_PATH = 'tests/data/eagle_lgt_scatter.graph'
PGT_PATH = 'tests/data/daliuge_pgt_scatter.json'


class TestPipelineStructureTranslation(unittest.TestCase):

    def setUp(self) -> None:
        pass

    def test_lgt_to_pgt(self):
        edt.unroll_logical_graph(
            'tests/data/eagle_lgt_scatter.graph',
            'tests/data/daliuge_pgt_scatter.json'
        )
        self.assertTrue(os.path.exists('tests/data/daliuge_pgt_scatter.json'))

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
        nx_graph = edt._daliuge_to_nx(minimal_pgt)
        self.assertEqual(21, len(nx_graph.nodes))
        self.assertEqual(21, len(nx_graph.edges))
        self.assertTrue(('Solve_27', 'Correct_4') in nx_graph.edges)

    def tearDown(self) -> None:
        """
        Remove files generated during various test cases
        Returns
        -------

        """
        if os.path.exists(PGT_PATH):
            os.remove(PGT_PATH)
        self.assertFalse(os.path.exists(PGT_PATH))


class TestCostGenerationAndAssignment(unittest.TestCase):

    def setUp(self) -> None:
        self.channels = 256
        self.system_sizing = pd.read_csv(TOTAL_SYSTEM_SIZING)
        self.observations = []

    def test_generate_workflow_from_observation(self):
        """
        From a single observation, produce a workflow, get the file path,
        and return the location and the 'pipelines' dictionary entry.
        Returns
        -------

        """

        self.assertTrue(False)

    def test_ingest_demand_calc(self):
        max_ingest = self.system_sizing[
            self.system_sizing['HPSO'] == 'hpso01'
            ]['Ingest [Pflop/s]']
        ingest_flops = 32 / 512 * (float(max_ingest) * SI.peta)
        self.assertEqual(123, hpo._find_ingest_demand(self.cluster,
                                                      ingest_flops))
