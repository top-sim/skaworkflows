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
import pipeline_cost_generator as pcg
from pipeline_cost_generator import SI
import pipelines.eagle_daliuge_translation as edt

TOTAL_SYSTEM_SIZING = 'data/pandas_sizing/total_compute_SKA1_Low_long.csv'
EAGLE_LGT = 'tests/data/eagle_lgt.graph'
PGT_PATH = 'tests/data/daliuge_pgt.json'


class TestPipelineStructureTranslation(unittest.TestCase):

    def setUp(self) -> None:
        self.lgt_path = EAGLE_LGT
        self.pgt_path = PGT_PATH

    def test_daliuge_nx_conversion(self):
        """
        Once we unroll to a daliuge JSON file, we want to keep this as a NX
        object until we convert it to JSON, which will happen at the point we
        want to allocate computational and data loads to tasks.


        Returns
        -------

        """

        nx = edt._daliuge_to_nx(self.pgt_path)
        self.assertTrue(False)


class TestCostGenerationAndAssignment(unittest.TestCase):

    def setUp(self) -> None:
        self.channels = 256
        self.system_sizing = pd.read_csv(TOTAL_SYSTEM_SIZING)

    def test_generate_workflow_from_observation(self):
        """
        From a single observation, produce a workflow, get the file path,
        and return the location and the 'pipelines' dictionary entry.
        Returns
        -------

        """

    def test_ingest_demand_calc(self):
        max_ingest = self.system_sizing[
            self.system_sizing['HPSO'] == 'hpso01'
            ]['Ingest [Pflop/s]']
        ingest_flops = 32 / 512 * (float(max_ingest) * SI.peta)
        self.assertEqual(123, pcg._find_ingest_demand(self.cluster,
                                                      ingest_flops))