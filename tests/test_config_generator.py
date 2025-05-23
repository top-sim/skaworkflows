# Copyright (C) 3/9/22 RW Bunney
import shutil
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

from skaworkflows.config_generator import create_config
import filecmp
from pathlib import Path

from skaworkflows import config_generator

HPSO_PARAMETERS = {
    "nodes": 256,
    "infrastructure": "parametric",
    "telescope": "low",
    "hpsos": [
        {
            "count": 2,
            "hpso": "hpso01",
            "demand": 128,
            "duration": 18000,
            "workflows": [
                "ICAL", "DPrepA"],
            "channels": 16384,
            "workflow_parallelism": 64,
            "baseline": 65000.0,
            "telescope": "low"
        },
    ]
}

class TestConfigGeneration(unittest.TestCase):

    def setUp(self):
        self.prototype_workflow_paths = {"ICAL": "prototype", "DPrepA": "prototype"}
        #
        # SCATTER_WORKFLOW_PATHS = {"ICAL": "scatter", "DPrepA": "scatter",
        #                           "DPrepB": "scatter", "DPrepC": "scatter",
        #                           "DPrepD": "scatter"}

        self.low_path_str = Path('tmp')
        # Generate configuration with prototype SKA Workflow

    # def tearDown(self):
    #     shutil.rmtree(self.low_path_str)

    def test_config_generation_low(self):
        config = config_generator.create_config(
            parameters=HPSO_PARAMETERS,
            output_dir=self.low_path_str,
            base_graph_paths=self.prototype_workflow_paths,
            timestep='seconds', data=True)
        self.assertTrue(Path(config[0]).exists())

    def TestConfigGenerationMid(self):
        pass

    def tearDown(self):
        shutil.rmtree('tmp')
