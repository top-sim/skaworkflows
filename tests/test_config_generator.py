# Copyright (C) 3/9/22 RW Bunney

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

from skaworkflows.config_generator import create_config
import filecmp
from pathlib import Path

from skaworkflows import config_generator


def setUp():
    hpso_path = (
        "tests/data/maximal_low_imaging_896channels.json")

    BASE_DIR = Path(f"tests")

    PROTOTYPE_WORKFLOW_PATHS = {"ICAL": "prototype", "DPrepA": "prototype",
                                "DPrepB": "prototype",
                                "DPrepC": "prototype",
                                "DPrepD": "prototype"}
    #
    # SCATTER_WORKFLOW_PATHS = {"ICAL": "scatter", "DPrepA": "scatter",
    #                           "DPrepB": "scatter", "DPrepC": "scatter",
    #                           "DPrepD": "scatter"}

    LOW_CONFIG = Path("low_sdp_config")

    # TODO we are going to use the file-based config generation for this

    base_graph_paths = PROTOTYPE_WORKFLOW_PATHS
    low_path_str = BASE_DIR / 'tmp'
    # Generate configuration with prototype SKA Workflow
    config_generator.create_config('low', 'parametric', 896,
                                   hpso_path=Path(hpso_path),
                                   output_dir=Path(low_path_str),
                                   base_graph_paths=PROTOTYPE_WORKFLOW_PATHS,
                                   timestep='seconds', data=True)

def test_config_generation_low():
    setUp()
    tmp_path = Path("tests/tmp/low_sdp_config_n896.json")
    valid_json_path  = Path("tests/data/maximal_workflows/low_sdp_config_n896.json")
    assert filecmp.cmp(valid_json_path, tmp_path)


def TestConfigGenerationMid():
    pass