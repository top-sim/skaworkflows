# Copyright (C) 5/1/23 RW Bunney

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
import logging

logging.basicConfig(level="INFO")
LOGGER = logging.getLogger(__name__)

from pathlib import Path

from skaworkflows import config_generator

hpso_path = (
    "tests/generator/maximal_low_imaging_896channels.json"
)

BASE_DIR = Path(f"tests/generator")

PROTOTYPE_WORKFLOW_PATHS = {"ICAL": "prototype", "DPrepA": "prototype",
                            "DPrepB": "prototype", "DPrepC": "prototype",
                            "DPrepD": "prototype"}
#

LOW_CONFIG = Path("low_sdp_config")

base_graph_paths = PROTOTYPE_WORKFLOW_PATHS
low_path_str = BASE_DIR / 'compute_and_data_runtime'

config_generator.create_config('low', 'parametric', 896,
                               hpso_path=Path(hpso_path),
                               output_dir=Path(low_path_str),
                               cfg_name=f'{LOW_CONFIG}_n{896}.json',
                               base_graph_paths=PROTOTYPE_WORKFLOW_PATHS,
                               timestep='seconds', data=False)

# Generate configuration with prototype SKA Workflow
config_generator.create_config('low', 'parametric', 896,
                               hpso_path=Path(hpso_path),
                               output_dir=Path(low_path_str),
                               cfg_name=f'{LOW_CONFIG}_n{896}.json',
                               base_graph_paths=PROTOTYPE_WORKFLOW_PATHS,
                               timestep='seconds', data=True)

