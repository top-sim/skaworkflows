# Copyright (C) 1/4/22 RW Bunney

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

import pytest
from pathlib import Path
from skaworkflows.config_generator import create_config
import skaworkflows.workflow.workflow_analysis as wa
# This is the default graph used for HPSO01 - also at
# skaworkflows.common.pipeline_graphs['dprepa']
PRODUCTION_GRAPH = "skaworkflows/data/hpsos/hpso01/dprepa.graph"
OUTPUT = 'tests/data/workflow_analysis'



@pytest.fixture
def generate_workflow():
    path = create_config('skaworkflows/data/hpsos/hpso01/dprepa.graph')
    return path


