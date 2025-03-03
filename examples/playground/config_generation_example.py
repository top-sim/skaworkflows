# Copyright (C) 6/5/22 RW Bunney

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

"""
"""

import logging
import time
from pathlib import Path
import skaworkflows.config_generator as config_generator
logging.basicConfig(level="INFO")
LOGGER = logging.getLogger(__name__)


# HPSOs observation plans
HPSO_PLANS = ["BasicHPSOPlan.json"]

graph_type = 'prototype'

BASE_DIR = Path("examples/playground/")

WORKFLOW_TYPE_MAP = {
    "ICAL": graph_type,
    "DPrepA": graph_type,
    "DPrepB": graph_type,
    "DPrepC": graph_type,
    "DPrepD": graph_type,
}

SCATTER_WORKFLOW_TYPE_MAP = {
    "ICAL": "scatter",
    "DPrepA": "scatter",
    "DPrepB": "scatter",
    "DPrepC": "scatter",
    "DPrepD": "scatter",
}

low_path = BASE_DIR / ""
mid_path_str = BASE_DIR / ""
start = time.time()

parameters = {
    "telescope" : "low",
    "infrastructure": "parametric",
    "nodes" : 16,
    "timestamp" : "seconds"

}
data = [False]
data_distribution = ["standard"]

for hpso in HPSO_PLANS:
    for d in data:
        for dist in data_distribution:
            config_generator.create_config(
                parameters=parameters,
                output_dir=BASE_DIR / "config",
                base_graph_paths=WORKFLOW_TYPE_MAP,
                data=d,
                data_distribution=dist,
                overwrite=True
            )

finish = time.time()

LOGGER.info(f"Total generation time was: {(finish-start)/60} minutes")
