# Copyright (C) 2024 RW Bunney

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


from skaworkflows.common import Telescope
from dataclasses import dataclass, asdict


@dataclass
class HPSOParameter:
    telescope: str
    count: int
    hpso: str
    duration: int
    workflows: list
    demand: int
    channels: int
    workflow_parallelism: int
    baseline: float

    def to_dict(self):
        return asdict(self)


class ObservationPlan:

    def __init__(self, telescope: str):
        self.telescope = Telescope(telescope)
        self._plan = {"nodes": self.telescope.default_compute_nodes,
                      "infrastructure": "parametric",
                      "telescope": str(self.telescope),
                      "hpsos": []
                      }

    def add_observation(self, hpso: HPSOParameter):
        self._plan["hpsos"].append(hpso.to_dict())

    def to_json(self):
        return self._plan
