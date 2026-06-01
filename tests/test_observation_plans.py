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

import unittest
import pandas as pd

from skaworkflows.observation.permutations import (create_hpso_counts_from_ratios,
                                                   allocate_observations)

class TestObservationPlans(unittest.TestCase):

    def test_observation_plans(self):
        """
        Process for generating observation plans:
            1. Generate set of parameterised HPSOs
            2. (Optional) create N number of these for experimental purposes
            3. Convert this to ObservationPlan
            4. Convert this observation plan to JSON
            5. Read this JSON
            6. Convert this JSON into a list of Observation objects
            7. Shuffle the list of Observation objects
            8. Add times to each Observation object.

            YEAH THIS IS A MESS
        Returns
        -------

        """
        observation_amounts, total_obs = create_hpso_counts_from_ratios(1)
        observation_sizes = pd.Series(
            {"small": 0, "medium": 15, "large": 5})
        plan = allocate_observations(observation_amounts, total_obs,
                                     observation_sizes)

        for hpso in plan:
            hpso
