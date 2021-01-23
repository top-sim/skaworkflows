# Copyright (C) 8/1/21 RW Bunney

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

from hpso_to_observation import \
    convert_systemsizing_csv_to_dict, create_observation_plan

class TestObservationPlanGeneration(unittest.TestCase):

    def setUp(self):
        low_compute_data = "csv/SKA1_Low_COMPUTE.csv"
        self.observations = convert_systemsizing_csv_to_dict(low_compute_data)

    def test_create_observation_plan(self):
        """
        create_observation_plan takes the set of observations from the CSV
        and produces a dictionary from this.

        There are a number of constraints on observations plans
        Returns
        -------

        """
        plan = create_observation_plan(self.observations)
