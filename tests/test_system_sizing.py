# Copyright (C) 19/3/21 RW Bunney

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
This test module ensures that the Pandas-oriented approach to retreiving data
from the SDP parametric model report CSVs is equivalent to using the
sdp-par-model helper methods.

The test data used was generated using methods described in
SKA1_System_Sizing.ipynb, located in the sdp-par-model repository
https://github.com/ska-telescope/sdp-par-model.
"""

import unittest
import pandas as pd

ORIGINAL_CSV = 'data/SKA1_Low_COMPUTE.csv '

class TestSystemSizingBackwardsCompatibility(unittest.TestCase):

    def setUp(self):
        """
        Introduce

        Attributes
        ----------
        orig_csv : pd.csv

        Returns
        -------
        None
        """

        self.orig_csv = pd.read_csv()
        pass