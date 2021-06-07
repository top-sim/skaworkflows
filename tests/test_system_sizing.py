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

import sys
import os
import unittest
import pandas as pd

sys.path.insert(0, os.path.abspath('../sdp-par-model'))
from sdp_par_model import reports
from sdp_par_model.parameters.definitions import *

from data import pandas_system_sizing as pss

DATA_DIR = 'data/parametric_model'
SHORT = f'{DATA_DIR}/2021-06-02_short_HPSOs.csv'
MID = f'{DATA_DIR}/2021-06-02_mid_HPSOs.csv'
LONG = f'{DATA_DIR}/2021-06-02_long_HPSOs.csv'
# Copied from sdp-par-model repository; to make sure our self-generated
# numbers are accurate.
BACK_COMPATIBLE = f'tests/data/2021-02-03-895254e_hpsos.csv'


class TestSystemPandasOutputCompute(unittest.TestCase):

    def setUp(self):
        """
        For a given parametric model system sizing, scoop up the interesting
        data and make sure we get it in the correct format.

        Notes
        -----
        Our system sizing separates telescopes (LOW and MID), so for each 1
        (one) system report (for Short, Mid, and Long baseline lengths) there
        are 2 (two) pandas-compatible reports.
        Returns
        -------
        """
        pass

    def test_hpso_report_generated(self):
        ret = pss.csv_to_pandas_total_compute(LONG)
        for telescope in ret:
            ret[telescope].to_csv(
                f'tests/data/total_compute_{telescope}_LONG.csv'
            )
            self.assertTrue(
                os.path.exists(
                    f'tests/data/total_compute_{telescope}_LONG.csv'
                )
            )
            os.remove(f'tests/data/total_compute_{telescope}_LONG.csv')

    def test_pipeline_component_report_generate(self):
        ret = pss.csv_to_pandas_pipeline_components(LONG, 'long')
        self.assertNotEqual({}, ret)
        data = ret['SKA1_Low'][['hpso', 'Correct']]

        # Check the size is large enough
        self.assertEqual(60, len(data))

        # If there are issues, it will get grumpy here and fail.
        for telescope in ret:
            ret[telescope].to_csv(
                f'tests/data/component_compute_{telescope}_SHORT.csv'
            )
            self.assertTrue(
                os.path.exists(
                    f'tests/data/component_compute_{telescope}_SHORT.csv'
                )
            )
            os.remove(f'tests/data/component_compute_{telescope}_SHORT.csv')

    def test_pipeline_components_accuracy(self):
        df = pss.csv_to_pandas_pipeline_components(LONG)

        # IN the pandas CSV, we would do df['Correct'].loc['DprepA'] to get
        # this value for LOW at Max Baseline
        dprepa_hpso01_correct = 0.059946046291823374
        res = df['SKA1_Low'][['hpso', 'Correct']].loc['DPrepA']
        val = res[res['hpso'] == 'hpso01']['Correct']
        self.assertTrue(dprepa_hpso01_correct, val)
        rcal_hpso01_correct_data = 0.36783429678851104
        res = df['SKA1_Low'][['hpso', 'Correct']].loc['RCAL_data']
        val = res[res['hpso'] == 'hpso01']['Correct']
        self.assertTrue(rcal_hpso01_correct_data, val)

        # Use DF to check results


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

        pass

    def test_baseline_comparisons(self):
        short = pd.read_csv(SHORT, index_col=0)
        sflag_val = 0.0003936331841155408
        scolumn = 'hpso01 (Ingest) [Bmax=4062.5]'
        data = float(
            short[scolumn].loc['-> Flag [''PetaFLOP/s]']
        )
        self.assertEqual(sflag_val, data)

        mid = pd.read_csv(MID, index_col=0)
        mflag_val = 0.006281068530030465
        data = float(
            mid['hpso01 (Ingest) [Bmax=32500]'].loc['-> Flag [''PetaFLOP/s]']
        )
        self.assertEqual(mflag_val, data)
        # Mid-length baseline should have greater compute.
        self.assertGreater(mflag_val, sflag_val)

        long = pd.read_csv(LONG, index_col=0)
        lflag_val = 0.01063407058944
        data = float(long['hpso01 (Ingest) []'].loc['-> Flag [PetaFLOP/s]'])
        self.assertEqual(lflag_val, data)
        # Ensure that lflag is greater than mflag
        self.assertGreater(lflag_val, mflag_val)
        # Ensure consistency with previous data generated
        prev = pd.read_csv(BACK_COMPATIBLE, index_col=0)
        bc_data = float(prev['hpso01 (Ingest) []'].loc['-> Flag [PetaFLOP/s]'])
        self.assertEqual(bc_data, lflag_val)
        # self.assertEqual(bc_data, data)




# NOTE This test will take a long time to run (At least 30 minutes) as it
# will re-generate the System Reports that are in the data/ directory.

@unittest.skip
class TestParametricModelGeneration(unittest.TestCase):

    def setUp(self) -> None:
        """
        Use the parametric model to generic the reports that produce the
        system sizing we use for our

        Notes
        ------
        This particular test requires you to have the parametric model
        available on your machine. We have also edited the `reports`
        directory to produce the data output for each pipeline component.
        The following code has been added to the `reports.RESULT_MAP` list:

        ```python3
        ('-> ', 'Tera/s', True, True, lambda tp: tp.get_products(
                'Rout',scale=c.tera
            )
        ),
        ```

        Returns
        -------
        """

        self.short_file = "2021-06-02_LowBaseline_HPSOs.csv"
        self.mid_file = "2021-06-02_MidBaseline_HPSOs.csv"
        self.long_file = "2021-06-02_HighBaseline_HPSOs.csv"

        self.short = {"Bmax": 4062.6}
        self.mid = {"Bmax": 32500}
        self.long = ""  # Max baseline is default

    def test_low_baseline_output(self):
        ret = reports.write_csv_hpsos(self.short_file,
                                      HPSOs.available_hpsos,
                                      parallel=0, adjusts=self.short,
                                      verbose=False)

    def test_mid_baseline_output(self):
        ret = reports.write_csv_hpsos(self.mid_file,
                                      HPSOs.available_hpsos,
                                      parallel=0, adjusts=self.mid,
                                      verbose=False)

    def test_long_baseline_output(self):
        ret = reports.write_csv_hpsos(self.mid_file,
                                      HPSOs.available_hpsos,
                                      parallel=0, adjusts=self.mid,
                                      verbose=False)
