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

from skaworkflows.datagen import pandas_system_sizing as pss

sys.path.insert(0, os.path.abspath('../sdp-par-model'))
from sdp_par_model import reports
from sdp_par_model.parameters.definitions import *

DATA_DIR = 'skaworkflows/data/sdp-par-model_output'
SHORT = f'{DATA_DIR}/2023-03-19_short_HPSOs.csv'
MID = f'{DATA_DIR}/2023-03-19_mid_HPSOs.csv'
LONG = f'{DATA_DIR}/2023-03-25_long_HPSOs.csv'
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

    def test_compile_all_sizing(self):
        total_sizing, component_sizing = pss.compile_baseline_sizing(DATA_DIR)
        self.assertEqual(15, len(total_sizing['SKA1_Low']))
        self.assertEqual(192, len(component_sizing['SKA1_Low']))

    def test_total_sizing_baselines(self):
        ret = pss.csv_to_pandas_total_compute(LONG)
        low = ret['SKA1_Low']
        mid = ret['SKA1_Mid']

        self.assertEqual(5, len(low))
        self.assertEqual(19, len(low.T))
        self.assertEqual(15, len(mid))
        self.assertEqual(20, len(mid.T))


    def test_isolate_total_sizing(self):
        """
        Internal function that processes total costs for specific pipelines
        for cross checking and use in telescope sizing for observations


        Returns
        -------

        """
        df_csv = pd.read_csv(LONG, index_col=0)
        tel = 'SKA1_Mid'
        df_tel = df_csv.T[df_csv.T['Telescope'] == tel].T
        hpso_dict = {header: 0 for header in pss.HPSO_DATA}
        # hpso_dict = {}
        rflop_total, rt_flop_total, hpso_dict = (
            pss._isolate_total_sizing(df_tel, hpso_dict, 'hpso14')
        )

        # Ingest + RCAL + FastImg
        real_time_total_hpso14 = 0.19424524633241602
        # ICAL + DPrepA + DPrepB + DPrepC
        batch_compute_hpso14 = 0.6309361569888947
        self.assertEqual(rt_flop_total, real_time_total_hpso14)
        self.assertEqual(batch_compute_hpso14, rflop_total - rt_flop_total)

        test_dict = {header: 0 for header in pss.HPSO_DATA}
        test_dict.update({
            'HPSO': 'hpso14',
            'Baseline': 25000.0,
            'Stations': 197,
            "Tobs [h]": 28800.0 / 3600,
            'Total Time [s]': 7200000.0,
            'Ingest Rate [TB/s]': 0.11107894713422155,
            "Total RT [Pflop/s]": real_time_total_hpso14,
            "Total Batch [Pflop/s]": batch_compute_hpso14,
            "Total [Pflop/s]": real_time_total_hpso14 + batch_compute_hpso14,
            'RCAL [Pflop/s]': 0.027555502716787368,
            'FastImg [Pflop/s]': 0.013671169517356207,
            'ICAL [Pflop/s]': 0.14887923234478867,
            'DPrepA [Pflop/s]': 0.08903299777714171,
            'DPrepB [Pflop/s]': 0.08626238145668516,
            'DPrepC [Pflop/s]': 0.3067615454102791,
            'DPrepD [Pflop/s]': 0,
            'Ingest [Pflop/s]': 0.15301857409827246,
        })

        self.assertDictEqual(test_dict, hpso_dict)

    def test_process_common_values(self):
        df_csv = pd.read_csv(LONG, index_col=0)
        tel = 'SKA1_Mid'
        df_tel = df_csv.T[df_csv.T['Telescope'] == tel].T
        hpso_dict = {header: 0 for header in pss.HPSO_DATA}
        hpso_dict['HPSO'] = 'hpso13'
        col = 'hpso13 (ICAL) [] [Bmax=35000]'
        hpso_dict = pss._process_common_values(
            df_tel, col, hpso_dict
        )
        test_dict = {header: 0 for header in pss.HPSO_DATA}
        test_dict.update({
            'HPSO': 'hpso13',
            'Baseline': 35000.0,
            'Stations': 197,
            "Tobs [h]": 28800.0 / 3600,
            'Total Time [s]': 18000000.0
        })
        self.assertDictEqual(test_dict, hpso_dict)

    def test_isolate_correct_baseline(self):
        """
        Depending on the baseline generated, there is a reported 'Bmax'
        component of the column name, which follows some bizarre rules.

        This is going to test that we get the right data from
        _isolate_products, and then associate the correct baseline to those
        products.

        Condtions:
        For SKA1_Low:
        * If Max Baseline [km] is 65.0, we do nothing
        * Else - report what the baseline is



        Returns
        -------

        """

        # Long, mid,  hpso15 DPrepC -> [Bmax=15000]

        # Long, mid, hpso13 (FastImg) [] [Bmax=35000]

        final_df_dict = pss.csv_to_pandas_pipeline_components(LONG)
        pl_df_mid = final_df_dict['SKA1_Mid']
        self.assertEqual(
            15000.0,
            pl_df_mid[pl_df_mid['hpso'] == 'hpso15'].loc['Ingest']['Baseline']
        )
        # hpso18(Ingest)[] 150km/150000m
        self.assertEqual(
            150000.0,
            pl_df_mid[pl_df_mid['hpso'] == 'hpso18'].loc['Ingest']['Baseline']
        )
        # hpso37a(RCAL)[][Bmax = 120000]
        self.assertEqual(
            120000.0,
            pl_df_mid[pl_df_mid['hpso'] == 'hpso37a'].loc['Ingest']['Baseline']
        )

        pl_df_low = final_df_dict['SKA1_Low']
        # hpso01(DPrepA)[] max 65km
        self.assertEqual(
            65000.0,
            pl_df_low[pl_df_low['hpso'] == 'hpso01'].loc['DPrepA']['Baseline']
        )

        final_df_dict = pss.csv_to_pandas_pipeline_components(SHORT)
        pl_df_mid = final_df_dict['SKA1_Mid']
        self.assertEqual(
            4062.5,
            pl_df_mid[pl_df_mid['hpso'] == 'hpso15'].loc['Ingest']['Baseline']
        )

    def test_isolated_correct_baseline_components(self):
        """

        Returns
        -------

        """
        df_csv_long = pd.read_csv(
            LONG,
            index_col=0
        )
        df_low_long = df_csv_long.T[df_csv_long.T['Telescope'] ==
                                    "SKA1_Low"].T.fillna(-1)
        df_mid_long = df_csv_long.T[df_csv_long.T['Telescope'] ==
                                    "SKA1_Mid"].T.fillna(-1)
        # Long, mid,  hpso15 Ingest, Flag -> [Bmax=15000]

        # pipeline_products_low = pss._isolate_products(df_low)
        products_mid_long_hpso15 = pss._isolate_products(df_mid_long, 'hpso15')
        self.assertEqual(
            0.0013957169918481806,
            products_mid_long_hpso15['Ingest']['Flag']
        )
        self.assertEqual(
            0.9813000364283198,
            products_mid_long_hpso15['ICAL_data']['Grid']
        )

        self.assertEqual(
            15000,
            products_mid_long_hpso15['Ingest']['Baseline']
        )
        # Long, mid, hpso13, Dprepa, FFT [] [Bmax=35000]
        products_mid_long_hpso13 = pss._isolate_products(df_mid_long, 'hpso13')
        self.assertEqual(
            0.003614069512036861,
            products_mid_long_hpso13['DPrepA']['FFT']
        )

        # Long, low, hpso01, DPrepC, Grid [] (Bmax=65km)
        products_low_long_hpso01 = pss._isolate_products(df_low_long, 'hpso01')
        self.assertEqual(
            0.11530553363337567,
            products_low_long_hpso01['DPrepC']['Grid']
        )

        df_csv_mid = pd.read_csv(
            MID,
            index_col=0
        )

        df_low_mid = df_csv_mid.T[df_csv_mid.T['Telescope'] ==
                                  "SKA1_Low"].T.fillna(-1)
        df_mid_mid = df_csv_mid.T[df_csv_mid.T['Telescope'] ==
                                  "SKA1_Mid"].T.fillna(-1)

        # Medium basline, Mid, hpso015
        products_mid_mid_hpso15 = pss._isolate_products(df_mid_mid, 'hpso15')
        self.assertEqual(
            0.0034172025900270336,
            products_mid_mid_hpso15['Ingest']['Flag']
        )
        products_mid_mid_hpso13 = pss._isolate_products(df_mid_mid, 'hpso13')
        self.assertEqual(
            0.0029486904534746536,
            products_mid_mid_hpso13['DPrepA']['FFT']
        )

        # Medium baseline, SKA Low, hpso04a  Ingest  [Bmax=325000]
        products_low_mid_hpso04a = pss._isolate_products(df_low_mid, 'hpso01')
        self.assertEqual(
            0.04363672860136016,
            products_low_mid_hpso04a['DPrepC']["Grid"]
        )

    def test_compile_baseline_sizing(self):
        """
        Test the system sizing generation that accumulates baselines

        Returns
        -------

        """
        expected_df_length = 192


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

        self.short_file = "2023-03-19_LowBaseline_HPSOs.csv"
        self.mid_file = "2023-03-19_MidBaseline_HPSOs.csv"
        self.long_file = "2023-03-19_HighBaseline_HPSOs.csv"

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
