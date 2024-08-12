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
import os
import shutil
import unittest
import random

import pandas as pd

from pathlib import Path

from skaworkflows.workflow.hpso_to_observation import (
    Observation,
    create_observation_from_hpso,
)

from skaworkflows.workflow.hpso_to_observation import (
    create_observation_plan,
    create_buffer_config,
    calc_ingest_demand,
    generate_instrument_config,
)

from skaworkflows.common import SI

from skaworkflows.hpconfig.specs.sdp import (
    SDP_LOW_CDR,
    SDP_PAR_MODEL_LOW,
    SDP_PAR_MODEL_MID,
)

SYSTEM_SIZING_DIR = "skaworkflows/data/pandas_sizing"
TOTAL_SYSTEM_SIZING = ("skaworkflows/data/pandas_sizing/"
                       "total_compute_SKA1_Low_2024-03-25.csv")

COMPONENT_SYSTEM_SIZING = (
    "skaworkflows/data/pandas_sizing/component_compute_SKA1_Low_2024-03-25.csv"
)
# CLUSTER = 'tests/PawseyGalaxy_nd_1619058732.json'

EAGLE_LGT = "tests/data/eagle_lgt.graph"

PATH_NOT_EXISTS = "path/doesnt/exist"


class TestObservationClass(unittest.TestCase):
    def setUp(self):
        self.obs1 = Observation(
            name = 2,
            hpso = "hpso01",
            workflows= ["dprepa"],
            demand=32,
            duration=60,
            channels=256*128,
            coarse_channels=256,
            baseline=65000.0,
            telescope='low'
        )

        self.obs2 = Observation(4, "hpso04a", ["dprepa"], 16, 30, 256*128, 256, 65000.0, 'low' )

    def test_add_workflow_path(self):
        self.assertRaises(RuntimeError, self.obs1.add_workflow_path, PATH_NOT_EXISTS)
        # self.assert()


class TestObservationPlanGeneration(unittest.TestCase):
    def setUp(self):
        # low_compute_data = "csv/SKA1_Low_COMPUTE.csv"
        # self.observations = convert_systemsizing_csv_to_dict(low_compute_data)
        self.obs1 = Observation(2, "hpso01", ["dprepa"], 32, 60, 256*128, 256, 65000.0,'low')
        self.obslist1 = create_observation_from_hpso(
            2, "hpso01", ["dprepa"], 32, 60, 256*128, 256, 65000.0, 'low', offset=0
        )
        self.obslist2 = create_observation_from_hpso(
            4, "hpso04a", ["dprepa"], 16, 30, 256*128, 128, 65000.0, 'low', offset=0
        )
        self.obs3 = Observation(3, "hpso01", ["dprepa"], 32, 30, 256*128, 256, 65000.0,'low')
        self.system_sizing = pd.read_csv(TOTAL_SYSTEM_SIZING)
        self.max_telescope_usage = 32  # 1/16th of the telescope

    def test_create_observation_plan_notiebreaks(self):
        """
        create_observation_plan takes the set of observations and generates a sequence
        and produces a dictionary from this.
        (0, 60, 32, 'hpso01', 'dprepa', 256)
        (60, 90, 16, 'hpso04a', 'dprepa',256)
        (60, 90, 16, 'hpso04a', 'dprepa',256)
        (90, 150, 32, 'hpso01', 'dprepa',256)
        (150, 180, 16, 'hpso04a', 'dprepa',256)
        (180, 210, 16, 'hpso04a', 'dprepa',256)

        There are a number of constraints on observations plans
        Returns
        -------
        """
        random.seed(0)
        # obslist = (self.obs1.unroll_observations()
        #            + self.obs2.unroll_observations())
        obslist = self.obslist1 + self.obslist2
        plan = create_observation_plan(obslist, self.max_telescope_usage)
        self.assertEqual("hpso01_1", plan[0].name)
        self.assertEqual(0, plan[0].start)
        self.assertEqual(150, plan[5].start)


class TestObservationTopSimTranslation(unittest.TestCase):
    def setUp(self):
        self.obs1 = Observation(
            name="hpso01_0",
            hpso="hpso01",
            demand=512,
            duration=60,
            workflows=["DPrepA"],
            channels=256*128,
            coarse_channels=256,
            baseline=65000.0,
            telescope='low'
        )

        self.observation_list = create_observation_from_hpso(
            count=2,
            hpso="hpso01",
            demand=512,
            duration=60,
            workflows=["DPrepA"],
            channels=256*128,
            coarse_channels=256,
            baseline=65000.0,
            telescope='low',
            offset=0,
        )
        self.observation_plan = create_observation_plan(self.observation_list, 512)
        self.max_telescope_usage = 32  # 1/16th of telescope
        self.plan = [
            (0, 60, 32, "hpso01", "dprepa", 256, 65000.0),
            (60, 120, 32, "hpso01", "dprepa", 256, 65000.0),
        ]
        self.obs2 = Observation(
            "hpso01_2",
            hpso="hpso01",
            workflows=["DPrepA"],
            demand=32,
            duration=60,
            channels=256*128,
            coarse_channels=256,
            baseline=65000.0,
            telescope='low'
        )
        self.obs3 = Observation(
            "hpso01_5",
            "hpso04a",
            ["DPrepA"],
            16,
            30,
            256*128,
            256,
            65000.0,
            'low'
        )
        self.system_sizing = pd.read_csv(TOTAL_SYSTEM_SIZING)
        self.component_sizing = pd.read_csv(COMPONENT_SYSTEM_SIZING)
        sdp = SDP_LOW_CDR()
        self.cluster = sdp.to_topsim_dictionary()
        self.config_dir_path = Path("tests/data/config")
        self.config_dir_path.mkdir(exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.config_dir_path)

    def testIngestDemandCalc(self):
        # Start with a spec, then determine how many machines we will need
        # based on the ingest size
        # ingest_flops = 32 / 512 * (float(max_ingest) * SI.peta)
        machines, ingest_flops, ingest_data = calc_ingest_demand(
            self.obs1, self.system_sizing, self.cluster
        )
        self.assertEqual(11, machines)
        self.assertEqual(632428093239751.1, ingest_flops)

    def testObservationWorkflowDict(self):
        """
        Produce the workflow entry for an observation:

        "hpso01_1": {
            "workflow": "test/data/config/workflow_config.json",
            "ingest_demand": 5
            },

        Returns
        -------

        """
        # self.observation_lis
        base_graph_paths = {"DPrepA": "prototype", "DPrepB": "prototype"}
        final_instrument_config = generate_instrument_config(
            self.observation_list,
            512,
            self.config_dir_path,
            self.component_sizing,
            self.system_sizing,
            self.cluster,
            base_graph_paths,
        )
        self.assertEqual(
            11,
            final_instrument_config["telescope"]["pipelines"]["hpso01_0"][
                "ingest_demand"
            ],
        )
        self.assertEqual(
            "workflows/hpso01_time-60_channels-256_tel-512-standard.json",
            final_instrument_config["telescope"]["pipelines"]["hpso01_1"]["workflow"],
        )
        self.assertEqual(
            {
                "name": "hpso01_1",
                "start": 0,
                "duration": 60,
                "instrument_demand": 512,
                "type": "hpso01",
                "data_product_rate": 459024629760.0,
            },
            final_instrument_config["telescope"]["observations"][0],
        )

    def test_buffer_config_sizing(self):
        """
        Call the generate_buffer_config, which is a wrapper for hpconfig
        buffer_to_topsim config

        Buffer information is in hpconfig or the Total System sizing. At the
        beginning of the configuration, the only necessary information for
        the buffer is how we split the Hot and Cold, as this is not currently
        set in stone.

        Returns
        -------

        """
        buffer_ratio = (1, 5)
        sdp = SDP_LOW_CDR()
        # For SDP_LOW_CDR, total_storage is 67.2 PetaBytes
        # TODO get values from sdp.total_storage instead
        hot_buffer = 13.44  # 20% of buffer
        cold_buffer = 53.76  # 80% of buffer
        spec = create_buffer_config(sdp)
        self.assertEqual(spec["hot"]["capacity"] / SI.peta, hot_buffer)
        self.assertEqual(spec["cold"]["capacity"] / SI.peta, cold_buffer)
        self.assertEqual(spec["hot"]["max_ingest_rate"] / SI.giga, 460)


class testLowParametricBufferConfig(unittest.TestCase):
    def setUp(self):
        self.sdp = SDP_PAR_MODEL_LOW()
        self.cluster = self.sdp.to_topsim_dictionary()

        self.spec = create_buffer_config(self.sdp)

    def testCapacityCorrect(self):
        # Input buffer
        self.assertEqual(
            43350000000000000,
            self.spec["hot"]["capacity"],
        )

        # Offline buffer
        self.assertEqual(25500000000000000, self.spec["cold"]["capacity"])

    def testLowDataRates(self):
        # Ensure this aligns with input transfer rate
        self.assertEqual(894687500000.0, self.spec["cold"]["max_data_rate"])

        # Ensure that offline data rates (internal I/O rates for offline
        # workflows) are correct
        self.assertEqual(6747312499999.0, self.sdp.total_compute_buffer_rate)
        self.assertEqual(
            int(self.sdp.total_compute_buffer_rate / self.sdp.total_nodes),
            self.cluster["system"]["resources"]["GenericSDP_m0"]["compute_bandwidth"],
        )


class testMidParametricBufferConfig(unittest.TestCase):
    def setUp(self):
        self.sdp = SDP_PAR_MODEL_MID()
        self.cluster = self.sdp.to_topsim_dictionary()

        self.spec = create_buffer_config(self.sdp)

    def testMidCapacityCorrect(self):
        # Input buffer
        self.assertEqual(
            4.8455e16,
            self.spec["hot"]["capacity"],
        )

        # Offline buffer
        self.assertEqual(4.0531e16, self.spec["cold"]["capacity"])

    def testMidDataRates(self):
        # Ensure this aligns with input transfer rate
        self.assertEqual(1054218750000.0, self.spec["cold"]["max_data_rate"])

        # Ensure that offline data rates (internal I/O rates for offline
        # workflows) are correct
        self.assertEqual(11083112499999.0, self.sdp.total_compute_buffer_rate)
        self.assertEqual(
            int(self.sdp.total_compute_buffer_rate / self.sdp.total_nodes),
            self.cluster["system"]["resources"]["GenericSDP_m0"]["compute_bandwidth"],
        )
