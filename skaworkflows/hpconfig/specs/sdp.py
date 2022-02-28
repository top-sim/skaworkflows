# Copyright (C) 16/3/20 RW Bunney

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
Contains the specifications and details for the SDP hardware outlined in SKA­TEL­SDP­0000091

https://www.microway.com/knowledge-center-articles/detailed-specifications-intel-xeon-e5-2600v3-haswell-ep-processors/
Based on the above link, the Galaxy Ivy Bridge has 8FLOPs/Cycle
"""
import numpy as np
import pandas as pd

from skaworkflows.common import SI
from skaworkflows.hpconfig.utils.classes import ARCHITECTURE
from skaworkflows import __version__

class SDP_LOW_CDR(ARCHITECTURE):
    """
    Itemised description of the SKA Low Science Data Processor architecture,
    as discussed in the SDP CDR documentation.

    Per-node summary of the archictecture that was used to assist in the
    costing for the SKA SDP.

    Notes
    ------
    Data derived from:
    - SDP Memo 025: Updated SDP Cost Basis of Estimate June 2016
    - SDP Parametric Model: https://github.com/ska-telescope/sdp-par-model

    """
    nodes = 896

    gpu_per_node = 2
    gpu_peak_flops = 31 * SI.tera  # Double precision
    memory_per_node = 320 * SI.giga

    storage_per_node = 75 * SI.tera
    storage_per_island = 4.2 * SI.peta

    #: Projected efficiency for CDR calculations; since superseded/
    estimated_efficiency = 0.25

    ethernet = 56 / 8 * SI.giga  # GbE

    required_ingest_rate_per_node = 3.5 / 8 * SI.giga  # Gb/s

    #: Largest science use case (in FLOPS)
    maximal_use_case = 1.5 * (10 ** 21)
    maximal_obs_time = 6 * 3600  # 6 hours, in seconds

    def to_df(self, human_readable=True):
        """
        Report data as dataframe

        Returns
        -------

        """
        cols = {
            'telescope': "Telescope",
            'num_nodes': '$|M_{\\mathrm{SDP}}$',
            'gpu_per_node': '$|P_m|$',
            'memory_per_node': '$d$ (GB)',
            'storage_per_node': '$s$ (TB)',
            'flops_per_node': ' $p_{i}^m$ (PFLOPS)(est.)',
            'total_storage': '$\\mathrm{TS}_{\\mathrm{SDP}}$ (PB)',
            'total_processing': '$\\mathrm{TP}_{\\mathrm{SDP}}$ (PFLOPS)'
        }

        if human_readable:
            low = np.array(['Low', self.nodes, self.gpu_per_node,
                            self.memory_per_node // SI.giga,
                            self.storage_per_node // SI.tera,
                            self.gpu_peak_flops // SI.tera,
                            self.total_storage // SI.peta,
                            self.total_compute // SI.peta])

            df = pd.DataFrame([low], columns=cols.values())
        else:
            low = ['Low', self.nodes, self.gpu_per_node,
                   self.memory_per_node, self.storage_per_node,
                   self.gpu_peak_flops,
                   self.total_storage, self.total_compute]

            df = pd.DataFrame([low], columns=cols.keys())

        return df

    @property
    def total_compute(self, expected=False):
        """
        Cumulative compute power provided by nodes in the system specification.

        Parameters
        -----------
        expected : bool
            If `True`, use expected_efficiency of system to get an idea of
            what the expected 'acheived' compute will be

        Notes
        -----
        Expected efficiency should be ~13.6PFLOPs according to CDR (Memo 25)

        Returns
        -------
        int : Total compute power of the cluster, in FLOPS
        """

        efficiency = 1.0
        if expected:
            efficiency = self.estimated_efficiency
        return int(
            self.nodes * self.gpu_per_node * self.gpu_peak_flops * efficiency
        )

    @property
    def total_storage(self):
        """
        Cumulative storage provided by nodes in the system specification

        Notes
        ------
        Total storage acts as Total Buffer size for the SDP.

        Returns
        -------

        """
        return int(self.nodes * self.storage_per_node)

    @property
    def total_bandwidth(self):
        """
        Cumulative bandwidth provided by nodes in system specification

        Notes
        ------
        Total bandwidth doubles as maximum ingest rate for the Buffer

        Returns
        -------

        """
        return int(self.nodes * self.ethernet)

    def to_topsim_dictionary(self):
        """
        Generate dictionary expected as part of TopSim configuration file

        "cluster": {
            "header": {
                "time": "false",
                "generator": "hpconfig",
                "architecture": {
                    "cpu": {
                        "GenericSDP": num_nodes,
                    },
                    "gpu": {}
                }
            },
            "system": {
                "resources": {
                    "XeonIvyBridge_m0": {
                        "flops": 48000000.0,
                        "rates": 10.0
                    },
                }
            }
        }


        Returns
        -------

        """
        node_dict = {}
        # system = {"system": {"resources": {}}}
        for i in range(0, self.nodes):
            node_dict[f"GenericeSDP_m{i}"] = {
                f"flops": self.gpu_peak_flops * self.gpu_per_node,
                f"rates": self.ethernet,
                f"memory": self.memory_per_node
            }
        cluster = {
            "header": {
                "time": False,
                "generator": "hpconfig",
                "version": __version__
            },
            'system': {
                'resources': node_dict
            }
        }
        return cluster


class SDP_MID_CDR(ARCHITECTURE):
    """
    Itemised description of the SKA Mid Science Data Processor architecture,
    as discussed in the SDP CDR documentation.

    Per-node summary of the archictecture that was used to assist in the
    costing for the SKA SDP.

    Notes
    ------
    Data derived from:
    - SDP Memo 025: Updated SDP Cost Basis of Estimate June 2016
    - SDP Parametric Model: https://github.com/ska-telescope/sdp-par-model
    """

    gpu_per_node = 2
    gpu_peak_flops = 31 * SI.tera  # Double precision
    memory_per_node = 320 * SI.giga

    nodes = 786
    storage_per_node = 147 * SI.tera
    storage_per_island = 7.7 * SI.peta

    estimated_efficiency = 0.25

    ethernet = 25 / 8 * SI.giga  # GbE

    required_ingest_rate_per_node = 3.8 / 8 * SI.giga  # Gb/s

    #: Largest science use case (in FLOPS)
    maximal_use_case = 1.5 * (10 ** 21)
    maximal_obs_time = 6 * 3600  # 6 hours, in seconds

    @property
    def total_storage(self):
        """
        Total data storage for SDP MID CDR requirements

        Notes
        -----
        This value becomes the SDP data buffer when considering total system
        sizing.

        Returns
        -------
        int : Total data of combined nodes in SDP
        """

        return self.nodes * self.storage_per_node

    @property
    def total_compute(self, expected=False):
        """
        Cumulative compute power provided by nodes in the system specification.

        Parameters
        -----------
        expected : bool
            If `True`, use expected_efficiency of system to get an idea of
            what the expected 'acheived' compute will be

        Notes
        -----
        Expected efficiency should be ~13.6PFLOPs according to CDR (Memo 25)

        Returns
        -------
        int : Total compute power of the cluster, in FLOPS
        """

        efficiency = 1.0
        if expected:
            efficiency = self.estimated_efficiency
        return int(
            self.nodes * self.gpu_per_node * self.gpu_peak_flops * efficiency
        )

    def to_topsim_dictionary(self):
        """
        Generate dictionary expected as part of TopSim configuration file

        Returns
        -------
        dict : JSON encodable dictionary of self.nodes descriptions
        """
        pass

    def to_df(self, human_readable=True):
        """
        Report data as dataframe

        Returns
        -------

        """
        cols = {
            'telescope': "Telescope",
            'num_nodes': '$|M_{\\mathrm{SDP}}$',
            'gpu_per_node': '$|P_m|$',
            'memory_per_node': '$d$ (GB)',
            'storage_per_node': '$s$ (TB)',
            'flops_per_node': ' $p_{i}^m$ (PFLOPS)(est.)',
            'total_storage': '$\\mathrm{TS}_{\\mathrm{SDP}}$ (PB)',
            'total_processing': '$\\mathrm{TP}_{\\mathrm{SDP}}$ (PFLOPS)'
        }

        if human_readable:
            mid = np.array(['Mid', self.nodes, self.gpu_per_node,
                            self.memory_per_node // SI.giga,
                            self.total_storage // SI.tera,
                            self.gpu_peak_flops // SI.tera,
                            self.total_storage // SI.peta,
                            self.total_compute // SI.peta])

            df = pd.DataFrame([mid], columns=cols.values())
        else:
            mid = ['mid', self.nodes, self.gpu_per_node,
                   self.memory_per_node, self.storage_per_node,
                   self.gpu_peak_flops,
                   self.total_storage, self.total_compute]

            df = pd.DataFrame([mid], columns=cols.keys())

        return df


class SKA_Adjusted:
    SKALOW_nodes = 896
    SKAMID_nodes = 786
    gpu_per_node = 2
    gpu_peak_flops = 31 * SI.tera  # Double precision
    memory_per_node = 31 * SI.giga

    SKALOW_buffer_size = 67 * SI.peta
    SKAMID_buffer_size = 116 * SI.peta
    SKALOW_buffer_storage_per_node = 75 * SI.tera
    SKAMID_buffer_storage_per_node = 147 * SI.tera
    SKALOW_storage_per_island = 4.2 * SI.peta
    SKAMID_storage_per_island = 7.7 * SI.peta

    # Maximmal usecase
    #
    # maximal_use_case = 1.5 * (10 ** 21)  # FLOPS
    # Maximal_obs_time = 2160  # sec
    # # 1 Petaflop	1.00E+15
    # # Average FLOP/s	1.36E+016
    # time_on_system = maximal_use_case / average_flops / 21600
    # Time
    # on
    # system
    # 30.6372549019608
    # FLOPs
    # for .77777777777778E+017
    # Additional
    # nodes
    # with 25 % 3585
    #     Total
    # nodes = 4481


def sdp_to_csv():
    pass
    # df = pd.DataFrame([low, mid], columns=cols.values())
