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

from hpconfig.utils.constants import SI


class SKA_CDR:
    SKALOW_nodes = 896
    SKAMID_nodes = 786
    gpu_per_node = 2
    gpu_peak_flops = 31 * SI.tera  # Double precision
    memory_per_node = 320 * SI.giga

    SKALOW_buffer_storage_per_node = 75 * SI.tera
    SKALOW_buffer_total = SKALOW_nodes * SKALOW_buffer_storage_per_node
    SKAMID_buffer_storage_per_node = 147 * SI.tera
    SKAMID_buffer_total = SKAMID_nodes * SKAMID_buffer_storage_per_node
    SKALOW_storage_per_island = 4.2 * SI.peta
    SKAMID_storage_per_island = 7.7 * SI.peta

    SKALOW_total_compute = (SKALOW_nodes * gpu_per_node * gpu_peak_flops)
    estimated_efficiency = 0.25

    SKAMID_total_compute = (SKAMID_nodes * gpu_per_node * gpu_peak_flops)

    maximal_use_case = 1.5 * (10 ** 21)
    maximal_obs_time = 6*3600 # 6 hours, in seconds


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
            low = np.array(['Low', self.SKALOW_nodes, self.gpu_per_node,
                            self.memory_per_node // SI.giga,
                            self.SKALOW_buffer_storage_per_node // SI.tera,
                            self.gpu_peak_flops // SI.tera,
                            self.SKALOW_buffer_total // SI.peta,
                            self.SKALOW_total_compute // SI.peta])

            mid = ['Mid', self.SKAMID_nodes, self.gpu_per_node,
                   self.memory_per_node // SI.giga,
                   self.SKAMID_buffer_storage_per_node // SI.tera,
                   self.gpu_peak_flops // SI.tera,
                   self.SKAMID_buffer_total // SI.peta,
                   self.SKAMID_total_compute // SI.peta]

            df = pd.DataFrame([low, mid], columns=cols.values())
        else:
            low = ['Low', self.SKALOW_nodes, self.gpu_per_node,
                   self.memory_per_node, self.SKALOW_buffer_storage_per_node,
                   self.gpu_peak_flops,
                   self.SKALOW_buffer_total, self.SKALOW_total_compute]

            mid = ['Mid', self.SKAMID_nodes, self.gpu_per_node,
                   self.memory_per_node, self.SKAMID_buffer_storage_per_node,
                   self.gpu_peak_flops,
                   self.SKAMID_buffer_total, self.SKAMID_total_compute]

            df = pd.DataFrame([low, mid], columns=cols.keys())

        return df

    @property
    def average_flops(self):
        """
        Calculate the average required operations rate given the predicted
        operations efficiency of the system

        Returns
        -------

        """
        return (
            self.SKALOW_total_compute*self.estimated_efficiency,
            self.SKAMID_total_compute*self.estimated_efficiency
        )


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
