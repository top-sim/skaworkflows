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


def create_topsim_machine_dict(name: str, num_machines: int, machine_data: dict):
    """
     Produce the dictionary structure for a machine to go into the TopSim config format

    Parameters
    ----------
    name: Name of the machine type
    num_machines: Number of machines
    machine_data: The machine 'specs'.

    Notes
    -----
    The machine_data spec is a dictionary with the following keys:

        - "flops"
        - "compute_bandwidth"
        - "memory"

    Returns
    -------
    machine: dict with the aforementioned specs
    """

    machine = {name: {
        "count": num_machines,
    }}
    machine[name].update(machine_data)
    return machine


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

    total_nodes = 896

    buffer_ratio = (1, 5)

    gpu_per_node = 2
    gpu_peak_flops = 31 * SI.tera  # Double precision
    memory_per_node = 320 * SI.giga

    storage_per_node = 75 * SI.tera
    storage_per_island = 4.2 * SI.peta

    #: Projected efficiency for CDR calculations; since superseded/
    estimated_efficiency = 0.25

    ethernet = 56 / 8 * SI.giga  # GbE

    # required_ingest_rate_per_node = 3.5 / 8 * SI.giga  # Gb/s
    ingest_rate = 0.46 * SI.tera

    #: Largest science use case (in FLOPS)
    maximal_use_case = 1.5 * (10 ** 21)
    maximal_obs_time = 6 * 3600  # 6 hours, in seconds

    used_nodes = total_nodes

    def set_nodes(self, nodes):
        self.used_nodes = nodes

    def to_df(self, human_readable=True):
        """
        Report data as dataframe

        # TODO update 'human_readable' to 'latex_compatible'
        # It's confusing and prints awkwardly
        Returns
        -------

        """
        # cols = {
        #     'telescope': "Telescope",
        #     'num_nodes': '$|M_{\\mathrm{SDP}}$',
        #     'gpu_per_node': '$|P_m|$',
        #     'memory_per_node': '$d$ (GB)',
        #     'storage_per_node': '$s$ (TB)',
        #     'flops_per_node': ' $p_{i}^m$ (TFLOPS)(est.)',
        #     'total_storage': '$\\mathrm{TS}_{\\mathrm{SDP}}$ (PB)',
        #     'total_processing': '$\\mathrm{TP}_{\\mathrm{SDP}}$ (PFLOPS)'
        # }

        cols = {
            'telescope': "Telescope",
            'num_nodes': 'Number of nodes',
            'gpu_per_node': 'GPU/Node',
            'memory_per_node': 'Memory/Node (GB)',
            'storage_per_node': 'Storage/Node (TB)',
            'flops_per_node': 'FLOPS/Node',
            'total_storage': 'Total Storage (PB)',
            'total_processing': 'Total Compute'
        }

        if human_readable:
            low = np.array(['Low', self.total_nodes, self.gpu_per_node,
                            self.memory_per_node // SI.giga,
                            self.storage_per_node // SI.tera,
                            self.gpu_peak_flops // SI.tera,
                            self.total_storage // SI.peta,
                            self.total_compute / SI.peta])

            df = pd.DataFrame([low], columns=cols.values())
        else:
            low = ['Low', self.total_nodes, self.gpu_per_node,
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
            self.total_nodes * self.gpu_per_node * self.gpu_peak_flops * efficiency
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
        return int(self.total_nodes * self.storage_per_node)

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
        return int(self.total_nodes * self.ethernet)

    @property
    def input_transfer_rate(self):
        """
        When we move something between 'input' and 'processing' buffers, this is
        the rate.

        Derived from parametric model

        Returns
        -------

        """
        input_transfer_rate = 1 - self.total_storage * (self.buffer_ratio[
                                                            0] / self.buffer_ratio[1])
        return input_transfer_rate - self.ingest_rate

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
                        "compute_bandwidth": 10.0
                    },
                }
            }
        }


        Returns
        -------

        """
        machine_data = {f"flops": self.gpu_peak_flops * self.gpu_per_node,
                        f"compute_bandwidth": int(self.total_bandwidth / self.total_nodes),
                        f"memory": self.memory_per_node}

        node_dict = create_topsim_machine_dict(name="GenericSDP_LOW_CDR",
                                               num_machines=self.used_nodes,
                                               machine_data=machine_data)

        cluster = {
            "header": {
                "time": False,
                "generator": "hpconfig",
                "version": __version__
            },
            'system': {
                'resources': node_dict,
                'system_bandwidth': self.ethernet
            }
        }
        return cluster

    def generate_parametric_model_values(self):
        """
        Using the Low Adjusted data produced by SDP parametric model.
        We then want to have the resulting configuration expectation for our
        simulator mdodel (that is, what are the specs for each device going to
        look like).

        Returns
        -------
        df
        """
        # SKALOW_nodes = 896
        # gpu_per_node = 2
        # gpu_peak_flops = 31 * SI.tera  # Double precision
        # memory_per_node = 31 * SI.giga
        # TODO make this a class attribute
        hot_rate_per_size = 3 * SI.giga / 10 / SI.tera  # 3 GB/s per 10 TB
        # [NVMe SSD]
        cold_rate_per_size = 0.5 * SI.giga / 16 / SI.tera  # 0.5 GB/s per
        # 16 TB [SATA SSD]

        # # These values are derived from 'somewhere'.
        df = pd.DataFrame([{
            'PFLOP/s': 9.623,
            # This is workflow buffer/offline
            'Input (Cold) Buffer (PB)': 43.35,  # Byte
            # This is ingest buffer
            'Hot Buffer (PB)': 25.5,  # Byte # 2
            'Delivery Buffer (PB)': 0.656,  # Byte
            'Hot Rate (GB/s per 10TB NVMe SSD)': 3,
            'Hot rate (TB/s, global)': 7.65,
            'Cold rate (GB/s) per 16TB SATA SSD)': 0.5,
            'Cold Rate (TB/s, Global)': 1.35
        }])
        return df


class SDP_PAR_MODEL_LOW(SDP_LOW_CDR):
    buffer_ratio = None

    # Assumptions about read-rate. This is how we calulcate runtime I/O costs
    # From Scheduling.ipynb in SDP: 3 GB/s per 10 TB [NVMe SSD]

    compute_buffer_rate = 3 * SI.giga / 10 / SI.tera
    # 0.5 GB/s per 16 TB [SATA SSD]
    input_buffer_rate = 0.5 * SI.giga / 16 / SI.tera
    ingest_rate = 0.46 * SI.tera
    delivery_rate = 100 / 8 * SI.giga  # assuming this is 100Gb ethernet speed?
    architecture_efficiency = 0.173
    total_input_buffer = int(43.35 * SI.peta)
    total_compute_buffer = int(25.50 * SI.peta)
    total_output_buffer = int(0.656 * SI.peta)

    @property
    def total_compute_buffer_rate(self):
        """
        Designed to match the HotBufferRate in the parametric model using the
        current Buffer ratio

        **Should** be 6747312499999.0
        Returns
        -------
        6747312499999
        """
        compute_buffer_rate = (
                self.total_compute_buffer * self.compute_buffer_rate
        )
        return int(
            compute_buffer_rate
            - self.input_transfer_rate
            - self.total_output_transfer_rate
        )

    @property
    def input_transfer_rate(self):
        """
        When we move something between 'input' and 'processing' buffers, this is
        the rate.

        Should be 894687500000.0

        Returns
        -------

        """

        input_transfer_rate = self.total_input_buffer * self.input_buffer_rate

        return input_transfer_rate - self.ingest_rate

    @property
    def total_output_transfer_rate(self):
        output_rate = (
                self.total_output_buffer * self.input_buffer_rate
                - self.delivery_rate
        )
        return output_rate

    @property
    def total_compute(self, expected=False):
        """
        Use archicture efficiency to change the value to be closure to
        parametric model

        Returns
        -------

        """
        return (
                self.total_nodes * self.gpu_per_node * self.gpu_peak_flops
                * self.architecture_efficiency
        )

    def to_topsim_dictionary(self):
        """
        Generate dictionary expected as part of TopSim configuration file

        Notes
        -----
        Here we substitute 'rate's for 'memory bandwidth'.

        Returns
        -------

        """
        # system = {"system": {"resources": {}}}
        machine_data = {
            f"flops": self.gpu_peak_flops * self.gpu_per_node * self.architecture_efficiency,
            f"compute_bandwidth": int(self.total_compute_buffer_rate / self.total_nodes),
            f"memory": self.memory_per_node}

        node_dict = create_topsim_machine_dict(name="GenericSDP_LOW_CDR",
                                               num_machines=self.used_nodes,
                                               machine_data=machine_data)

        cluster = {
            "header": {
                "time": False,
                "generator": "hpconfig",
                "version": __version__
            },
            'system': {
                'resources': node_dict,
                'system_bandwidth': self.ethernet
            }
        }
        return cluster


class SDP_LOW_HETEROGENEOUS(SDP_PAR_MODEL_LOW):
    """
    Build on the existing SDP parametric model numbers, creating a system that uses a
    system with better memory for IO intensive tasks.
    """

    # produce generic with less nodes
    node_dict = {

    }

    # extend node dict with higher-memory nodes


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

    total_nodes = 786
    storage_per_node = 147 * SI.tera
    storage_per_island = 7.7 * SI.peta

    estimated_efficiency = 0.25

    ethernet = 25 / 8 * SI.giga  # GbE
    required_ingest_rate_per_node = 3.8 / 8 * SI.giga  # Gb/s

    #: Largest science use case (in FLOPS)
    maximal_use_case = 1.5 * (10 ** 21)
    maximal_obs_time = 6 * 3600  # 6 hours, in seconds

    used_nodes = total_nodes

    def set_nodes(self, nodes):
        self.used_nodes = nodes

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

        return self.total_nodes * self.storage_per_node

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
            self.total_nodes * self.gpu_per_node * self.gpu_peak_flops * efficiency
        )

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
        return int(self.total_nodes * self.ethernet)

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
            mid = np.array(['Mid', self.total_nodes, self.gpu_per_node,
                            self.memory_per_node // SI.giga,
                            self.total_storage // SI.tera,
                            self.gpu_peak_flops // SI.tera,
                            self.total_storage // SI.peta,
                            self.total_compute // SI.peta])

            df = pd.DataFrame([mid], columns=cols.values())
        else:
            mid = ['mid', self.total_nodes, self.gpu_per_node,
                   self.memory_per_node, self.storage_per_node,
                   self.gpu_peak_flops,
                   self.total_storage, self.total_compute]

            df = pd.DataFrame([mid], columns=cols.keys())

        return df


class SDP_PAR_MODEL_MID(SDP_MID_CDR):
    architecture_efficiency = 0.121
    # We don't use buffer ratio for parametric model
    buffer_ratio = None

    # Assumptions about read-rate. This is how we calulcate runtime I/O costs
    # From Scheduling.ipynb in SDP: 3 GB/s per 10 TB [NVMe SSD]
    compute_buffer_rate = 3 * SI.giga / 10 / SI.tera
    # 0.5 GB/s per 16 TB [SATA SSD]
    input_buffer_rate = 0.5 * SI.giga / 16 / SI.tera
    ingest_rate = 0.46 * SI.tera
    delivery_rate = 100 / 8 * SI.giga  # assuming this is 100Gb ethernet speed?
    total_input_buffer = int(48.455 * SI.peta)
    total_compute_buffer = int(40.531 * SI.peta)
    total_output_buffer = int(1.103 * SI.peta)

    @property
    def total_compute_buffer_rate(self):
        """
        Designed to match the HotBufferRate in the parametric model using the
        current Buffer ratio

        Returns
        -------

        """
        compute_buffer_rate = (
                self.total_compute_buffer * self.compute_buffer_rate
        )
        return int(
            compute_buffer_rate
            - self.input_transfer_rate
            - self.total_output_transfer_rate
        )

    @property
    def input_transfer_rate(self):
        """
        When we move something between 'input' and 'processing' buffers,
        this is the rate.

        Returns
        -------

        """

        input_transfer_rate = self.total_input_buffer * self.input_buffer_rate

        return input_transfer_rate - self.ingest_rate

    @property
    def total_output_transfer_rate(self):
        output_rate = (
                self.total_output_buffer * self.input_buffer_rate
                - self.delivery_rate
        )
        return output_rate

    @property
    def memory_bandwidth(self):
        """
        I/O bandwidth used by the SDP to determine the rate at which we can
        read data at runtime.

        Used in the "compute_bandwidth" value in the TopSim Dictionary for this architecture

        Notes
        ------
        This is derived from the 'HotBufferRate' used in the parametric
        model. The parametric model uses HotBuffer in a different
        way to us.

        Returns
        -------

        """
        # Calculate the global read rate and then distribute across nodes
        return int(self._global_io_rate / self.total_nodes)

    @property
    def total_compute(self, expected=False):
        """
        Use archicture efficiency to change the value to be closure to
        parametric model

        Returns
        -------

        """
        return (
                self.total_nodes * self.gpu_per_node * self.gpu_peak_flops
                * self.architecture_efficiency
        )

    def to_topsim_dictionary(self):
        """
        Generate dictionary expected as part of TopSim configuration file

        Notes
        -----
        Here we substitute 'rate's for 'memory bandwidth'.

        Returns
        -------

        """
        machine_data = {f"flops": self.gpu_peak_flops * self.gpu_per_node * self.architecture_efficiency,
                        f"compute_bandwidth": int(self.total_compute_buffer_rate / self.total_nodes),
                        f"memory": self.memory_per_node}

        node_dict = create_topsim_machine_dict(name="GenericSDP_PAR_MODEL_MID",
                                               num_machines=self.used_nodes,
                                               machine_data=machine_data)

        cluster = {
            "header": {
                "time": False,
                "generator": "hpconfig",
                "version": __version__
            },
            'system': {
                'resources': node_dict,
                'system_bandwidth': self.ethernet
            }
        }
        return cluster


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
