# Copyright (C) 2020 RW Bunney

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>

from abc import ABC, abstractmethod


class ARCHITECTURE(ABC):
    """
    Abstract base class for an Architecture

    The idea is to represent the system specifications of a particular
    cluster or HPC centre using a node-based description. This will likely
    vary a lot depending on the information at hand, and the 'realness' of
    the system (i.e. does it exist or is it theorietical?). The @property
    methods are therefore designed to unify some output so that
    generalisations can be made between architectures.

    """

    # def __init__(self):
    #     pass

    def set_nodes(self, nodes):
        self.nodes = nodes

    @property
    @abstractmethod
    def total_storage(self):
        """
        For the given architecture, calculate the cumulative storage provided

        Notes
        ------
        Output expected in Bytes (easier to manipulate after the fact)

        Returns
        -------
        total_storage : int
            Total storage in bytes
        """
        pass

    @property
    @abstractmethod
    def total_compute(self):
        """
        For the given architecture, calculate the cumulative storage provided

        Notes
        ------
        Output expected in Bytes (easier to manipulate after the fact)

        Returns
        -------
        total_storage : int
            Total storage in bytes
        """

    @property
    @abstractmethod
    def total_bandwidth(self):
        """
        Calculate total bandwidth based cumulative ethernet

        Returns
        -------
        total_bandwidth: int
            Total bandwidth in bytes/sec
        """

    @abstractmethod
    def to_topsim_dictionary(self):
        """
        Create and return a topsim_compatible dictionary

        TopSim expects its system configuration to look as follows:

        "cluster": {
            "header": {
                "time": "false",
                "generator: insert generator"
                }
            },
            "system": {
            "resources": {
                "cat0_m0": {
                    "flops": 84,
                    "rates": 10
                },
                .....
                },
                "cat1_m16": {
                    "flops": 128,
                    "rates": 16
                },

        Returns
        -------

        """


class CPU_NODE:
    def __init__(self, name, cores, flops_per_cycle, ncycles, bandwidth):
        self.name = name
        self.cores = cores
        self.flops_per_cycle = flops_per_cycle
        self.ncyles = ncycles
        self.bandwidth = bandwidth

    def __str__(self):
        return self.name

    def __repr__(self):
        return (
            f'{self.name}(cores={self.cores},flops/cycle='
            f'{self.flops_per_cycle},Number of Cycles={self.ncyles}'
            f'bandwidth={self.bandwidth})'
        )

    def total_flops(self):
        """
        Calculate total flops based on hardware specs


        Returns
        -------
        Integer FLOPS values
        """
        return self.cores * self.flops_per_cycle * self.ncyles


class GPU_NODE:
    def __init__(
            self, name, memory, memory_bandwidth, single_pflops,
            double_pflops, cuda_cores
    ):
        self.name = name
        self.memory = memory
        self.memory_bandwidth = memory_bandwidth
        self.single_pflops = single_pflops
        self.double_pflops = double_pflops
        self.cuda_cores = cuda_cores

    def __str__(self):
        return self.name
