# Copyright (C) 12/3/20 RW Bunney

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
Contains the specifications and details for the hardware used in Pawsey's
"Galaxy" HPC facility.
https://www.microway.com/knowledge-center-articles/detailed-specifications-intel-xeon-e5-2600v3-haswell-ep-processors/
Based on the above link, the Galaxy Ivy Bridge has 8FLOPs/Cycle
"""

import json
import time

from hpconfig.utils.constants import SI
from hpconfig.utils.classes import CPU_NODE, GPU_NODE


class PawseyGalaxy:
    XeonSandyBridge = CPU_NODE(
        name="XeonSandyBridge",
        cores=8,
        flops_per_cycle=8,
        ncycles=2.6 * SI.giga,
        bandwidth=1600 * SI.mega
    )  # ~240 giga flops

    XeonIvyBridge = CPU_NODE(  # Total flops ~ 480 gigaflops
        name="XeonIvyBridge",
        cores=10,
        flops_per_cycle=16,
        ncycles=3.0 * SI.giga,  # frequency e.g. 2.2GHz
        bandwidth=1866 * SI.mega  # megaherz
    )

    NvidiaKepler = GPU_NODE(
        name="NvidiaKepler",
        memory=6 * SI.giga,
        memory_bandwidth=1300 * SI.mega,
        single_pflops=3.95 * SI.tera,
        double_pflops=1.3 * SI.tera,
        cuda_cores=2688
    )

    def __init__(self):

        self.name = 'PawseyGalaxy'
        memory_per_cpu_node = 64
        memory_per_gpu_node = 32
        self.architecture = {
            'cpu': {
                self.XeonIvyBridge: 50,
                self.XeonSandyBridge: 100,
            },
            'gpu': {
                self.NvidiaKepler: 64
            }
        }

    def update_architecture(self, updated_counts):
        """
        Update the combination of cpus on the system

        Parameters
        ----------
        updated_counts : list()
            List of counts, e.g. [10,25,4] for SandyBride, XeonBrige and Kepler
            respectively

        Returns
        -------

        """
        new_architecture = {'cpu': {}, 'gpu': {}}
        total_counts = (len(self.architecture['cpu'])
                        + len(self.architecture['gpu']))
        if len(updated_counts) != total_counts:
            return None
        i = 0
        for cpu in (self.architecture['cpu'].keys()):
            new_architecture['cpu'][cpu] = updated_counts[i]
            i += i
        for gpu in (self.architecture['gpu'].keys()):
            new_architecture['gpu'][gpu] = updated_counts[i]
            i += i
        return new_architecture

    def print_config(self):
        """
        Helper function for the commandline to list default configuration
        values in the even that a user wants to choose a subset of the
        cluster.


        Returns
        -------
        True if executes completely; exception raised at RunTime if print fails
        """
        print_str = f''
        for hw_type, arch in self.architecture.items():
            if arch:
                for cpu_type, num in self.architecture[hw_type].items():
                    print_str += f'{str(cpu_type)}: {num} CPUs\n'
        return print_str

    def create_config_dict(self):
        resources = {}
        for cpu_type, num in self.architecture['cpu'].items():
            for i in range(num):
                resources[f'{str(cpu_type)}_m{i}'] = {
                    'flops': cpu_type.total_flops(),
                    'rates': 1.0  # TODO update when uncover the value
                }
        for gpu_type, num in self.architecture['gpu'].items():
            for i in range(num):
                resources[f'{str(gpu_type)}_m{i}'] = {
                    'single_flops': gpu_type.single_pflops,
                    'doulbe_flops': gpu_type.double_pflops,
                    'rates': 1.0  # TODO update when uncover the value
                }

        arch = {}
        arch['cpu'] = {
            str(cpu): self.architecture['cpu'][cpu] for cpu in
            self.architecture['cpu']
        }

        arch['gpu'] = {
            str(gpu): self.architecture['gpu'][gpu] for gpu in
            self.architecture['gpu']
        }

        cluster = {
            "cluster": {
                "header": {
                    "time": "false",
                    "generator": "hpconfig",
                    "architecture": arch
                },
                "system": {
                    "resources": resources,
                    "bandwidth": 1.0  # TODO update when uncover the value
                }
            }
        }
        return cluster

    def to_json(self):
        """
        Write configuration to a JSON file

        Returns
        -------
        name : str
            Path to the JSON file
        """
        jdict = self.create_config_dict()
        timestamp = f'{time.time()}'.split('.')[0]
        name = f'{str(self)}_nd_{timestamp}.json'
        with open(name, 'w+') as jfile:
            json.dump(jdict, jfile, indent=4)
        return name

    def __str__(self):
        return self.name


class GalaxyNoGPU(PawseyGalaxy):

    def __init__(self):
        super(GalaxyNoGPU, self).__init__()
        self.name = "GalaxyNoGPU"
        self.architecture = {
            'cpu': {
                self.XeonIvyBridge: 50,
                self.XeonSandyBridge: 100
            },
            'gpu': {}
        }
        memory_per_cpu_node = 64
