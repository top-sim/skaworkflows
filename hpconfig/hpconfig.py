# Copyright (C) 14/4/21 RW Bunney

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
hpconfig is a generator for hardware configuration files based on existing
and theoretical high-performance computing facilities. Its intended use is to
be used in the development of

hpconfig may be used as a command-line interface, or as an 'API' (single
class) that may be imported into a

"""


import sys
import argparse

from hpconfig.specs.pawsey import galaxy

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='High Performance Computing Config generation'
    )

    parser.add_argument(
        '--environment', help='The system environment you want to model',
        choices=['pawsey-galaxy', 'sdp']
    )
    parser.add_argument(
        '--nogpu', action='store_true',
        help='Configuration that ignores GPU'
    )
    parser.add_argument(
        '--nodefault', nargs='+', type=int,
        help='Update the cpu/gpu combinations from default'
    )
    parser.add_argument(
        '--list_defaults', help='list default configuration for environment'
    )

    args = parser.parse_args()
    if not args:
        parser.print_help()
    curr_env = None
    if args.environment:
        if args.environment == 'pawsey-galaxy':
            if args.nogpu:
                curr_env = galaxy.GalaxyNoGPU()
            else:
                curr_env = galaxy.PawseyGalaxy()
        else:
            print("This environment is currently unsupported")
            parser.print_help()
    else:
        print("Please specify an environment using --environment ")
        parser.print_help()
        sys.exit()

    if args.list_defaults:
        curr_env.print_default_config()

    elif args.nodefault:
            default_str = curr_env.print_config()
            curr_env.architecture = curr_env.update_architecture(args.nodefault)
            if curr_env.architecture:
                print(f'Default Configuration\n')
                print(default_str)
                print(f'New Configuration')
                print(curr_env.print_config())
                curr_env.to_json
            else:
                print("Incorrect number of values passed as arguments")
    else:
        curr_env.to_json()

