# Copyright (C) 2025 RW Bunney

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

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

from pathlib import Path

from skaworkflows.common import Telescope

def get_toml_defaults(telescope: str):
    """
    Given the telescope fetch the right TOML defaults.
    Parameters
    ----------
    telescope

    Returns
    -------

    """
    if "low" in telescope.lower():
        with open(Telescope(telescope).observation_defaults, "rb") as fp:
            return tomllib.load(fp)
    else:
        with open(Telescope(telescope).observation_defaults, "rb") as fp:
            return  tomllib.load(fp)

def load_observation_defaults(telescope: str):
    """
    Load observation defaults and return a useful dictionary of dictionaries,
    rather than a list of dictionaries.
    Parameters
    ----------
    telescope

    Returns
    -------

    """

    tomld = get_toml_defaults(telescope)

    hpsos = {}
    for item in tomld["hpsos"]:
        name = item["hpso"]
        del item["hpso"]
        hpsos[name] = item
    tomld["hpsos"] = hpsos

    return tomld

def create_base_file_from_defaults(telescope: str, path: Path):
    tomld = get_toml_defaults(telescope)


