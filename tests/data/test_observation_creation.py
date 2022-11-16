# Copyright (C) 1/4/22 RW Bunney

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


import pytest
from pathlib import Path

import skaworkflows.workflow.hpso_to_observation as hto

from skaworkflows.common import LOWBaselines

@pytest.fixture()
def read_hpso_spec():
    """
    Create path object for process_hpso_from_spec

    Returns
    -------
    path: pathlib.Path
        A Path object for `'tests/data/hpso_spec.json'`
    """

    path = Path('tests/data/hpso_spec.json')
    return path


def test_obs_list_length_from_spec(read_hpso_spec):
    obslist = hto.process_hpso_from_spec(read_hpso_spec)
    assert len(obslist) == 5

def test_obs_list_hpso_attributes(read_hpso_spec):
    obslist = hto.process_hpso_from_spec(read_hpso_spec)
    o = obslist[0]
    assert o.name == 'hpso01_0'
    assert o.duration == 18000
    assert o.demand == 512
    o = obslist[2]
    assert o.name == 'hpso01_2'
    assert o.demand == 256
    assert o.baseline == LOWBaselines.short.name

