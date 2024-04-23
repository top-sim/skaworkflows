# Copyright (C) 2024/02/25  RW Bunney

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

import skaworkflows.workflow.hpso_to_observation as h2o

from pathlib import Path

"""
This module provides methods for producing an observation plan based on an input pool of observations. The pool is built from the Observation plan JSON specification:

..code-block: json
    {
        "items": [
            {
                "count": 2,
                "hpso": "hpso01",
                "demand": 512,
                "duration": 18000,
                "workflows": [
                    "DPrepA"
                ],
                "channels": 256,
                "baseline": "long"
            },
        ]
    }

This produces the follow observation plan, which is included in the TopSim simulation configuration: 

..code-block: json
      "observations": [
        {
          "name": "hpso01_0",
          "start": 0,
          "duration": 18000,
          "instrument_demand": 512,
          "type": "hpso02b",
          "data_product_rate": 459024629760.0
        },
        {
          "name": "hpso01_1",
          "start": 18000,
          "duration": 18000,
          "instrument_demand": 512,
          "type": "hpso01",
          "data_product_rate": 459024629760.0
        },
    ]

"""

def generate_observation_plan(path: Path):
    observations = h2o.process_hpso_from_spec(path)
    return h2o.create_observation_plan(observations,512) 


def generate_observation_plan_from_pool(path: Path, duration: int=86400):
    plan = generate_observation_plan(path)
    curr_plan_length = calculate_observation_plan_duration(plan) 
    if(curr_plan_length < duration):
        pool_observations = set(plan) 

    

def calculate_observation_plan_duration(plan):
    plan = sorted(plan, key=lambda obs: obs.start)
    return plan[-1].start + plan[-1].duration


if __name__ == '__main__':
    plan = generate_observation_plan_from_pool(Path("skaworkflows/observation/test_spec.json"))
    print(f"{plan=}, {len(plan)=}")
    print(calculate_observation_plan_duration(plan)/3600)