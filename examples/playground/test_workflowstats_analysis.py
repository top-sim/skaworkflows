# Copyright (C) 20/4/24 RW Bunney

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
Exploratory script for testing utility functions to use with SKAWorkflow outpu
"""

import json
import os
import pandas as pd
from pathlib import Path


def collate_simulation_parameters_to_dataframe():
    """
    Given a simulation parameter file, create a dataframe of components

    Returns
    -------
    df : pandas.DataFrame
    """

    BASE_DIR = Path("examples/playground/config")
    for cfg_path in os.listdir(BASE_DIR):
        if (BASE_DIR / cfg_path).is_dir():
            continue
        with open(BASE_DIR / cfg_path) as fp:
            cfg = json.load(fp)
        pipelines = cfg["instrument"]["telescope"]["pipelines"]
        nodes = len(cfg["cluster"]["system"]["resources"])
        observations = pipelines.keys()
        parameters = (
            pd.DataFrame.from_dict(pipelines, orient="index")
            .reset_index()
            .rename(columns={"index": "observation"})
        )
        parameters["nodes"] = nodes
        parameters["dir"] = BASE_DIR

    return parameters


if __name__ == "__main__":
    BASE_DIR = Path("examples/playground/config")
    for cfg_path in os.listdir(BASE_DIR):
        if (BASE_DIR / cfg_path).is_dir():
            continue
        with open(BASE_DIR / cfg_path) as fp:
            cfg = json.load(fp)
        pipelines = cfg["instrument"]["telescope"]["pipelines"]
        nodes = len(cfg["cluster"]["system"]["resources"])
        observations = pipelines.keys()
        parameters = (
            pd.DataFrame.from_dict(pipelines, orient="index")
            .reset_index()
            .rename(columns={"index": "observation"})
        )
        parameters["nodes"] = nodes
        parameters["dir"] = BASE_DIR

        workflow_stats_path = BASE_DIR / f"{parameters['workflow'].iloc[0]}.csv"

        wf_df = pd.read_csv(workflow_stats_path, index_col=0)
        for c in parameters:
            wf_df[c] = str(parameters[c].values[0])
        wf_df.to_csv(BASE_DIR / 'test_ouput.csv')
