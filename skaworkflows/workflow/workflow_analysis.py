# Copyright (C) 28/3/22 RW Bunney

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


import json
import pandas as pd
import networkx as nx

from pathlib import Path


def generate_workflow_stats(wf_path):
    """
    For a given workflow, produce user-friendly data on the structure and
    attributes stored in the graph.

    Parameters
    ----------
    wf_path

    Returns
    -------
    overview: dict
        Dictionary of material printed to user
    """

    with open(wf_path) as fp:
        jgraph = json.load(fp)

    graph = nx.readrwite.node_link_graph(jgraph['graph'])


def calculate_total_flops(wf_path):
    """
    For a given workflow produced by `skaworkflows`, calculate the total flops

    Notes
    ------
    To compare against SDP parametric model

    Parameters
    ----------
    wf_path : :py:obj:`pathlib.Path` object

    Returns
    -------

    """

    with open(wf_path) as fp:
        jgraph = json.load(fp)

    total_flops = 0
    graph = nx.readwrite.node_link_graph(jgraph['graph'])
    for node in graph:
        total_flops += graph.nodes[node]['comp']

    return total_flops


def calculate_expected_flops(hpso, workflows, duration, sizing, baseline=65000):
    """
    For a given HPSO spec, we want to calculate
    the expected FLOPS based on the parametric total
    sizing.

    Returns
    -------
    expected_flops : int
        Cumulative sum of all workflows in hpso spec
    """

    expected_flops = 0

    df = pd.read_csv(sizing)

    cols = ['HPSO', 'Baseline'] + [f"{w} [Pflop/s]" for w in workflows]

    df_workflows = df[cols]
    hpso_costs = (
        df_workflows[
            (df_workflows['HPSO'] == hpso)
            & (df_workflows['Baseline'] == baseline)
            ].loc[:, "ICAL [""Pflop/s]":]
    )
    expected_flops = hpso_costs.sum(axis="columns") * duration * (10 ** 15)
    return float(expected_flops)


def generate_sdp_flops(config: Path):
    with config.open() as fp:
        total_config = json.load(fp)
    compute = total_config['cluster']
    total_compute = 0
    for machine in compute['system']['resources']:
        total_compute += compute['system']['resources'][machine]['flops']
    return total_compute



