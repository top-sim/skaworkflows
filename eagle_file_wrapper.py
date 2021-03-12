# Copyright (C) 5/3/21 RW Bunney

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
When we generate an EAGLE Graph, it is more convenient in the editor to focus
on the pipeline components (Gridding, the inner and outer loops etc.),
and then split according to frequency channels in the greater graph.
"""

import networkx as nx

def split_frequency_channels(hpso_pipeline, channels):
    """
    Return a workflow that splits a general pipeline on frequency channels,
    creating parallel work, with each parallel-node containing a smaller
    imaging pipeline loop.

    Parameters
    ----------
    hpso_pipeline : path/str
        The path to the hpso pipeline we are splitting. For example,
        reference to a continuum pipeline that is in .json form.
    channels
        The number of channels that is due to be observed on the pipeline

    Referring to SDP PDR-02-05 (Pipelines Element Subsystem Design), we see
    there are a number of proposed approaches to implementing the proposed
    pipelines for each of the high priority science projectss (HPSOs).

    For example, 7.2.2

    Returns
    -------

    """

head = 'channel_split'
channel_graph = nx.DiGraph()
children = [head + str(i) for i in range(0, CHANNELS)]
channel_graph.add_nodes_from(children)
channel_graph.add_node(head)
channel_graph.add_edges_from(
    [(head, head + str(x)) for x in range(0, CHANNELS)])
for node in channel_graph.nodes():
    channel_graph.nodes[node]['comp'] = 100000
for edge in channel_graph.edges():
    channel_graph.edges[edge]['data_size'] = 0
# glist = []
# for x in range(0,len(children)):
#     ng = nx.DiGraph()
#     ng.add_nodes_from([z for z in range(i, i + len(children))])
#     ng.add_edges_from([(z, z + 1) for z in range(i, i+len(children)-1)])
#     glist.append(ng)
#     i += len(children)

glistgraph = nx.compose_all(graph_list)
final = nx.compose(glistgraph, channel_graph)

for i in range(0, CHANNELS):
    minor_heads = []
    for node in graph_list[i].nodes():
        pred = list(final.predecessors(node))
        if len(pred) == 0:
            minor_heads.append(node)
    for node in minor_heads:
        final.add_edge(head + str(i), node, data_size=0)

nx.drawing.nx_pydot.write_dot(final, 'testgraph.dot')

jgraph = {
    "header": {
        "time": False,
        "gen_specs": {
            'file': "channel_split_continuum.json",
            'mean': MEAN,
            'range': "+-{0}".format(UNIFORM_RANGE),
            'seed': 20,
            'ccr': CCR,
            'multiplier': MULTIPLIER
        },
    },
    'graph': nx.readwrite.node_link_data(final)
}

