# Copyright (C) 10/2/20 RW Bunney

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
import subprocess
import os
import json
import random
import logging

import networkx as nx


def update_number_of_channels(lgt_path, channels):
    """
    Given an EAGLE LGT, update the number of channels

    Parameters
    ----------
    lgt_path
    channels

    The EAGLE graph is a JSON structure we can read in as a python dictionary.
    We use 'nodeDataArray' key, find the 'scatter' element in the list stored
    there, and edit the 'copies' field.

    'nodeDataArray'

    Returns
    -------

    """


def unroll_logical_graph(input_lgt_path, output_pgt_path):
    """
    Take an EAGLE LGT and produce a PGT Using the DALIUGE graph translator

    Parameters
    ----------
    input_lgt_path : str
        The path of the LGT
    output_pgt_path : str
        The output path where the translated JSON will be stored.

    Returns
    -------
    The path that is produced by they
    """

    cmd_list = ['dlg', 'unroll', '-fv', '-L', input_lgt_path]
    with open(format(output_pgt_path), 'w+') as f:
        subprocess.run(cmd_list, stdout=f)
    return output_pgt_path


def generate_dot_from_networkx_graph(nx_graph, output_path):
    """
    Given an networkx graph, produce a Graphviz 'dot'
    Parameters
    ----------
    nx_graph : nx.DiGraph
        NetworkX Directed Graph object
    output_path : str
        The output file path for the .dot file.

    Returns
    -------
    Return value of `nx.drawing.nx_pydot.write_dot()`
    """
    return nx.drawing.nx_pydot.write_dot(nx_graph, output_path)


def json_to_topsim(daliuge_json, output_file):
    """
    Daliuge import will use
    :return: The NetworkX graph for visualisation purposed;
    The path of the output file; None if the process fails
    """
    # Process DALiuGE JSON graph
    unrolled_nx = _daliuge_to_nx(daliuge_json)

    # Convering DALiuGE nodes to readable nodes

    jgraph = {
        "header": {
            "time": False,
        },
        'graph': nx.readwrite.node_link_data(unrolled_nx)
    }

    with open("{0}".format(output_file), 'w') as jfile:
        json.dump(jgraph, jfile, indent=2)

    return output_file


def _add_generated_values_to_graph(
        nxgraph,
        mean,
        uniform_range,
        ccr,
        multiplier,
        node_identifier,
        data_intensive=False
):
    """
    Produces a new graph that converts the DALiuGE Node labels into easier-to-read values,
    and adds the generated computation and data values to the nodes and edges respectively.
    :param nxgraph: The NetworkX DiGraph that is with raw DALiuGE node information
    :return: A NetworkX DiGraph
    """
    translation_dict = {}
    for i, node in enumerate(nx.topological_sort(nxgraph)):
        translation_dict[node] = i

    translated_graph = nx.DiGraph()
    for key in translation_dict:
        translated_graph.add_node(translation_dict[key])

    for edge in nxgraph.edges():
        (u, v) = edge
        translated_graph.add_edge(translation_dict[u], translation_dict[v])

    new = [node_identifier + str(node) for node in translated_graph.nodes()]
    mapping = dict(zip(translated_graph, new))
    translated_graph = nx.relabel_nodes(translated_graph, mapping)

    return translated_graph


def _daliuge_to_nx(dlg_json):
    """

    Take a daliuge json file and read it into a NetworkX

    dlg_json: the DALiuGE file we are translating

    Returns
    -------
    unrolled_nx : networkx.DiGraph
        Directed graph representation of Physical Graph Template

    Notes
    -----
    Adapted from code in SHADOW library.
    https://github.com/myxie/shadow


    """
    with open(dlg_json) as f:
        dlg_json_dict = json.load(f)
    unrolled_nx = nx.DiGraph()
    labels = {}
    node_list = []
    edge_list = []
    size = len(dlg_json_dict)
    pop = [x for x in range(0, size)]
    for element in dlg_json_dict:
        if 'app' in element.keys():
            oid = element['oid']
            label = element['nm']
            index = random.randint(0, size - 1)
            val = pop.pop(index)
            size -= 1
            labels[oid] = f"{element['nm']}_{val}"
            # el = (element['oid'], {"label": element['nm']})
            node_list.append(labels[oid])

    for element in dlg_json_dict:
        if 'storage' in element.keys():
            if 'producers' in element and 'consumers' in element:
                for u in element['producers']:
                    for v in element['consumers']:
                        edge_list.append((labels[u], labels[v]))
    unrolled_nx.add_nodes_from(node_list)
    unrolled_nx.add_edges_from(edge_list)
    return unrolled_nx


def produce_final_workflow_structure(lgt_path, pgt_path, channels):
    """
    For a given logical graph template, produce a workflow with the specific
    number of channels and return it as a JSON serialisable dictionary.

    Parameters
    ----------
    lgt_path : str
    The path of the LGT
    channels : int

    Returns
    -------

    """
    wfdict = {}
    return wfdict


logging.basicConfig(level="INFO")
LOGGER = logging.getLogger(__name__)

if __name__ == '__main__':
    LOGGER.info("Generating test data and unrolling")
    res = unroll_logical_graph(
        'tests/data/eagle_lgt_scatter.graph',
        'tests/data/daliuge_pgt_scatter.json'
    )
    LOGGER.info(f"Test PGT generated at: {res}")
