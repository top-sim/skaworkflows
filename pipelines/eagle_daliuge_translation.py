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
import pydot
import os
import json
import random
import logging

import networkx as nx


def update_number_of_channels(lgt_path, channels):
    """
    Update the number of channels in an EAGLE graph

    The EAGLE graph is a JSON structure we can read in as a python dictionary.
    We use 'nodeDataArray' key, find the 'scatter' element in the list stored
    there, and edit the 'copies' field.


    Parameters
    ----------
    lgt_path
    channels

    Notes
    -----
    This is upsettingly hacky, as any bespoke/post-hoc alteration of the
    EAGLE LGT structure has to be edited locally by reading and manipulating
    the raw JSON data.


    Returns
    -------
    Dictionary with updated channel values
    """

    node_data_key = 'nodeDataArray'
    with open(lgt_path, 'r') as f:
        lgt_dict = json.load(f)

    # Finds scatter & gather category
    for node in lgt_dict[node_data_key]:
        if (
                node['category'] == 'Scatter'
                and node['text'] == 'FrequencySplit'
        ):
            for field in node['fields']:
                if field['name'] == 'num_of_copies':
                    field['value'] = channels
        elif (
                node['category'] == 'Gather'
        ):
            for field in node['fields']:
                if field['name'] == 'num_of_inputs':
                    field['value'] = channels

    return lgt_dict


def unroll_logical_graph(input_lgt, output_pgt_path=None, file_in=True):
    """
    Take an EAGLE LGT and produce a PGT Using the DALIUGE graph translator

    Parameters
    ----------
    file_in
    input_lgt: str
        A string containing either the path to a LGT file,
        OR a JSON-encodable string of an LGT
    output_pgt_path : str (optional)
        The output path where the translated JSON will be stored.

    Notes
    -----
    If no `output_pgt_path` is provided, the result of running the DALiuGE
    translator is saved to stderr and stdout. stdout is where the
    JSON-compatible is output when running `dlg unroll ...`, so we can save
    it to result.stdout and then use `json.loads(str)` instead of `json.load(
    f)`.

    This approach is useful if bundlng the unrolling in a pipeline and we
    want to save read/writes on disk by reducing the number of intermediary
    files on disk.

    The same is true for the `file_in` parameter; when programmatically
    changing an LGT to increase the scatter size, for example, it may be
    cumbersome to store the data in a second file, then clean up after.
    Therefore, we can pass in the string and then pretend it is a file using
    /dev/stdin.

    Returns
    -------
    restuls.stdout
        String representation of a JSON encodable dictionary
    """

    cmd_list = ['dlg', 'unroll', '-fv', '-L', input_lgt]

    jdict = ''
    if file_in:
        pass
    else:
        jdict = input_lgt

    if output_pgt_path and file_in:
        with open(output_pgt_path, 'w+') as f:
            result = subprocess.run(cmd_list, stdout=f)
        return result
    elif not file_in:
        result = subprocess.run(
            ['dlg', 'unroll', '-L', '/dev/stdin'],
            input=json.dumps(jdict), capture_output=True, text=True
        )
        return result.stdout
    else:
        result = subprocess.run(['dlg', 'unroll', '-L', f'{input_lgt}'],
                                capture_output=True, text=True)
        return result.stdout


def generate_graphic_from_networkx_graph(nx_graph, output_path):
    """
    Given an networkx graph, produce a Graphviz 'dot'
    Parameters
    ----------
    nx_graph : nx.DiGraph
        NetworkX Directed Grsph object
    output_path : str
        The output file path for the .dot file.

    Returns
    -------
    Return value of `nx.drawing.nx_pydot.write_dot()`
    """
    graph_dot = nx.drawing.nx_pydot.to_pydot(nx_graph)
    graph_dot.write_ps(output_path)
    # cmd_list = ['dot', 'unroll', '-fv', '-L', ]


def eagle_to_topsim(eagle_graph, generate_dot=False):
    """
    Produce a JSON-compatible configuration file for a topsim graph

    Notes
    -----
    We default to `"time":False` here as we will be using FLOPs and Bytes
    values when creating the pipeline characterisations. If `"time": True` was
    the case, then the values would be assumed as time units rather than
    compute units.

    Returns
    -------
    jgraph : dict
        JSON-compatible dictionary

    """

    # Process DALiuGE JSON graph\
    if not os.path.exists(eagle_graph):
        raise FileExistsError(f'{eagle_graph} does not exist')

    LOGGER.info(f"Preparing {eagle_graph} for LGT->PGT Translation")
    daliuge_json = unroll_logical_graph(eagle_graph)
    jdict = json.loads(daliuge_json)
    unrolled_nx = _daliuge_to_nx(jdict)

    # Convering DALiuGE nodes to readable nodes
    LOGGER.info(f"Graph converted to TopSim-compliant data")

    jgraph = {
        "header": {
            "time": False,
        },
        'graph': nx.readwrite.node_link_data(unrolled_nx)
    }

    return jgraph


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


def _daliuge_to_nx(dlg_json_dict):
    """

    Take a daliuge json file and read it into a NetworkX
    The purpose of this is to re-organise nodes in the

    dlg_json_dict: the DALiuGE dictionary we are translating

    Returns
    -------
    unrolled_nx : networkx.DiGraph
        Directed graph representation of Physical Graph Template

    Notes
    -----
    Adapted from code in SHADOW library.
    https://github.com/myxie/shadow


    """
    unrolled_nx = nx.DiGraph()
    labels = {}
    node_list = []
    edge_list = []
    # store task names and counts
    task_names = {}

    for element in dlg_json_dict:
        if 'app' in element.keys():
            oid = element['oid']
            label = element['nm']
            if label in task_names:
                task_names[label] += 1
            else:
                task_names[label] = 1
            labels[oid] = f"{label}_{task_names[label]}"
            node = (f'{label}_{task_names[label]}', {'comp': 0})
            node_list.append(node)

    for element in dlg_json_dict:
        if 'storage' in element.keys():
            if 'producers' in element and 'consumers' in element:
                for u in element['producers']:
                    for v in element['consumers']:
                        edge = (labels[u], labels[v])
                        edge_list.append(
                            (
                                labels[u],
                                labels[v],
                                {
                                    'data_size': 0,
                                    'u': u,
                                    'v': v,
                                    'data_drop_oid': element['oid']
                                }
                            )
                        )

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


logging.basicConfig(level="DEBUG")
LOGGER = logging.getLogger(__name__)

if __name__ == '__main__':
    LOGGER.info("Generating test data and unrolling")
    res = unroll_logical_graph(
        'tests/data/eagle_lgt_scatter_minimal.graph',
        'tests/data/daliuge_pgt_scatter_minimal.json'
    )

    # lgt_object = update_number_of_channels(
    #     'tests/data/eagle_lgt_scatter_minimal.graph', 4
    # )
    # pgt_object = unroll_logical_graph(
    #     lgt_object, file_in=False
    # )

    # TODO UPDATE THE CODE FOR _DALIGUE_TO_NX TO COUNT EACH DIFFERENT KEY
    # This way we can keep track of the number of each task, rather than
    # having random assignments for each
    # USEFUL FOR ERROR CHECKING 
    # LOGGER.info(f"Test PGT generated at: {res}")
    jdict = eagle_to_topsim('tests/data/eagle_lgt_scatter.graph')
    LOGGER.info(json.dumps(jdict, indent=2))
