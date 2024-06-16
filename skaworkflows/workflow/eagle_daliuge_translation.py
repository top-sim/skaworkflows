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
import math

import networkx as nx

LOGGER = logging.getLogger(__name__)


def update_number_of_channels(lgt_path, channels, telescope_demand=512):
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
    LOGGER.info("Updating coarse channel parallelism to %s", channels)

    # Finds scatter & gather category
    for node in lgt_dict[node_data_key]:
        if (
                node['category'] == 'Scatter'
                and node['name'] == 'FrequencySplit'
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

    LOGGER.info(f"Translating EAGLE graph...")
    if output_pgt_path and file_in:
        with open(output_pgt_path, 'w+') as f:
            result = subprocess.run(cmd_list, stdout=f)
        return result
    elif not file_in:
        result = subprocess.run(
            ['dlg', 'unroll', '-fv','-L', '/dev/stdin'],
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


def eagle_to_nx(eagle_graph, workflow, file_in=True, cached_workflow=None):
    """
    Produce a JSON-compatible dictionary of a topsim graph

    Parameters
    ----------
    eagle_graph : str
        Path directory to the Logical Graph template we are translating

    workflow: str
        The type of workflow (DPrepA, ICAL etc.) that is being generated.
        Necessary for when we concatenate the workflows together.
    file_in : bool
        True if passing a file; False if passing in a string represetnation

    Notes
    -----
    We default to `"time": False` here as we will be using FLOPs and Bytes
    values when creating the pipeline characterisations. If `"time": True` was
    the case, then the values would be assumed as time units rather than
    compute units.

    Returns
    -------
    unrolled_nx : :py:object:`networkx.DiGraph`
    task_dict : dictc
    cached_graph : dict

    """

    # Process DALiuGE JSON graph\
    if file_in:
        if not os.path.exists(eagle_graph):
            raise FileExistsError(f'{eagle_graph} does not exist')

    LOGGER.info(f"Preparing {workflow} for LGT->PGT Translation")
    if cached_workflow is None:
        daliuge_json = unroll_logical_graph(eagle_graph, file_in=file_in)
        LOGGER.info("Finished translating graph")
        jdict = json.loads(daliuge_json)
        # with open(f"unrolled_{file_in}.json", 'w') as fp:
        #     json.dump(jdict, fp, indent=2)
    else:
        LOGGER.info(f"Using cached translation for workflow")
        jdict = cached_workflow

    unrolled_nx, task_dict = daliuge_to_nx(jdict, workflow)

    # Convering DALiuGE nodes to readable nodes
    LOGGER.info(f"Graph converted to TopSim-compliant data")

    return unrolled_nx, task_dict, jdict


def daliuge_to_nx(dlg_json_dict, workflow):
    """

    Take a daliuge json file and read it into a NetworkX
    The purpose of this is to re-organise nodes in the

    Parameters
    -----------
    dlg_json_dict: dict
        the DALiuGE dictionary we are translating
    workflow : str
        What type of workflow (i.e. DPrepA, DPrepB, ICAL) to append to
        the names of components

    Returns
    -------
    unrolled_nx : networkx.DiGraph
        Directed graph representation of Physical Graph Template


    Notes
    -----
    Adapted from code in SHADOW library.
    https://github.com/myxie/shadow


    """
    LOGGER.info("Converting DALiuGE to networkx...")
    unrolled_nx = nx.DiGraph()
    labels = {}
    node_list = []
    edge_list = []
    # store task names and counts
    task_names = {}

    for element in dlg_json_dict:
        if 'categoryType' in element:
            if element['categoryType'] == 'Application' or element['categoryType'] == 'Control':
                oid = element['oid']
                label = element['name']
                if label in task_names:
                    task_names[label]['node'] += 1
                else:
                    task_names[label] = {'node': 1}
                labels[oid] = f"{label}_{task_names[label]['node'] - 1}"
                name = f"{workflow}_{labels[oid]}"
                node = (name, {'comp': 0})
                node_list.append(node)

    for element in dlg_json_dict:
        if 'categoryType' in element:
            if (element['categoryType'] == 'Data') and (
                    'producers' in element and 'consumers' in element
            ):
                for uentry in element['producers']:
                    try:
                        uentry.keys()
                    except AttributeError:
                        u = uentry
                    else:
                        u = list(uentry.keys())[0]
                    component, num = labels[u].split('_')
                    if 'out_edge' in task_names[component]:
                        task_names[component]['out_edge'] += 1
                    else:
                        task_names[component]['out_edge'] = 1
                    for ventry in element['consumers']:
                        try:
                            ventry.keys()
                        except AttributeError:
                            v = ventry
                        else:
                            v = list(ventry.keys())[0]
                        edge_list.append(
                            (
                                f'{workflow}_{labels[u]}',
                                f'{workflow}_{labels[v]}',
                                {
                                    "transfer_data": 0,
                                    'u': u,
                                    'v': v,
                                    'data_drop_oid': element['oid']
                                }
                            )
                        )
    unrolled_nx.add_nodes_from(node_list)
    unrolled_nx.add_edges_from(edge_list)

    return unrolled_nx, task_names


def concatenate_workflows(
        unrolled_graphs: dict = None, workflows: list = None
):
    """
    For a list of workflows and unrolled graphs, generate a concatenated
    'super-graph' that is the sequence of these workflows

    Notes
    -----
    If one looks into how the SDP parametric model works, it takes into account
    all pipelines when generating a schedule for a particular observation.

    Parameters
    ----------
    unrolled_graphs
    workflows

    Returns
    -------

    """
    # final_graph = nx.DiGraph()
    start = workflows[0]
    # start_graph = unrolled_graphs[start]
    curr_parent = list(nx.topological_sort(unrolled_graphs[start]))[-1]
    workflows.remove(start)
    final_graph = nx.compose_all(list(unrolled_graphs.values()))
    for workflow in workflows:
        curr_child = list(nx.topological_sort(unrolled_graphs[workflow]))[0]
        # for i in range(str(final_graph.nodes).count(f"{workflow}_ExtractLSM")):
        #     curr_child = f'{workflow}_ExtractLSM_{i}'
        final_graph.add_edge(curr_parent, curr_child, transfer_data=0)
        curr_parent = list(nx.topological_sort(unrolled_graphs[workflow]))[-1]

    return final_graph

