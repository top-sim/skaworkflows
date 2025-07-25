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

"""
This module is used to create baseline runtime estimates to compare SKA workflow
schedule makespans with the SDP Parametric model.

This code is based on the run_schedule.py module in the SDP Parametric model,
found at https://github.com/ska-telescope/sdp-par-model. The module separates
the monolithic file into various helper functions, such that a user can more
easily specify the parameters for the estimates (e.g. by teleopscope and HPSO).
"""

import math
import random
import time
from pathlib import Path

from sdp_par_model import reports
from sdp_par_model.config import PipelineConfig
from sdp_par_model.scheduling import graph
from sdp_par_model.parameters import definitions
from sdp_par_model.parameters.definitions import Telescopes, Pipelines, Constants, HPSOs
from sdp_par_model import config

telescope = Telescopes.SKA1_Low

# Assumptions about throughput per size for hot and cold buffer
# 3 GB/s per 10 TB [NVMe SSD]
hot_rate_per_size = 3 * Constants.giga / 10 / Constants.tera
# 0.5 GB/s per 16 TB [SATA SSD]
cold_rate_per_size = 0.5 * Constants.giga / 16 / Constants.tera

# Common system sizing
ingest_rate = 0.46 * Constants.tera  # Byte/s
delivery_rate = int(100 / 8 * Constants.giga)  # Byte/s
lts_rate = delivery_rate

# Costing scenarios to assume
scenario = "low-adjusted"
batch_parallelism = 2


def set_values(scenario):
    """
    Set the value of the capacities for the total system sizing
    Parameters
    ----------
    scenario : str
        The scenario that is being run (e.g. SKA Low Baseline Adjuments)

    Returns
    -------
    telescope : str
        Telescope name
    total_flops : float
        Total flops provided by sysem
    input_buffer_size : float
        Size of the input buffer (hot buffer in TopSim)
    hot_buffer_size : float
        Size of the hot buffer (cold buffer in TopSim)
    delivery_buffer_size : float
        Size of the delivery buffer
    """

    if scenario == "low-cdr":
        telescope = Telescopes.SKA1_Low
        total_flops = int(13.8 * Constants.peta)  # FLOP/s
        input_buffer_size = int((0.5 * 46.0 - 0.6) * Constants.peta)  # Byte
        hot_buffer_size = int(0.5 * 46.0 * Constants.peta)  # Byte
        delivery_buffer_size = int(0.656 * Constants.peta)  # Byte
    elif scenario == "mid-cdr":
        telescope = Telescopes.SKA1_Mid
        total_flops = int(12.1 * Constants.peta)  # FLOP/s
        input_buffer_size = int((0.5 * 39.0 - 1.103) * Constants.peta)  # Byte
        hot_buffer_size = int(0.5 * 39.0 * Constants.peta)  # Byte
        delivery_buffer_size = int(0.03 * 39.0 * Constants.peta)  # Byte
    elif scenario == "low-adjusted":
        telescope = Telescopes.SKA1_Low
        total_flops = int(9.623 * Constants.peta)  # FLOP/s
        input_buffer_size = int(43.35 * Constants.peta)  # Byte
        hot_buffer_size = int(25.5 * Constants.peta)  # Byte # 2
        delivery_buffer_size = int(0.656 * Constants.peta)  # Byte
    elif scenario == "mid-adjusted":
        telescope = Telescopes.SKA1_Mid
        total_flops = int(5.9 * Constants.peta)  # FLOP/s
        input_buffer_size = int(48.455 * Constants.peta)  # Byte
        hot_buffer_size = int(40.531 * Constants.peta)  # Byte
        delivery_buffer_size = int(1.103 * Constants.peta)  # Byte
    else:
        assert False, "Unknown costing scenario {}!".format(scenario)

    return (
        telescope,
        total_flops,
        input_buffer_size,
        hot_buffer_size,
        delivery_buffer_size,
    )


def calculate_realtime_flop_requirements(system_sizing_path, telescope, pipelines=None):
    """
    Calculates the FLOPs required to support real-time demand for the given
    telescope
    Parameters
    ----------
    system_sizing_path : str
        Path to the System Sizing we are using as a basis for the balues
        System sizing is generated by the

    telescope

    Returns
    -------

    """
    realtime_flops = 0

    for hpso in definitions.HPSOs.all_hpsos:
        if definitions.HPSOs.hpso_telescopes[hpso] != telescope:
            continue
        # Sum FLOP rates over involved real-time pipelines
        rt_flops = 0
        # if not pipelines:
        #     pipelines =
        for pipeline in definitions.HPSOs.hpso_pipelines[hpso]:
            cfg_name = config.PipelineConfig(hpso=hpso, pipeline=pipeline).describe()
            flops = int(
                math.ceil(
                    float(
                        reports.lookup_csv(
                            system_sizing_path,
                            cfg_name,
                            "Total Compute Requirement [PetaFLOP/s]",
                        )
                    )
                    * definitions.Constants.peta
                )
            )
            if pipeline in definitions.Pipelines.realtime:
                rt_flops += flops
        # Dominates?
        if rt_flops > realtime_flops:
            realtime_flops = rt_flops
    return realtime_flops


def determine_global_batch_flops(total_flops, realtime_flops):
    """
    Given we have total_flops as determined by the scenario,
    and we calculate the maximum realtime_flops in
    calculate_flops_requirements, we can use this to determine offline/batch
    FLOPs

    Parameters
    ----------
    total_flops
    realtime_flops

    Returns
    -------
    batch_flops

    """
    batch_flops = total_flops  # - realtime_flops
    return batch_flops


def generate_capacities(
    batch_flops,
    realtime_flops,
    input_buffer_size,
    hot_buffer_size,
    delivery_buffer_size,
):
    """
    Create a dictionary of the capacities defined in `set_values` for a given
    telescope sceneario.

    Use the `graph.Resources` class attributes as strings. 'Benefit' of the
    class appears to be the unit string representations for each attribute

    Parameters
    ----------
    batch_flops
    realtime_flops
    input_buffer_size
    hot_buffer_size
    delivery_buffer_size

    Notes
    ------
    In all software I write in the future, I will never reduce
    units until they are reported to the user. Working with the
    sdp-par-model has taught me this, if nothing else.

    Returns
    -------

    """

    capacities = {
        graph.Resources.Observatory: 1,
        graph.Resources.BatchCompute: batch_flops,  # + realtime_flops,
        graph.Resources.RealtimeCompute: realtime_flops,
        graph.Resources.InputBuffer: input_buffer_size,
        graph.Resources.HotBuffer: hot_buffer_size,
        graph.Resources.OutputBuffer: delivery_buffer_size,
        graph.Resources.IngestRate: ingest_rate,
        graph.Resources.DeliveryRate: delivery_rate,
        graph.Resources.LTSRate: lts_rate,
    }
    return capacities


def add_rates(capacities):
    """
    Based on the global system data capacities, generate the global rates for
    the relevant capacity.

    Parameters
    ----------
    capacities : dict

    Returns
    -------
    capacities : dict
    """
    capacities[graph.Resources.HotBufferRate] = (
        hot_rate_per_size * capacities[graph.Resources.HotBuffer]
    )
    capacities[graph.Resources.InputBufferRate] = (
        cold_rate_per_size * capacities[graph.Resources.InputBuffer]
    )
    capacities[graph.Resources.OutputBufferRate] = (
        cold_rate_per_size * capacities[graph.Resources.OutputBuffer]
    )
    return capacities


def create_fixed_sequence(telescope, hpso, verbose=False):
    t_obs = {}
    hpso_sequence = []
    hpso_sequence.append(hpso)
    tp = PipelineConfig(hpso=hpso, pipeline=Pipelines.Ingest).calc_tel_params()
    t_obs[hpso] = tp.Tobs
    t_obs_sum = t_obs[hpso]

    return hpso_sequence, t_obs_sum


def generate_nodes_from_sequence(sequence, csv, capacities, pipeline_set):
    Tobs_min = 1 * 3600
    batch_parallelism = 1
    t = time.time()
    hpso_sequence = sorted(sequence)
    # pipeline_set = {"ICAL", "DPrepA"}
    nodes = graph.hpso_sequence_to_nodes(
        csv,
        hpso_sequence,
        capacities,
        Tobs_min,
        batch_parallelism=batch_parallelism,
        force_order=True,
        pipeline_set=pipeline_set,
    )
    return nodes


def calculate_total_offline_flops(csv, scenario, hpso, pipeline_set):
    offline_imaging = "ICAL"  # Minimum requirement for a batch task

    (
        telescope,
        total_flops,
        input_buffer_size,
        hot_buffer_size,
        delivery_buffer_size,
    ) = set_values(scenario)
    realtime_flops = calculate_realtime_flop_requirements(csv, telescope)
    batch_flops = determine_global_batch_flops(total_flops, realtime_flops)
    capacities = generate_capacities(
        batch_flops,
        realtime_flops,
        input_buffer_size,
        hot_buffer_size,
        delivery_buffer_size,
    )

    capacities = add_rates(capacities)
    # TODO update create_fixed_sequence to reflect make_hpso_sequence,
    #  such that it picks either a specified duration or the minimum possible
    #  duration of that HPSO
    hpso_sequence, Tobs_sum = create_fixed_sequence(telescope, hpso, verbose=True)
    nodes = generate_nodes_from_sequence(hpso_sequence, csv, capacities, pipeline_set)

    result = {"total_flops": 0, "time": 0, "batch_flops": 0, "duration": 0}

    for task in nodes:
        if offline_imaging in task.name:
            tflops = task.time * batch_flops
            # print(f'{task.name=}: {tflops:e=} {task.time=}')
            result["total_flops"] = tflops
            result["time"] = task.time
            result["batch_flops"] = batch_flops
            result["realtime_flops"] = realtime_flops
            result["duration"] = Tobs_sum
    return result


def calculate_parametric_runtime_estimates(
        csv_path: str, scenario: str, hpsos: list, pipeline_set: list
):
    csv = reports.read_csv(csv_path)
    results = {}
    for h in hpsos:
        results[h] = calculate_total_offline_flops(csv, scenario, h, pipeline_set)

    return results


if __name__ == "__main__":
    random.seed(0)
    LONG_SYSTEM_SIZING = Path(
        "skaworkflows/data/sdp-par-model_output/ParametricOutput_Mid_antenna-197_channels-65536_baseline-150.csv"
    )
    # hpsos = [HPSOs.hpso02a, HPSOs.hpso01, HPSOs.hpso02b]
    hpsos = [HPSOs.hpso13] # , HPSOs.hpso15]
    scenario = "mid-adjusted"
    result = calculate_parametric_runtime_estimates(
        LONG_SYSTEM_SIZING, scenario, hpsos, ["ICAL", "DPrepA", "DPrepB", "DPrepC"]
    )
    print(result)
    for hpso in result:
        print(
            hpso,
            result[hpso]["total_flops"] / (result[hpso]["batch_flops"]),
        )
