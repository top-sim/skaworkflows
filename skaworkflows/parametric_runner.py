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


import math
import random
import time
from pathlib import Path

from sdp_par_model import reports, config
from sdp_par_model.config import PipelineConfig
from sdp_par_model.scheduling import graph, level_trace, scheduler
from sdp_par_model.parameters import definitions
from sdp_par_model.parameters.definitions import Telescopes, Pipelines, \
    Constants, HPSOs
from sdp_par_model import config

telescope = Telescopes.SKA1_Mid

# Assumptions about throughput per size for hot and cold buffer
hot_rate_per_size = 3 * Constants.giga / 10 / Constants.tera  # 3 GB/s per 10 TB [NVMe SSD]
cold_rate_per_size = 0.5 * Constants.giga / 16 / Constants.tera  # 0.5 GB/s per 16 TB [SATA SSD]

# Common system sizing
ingest_rate = 0.46 * Constants.tera  # Byte/s
delivery_rate = int(100 / 8 * Constants.giga)  # Byte/s
lts_rate = delivery_rate

# Costing scenarios to assume
scenario = "low-adjusted"
batch_parallelism = 2


def set_values(scenario):
    if scenario == 'low-cdr':
        telescope = Telescopes.SKA1_Low
        total_flops = int(13.8 * Constants.peta)  # FLOP/s
        input_buffer_size = int((0.5 * 46.0 - 0.6) * Constants.peta)  # Byte
        hot_buffer_size = int(0.5 * 46.0 * Constants.peta)  # Byte
        delivery_buffer_size = int(0.656 * Constants.peta)  # Byte
    elif scenario == 'mid-cdr':
        telescope = Telescopes.SKA1_Mid
        total_flops = int(12.1 * Constants.peta)  # FLOP/s
        input_buffer_size = int((0.5 * 39.0 - 1.103) * Constants.peta)  # Byte
        hot_buffer_size = int(0.5 * 39.0 * Constants.peta)  # Byte
        delivery_buffer_size = int(0.03 * 39.0 * Constants.peta)  # Byte
    elif scenario == 'low-adjusted':
        telescope = Telescopes.SKA1_Low
        total_flops = int(9.623 * Constants.peta)  # FLOP/s
        # input_buffer_size = int(30.0 * Constants.peta) # Byte # 1
        input_buffer_size = int(43.35 * Constants.peta)  # Byte
        # hot_buffer_size = int(17.5 * Constants.peta) # Byte # 1
        hot_buffer_size = int(25.5 * Constants.peta)  # Byte # 2
        # hot_buffer_size = int(27.472 * Constants.peta) # Byte
        delivery_buffer_size = int(0.656 * Constants.peta)  # Byte
    elif scenario == 'mid-adjusted':
        telescope = Telescopes.SKA1_Mid
        total_flops = int(5.9 * Constants.peta)  # FLOP/s
        input_buffer_size = int(48.455 * Constants.peta)  # Byte
        hot_buffer_size = int(40.531 * Constants.peta)  # Byte
        delivery_buffer_size = int(1.103 * Constants.peta)  # Byte
    else:
        assert False, "Unknown costing scenario {}!".format(scenario)

    return (
        telescope, total_flops, input_buffer_size, hot_buffer_size,
        delivery_buffer_size
    )


# csv = reports.read_csv(reports.newest_csv(reports.find_csvs()))
# csv = reports.strip_csv(csv)


def calculate_realtime_flop_requirements(csv, telescope):
    realtime_flops = 0
    realtime_flops_hpso = None

    for hpso in definitions.HPSOs.all_hpsos:
        if definitions.HPSOs.hpso_telescopes[hpso] != telescope:
            continue
        # Sum FLOP rates over involved real-time pipelines
        rt_flops = 0
        for pipeline in definitions.HPSOs.hpso_pipelines[hpso]:
            cfg_name = config.PipelineConfig(
                hpso=hpso,
                pipeline=pipeline
            ).describe()
            flops = int(
                math.ceil(
                    float(
                        reports.lookup_csv(
                            csv, cfg_name, 'Total Compute Requirement')
                    ) * definitions.Constants.peta)
            )
            if pipeline in definitions.Pipelines.realtime:
                rt_flops += flops
        # Dominates?
        if rt_flops > realtime_flops:
            realtime_flops = rt_flops
            realtime_flops_hpso = hpso
    return realtime_flops


def determine_global_batch_flops(total_flops, realtime_flops):
    """
    Given we have total_flops as determined by the scenario,
    and we calculate the maximum realtime_flops in
    calculate_flops_requirements, we can
    Parameters
    ----------
    total_flops
    realtime_flops

    Returns
    -------

    """
    batch_flops = total_flops - realtime_flops
    return batch_flops


def generate_capacities(
        batch_flops, realtime_flops, input_buffer_size, hot_buffer_size,
        delivery_buffer_size
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
        graph.Resources.BatchCompute: batch_flops + realtime_flops,
        graph.Resources.RealtimeCompute: realtime_flops,
        graph.Resources.InputBuffer: input_buffer_size,
        graph.Resources.HotBuffer: hot_buffer_size,
        graph.Resources.OutputBuffer: delivery_buffer_size,
        graph.Resources.IngestRate: ingest_rate,
        graph.Resources.DeliveryRate: delivery_rate,
        graph.Resources.LTSRate: lts_rate
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

    # update_rates(capacities)


# Tsequence = 2 * 3600


def create_fixed_sequence(telescope, hpso, verbose=False):
    Tobs = {}
    hpso_sequence = []
    hpso_sequence.append(hpso)
    tp = PipelineConfig(
        hpso=hpso, pipeline=Pipelines.Ingest
    ).calc_tel_params()
    Tobs[hpso] = tp.Tobs

    # (f"{hpso} has duration {Tobs[hpso]}")
    Tobs_sum = Tobs[hpso]

    return hpso_sequence, Tobs_sum


def generate_nodes_from_sequence(sequence, csv, capacities):
    Tobs_min = 1 * 3600
    batch_parallelism = 1
    t = time.time()
    hpso_sequence = sorted(sequence)
    nodes = graph.hpso_sequence_to_nodes(
        csv, hpso_sequence, capacities, Tobs_min,
        batch_parallelism=batch_parallelism, force_order=True
    )
    return nodes


def calculate_total_offline_flops(csv, scenario, hpso):
    offline_imaging = "ICAL + DPrepA" # Minimum requirement for a batch task
    # offline_pulsar_transient = "PST"
    # offline_pulsar_search = "PSS"

    (
        telescope,
        total_flops,
        input_buffer_size,
        hot_buffer_size,
        delivery_buffer_size
    ) = set_values(scenario)
    realtime_flops = calculate_realtime_flop_requirements(csv, telescope)
    batch_flops = determine_global_batch_flops(total_flops, realtime_flops)
    capacities = generate_capacities(
        batch_flops,
        realtime_flops,
        input_buffer_size,
        hot_buffer_size,
        delivery_buffer_size
    )

    capacities = add_rates(capacities)
    # TODO update create_fixed_sequence to reflect make_hpso_sequence,
    #  such that it picks either a specified duration or the minimum possible
    #  duration of that HPSO
    hpso_sequence, Tobs_sum = create_fixed_sequence(
        telescope, hpso, verbose=True
    )
    nodes = generate_nodes_from_sequence(hpso_sequence, csv, capacities)

    result = {'total_flops': 0, 'time': 0, 'batch_flops': 0, "duration": 0}

    for task in nodes:
        if offline_imaging in task.name:
            tflops = task.time * batch_flops
            # print(f'{task.name=}: {tflops:e=} {task.time=}')
            result['total_flops'] = tflops
            result['time'] = task.time
            result['batch_flops'] = batch_flops
            result['duration'] = Tobs_sum
    return result


def calculate_parametric_runtime_estimates(csv_path, scenario, hpsos):
    csv = reports.read_csv(csv_path)
    results = {}
    for h in hpsos:
        results[h] = calculate_total_offline_flops(csv, scenario, h)

    return results


if __name__ == '__main__':
    random.seed(0)
    LONG_SYSTEM_SIZING = Path(
        "/home/rwb/github/sdp-par-model/2023-03-25_longBaseline_HPSOs.csv"
    )
    hpsos = [HPSOs.hpso13] # , HPSOs.hpso05a, HPSOs.hpso02a]
    scenario = 'mid-adjusted'
    result = calculate_parametric_runtime_estimates(
        LONG_SYSTEM_SIZING, scenario, hpsos
    )
    print(result)
