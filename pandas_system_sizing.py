# Copyright (C) 3/9/20 RW Bunney

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
This is taken directly from the SKA Parametric Model GitHub repository:
https://github.com/ska-telescope/sdp-par-model/blob/master/notebooks/SKA1_System_Sizing.ipynb

All credit must go to Peter Wortmann for his work on the parametric model;
this is just a quick script that takes his notebook code and uses it to spit
out his system sizing analysis into csv file (for ease of repeatability).
"""

import sys
import re
import pandas as pd

# This requires you to have the sdp-par-model code on your machine; append it
# to your path

# TODO MOVE THESE INTO DICTIONARY FOR EASIER ACCESS WITH PANDAS DATAFRAME
KEYS = {
    'hpso': "HPSO", 'time': "Time [%]", 'tobs': " Tobs [h]",
    'ingest': "Ingest [Pflop/s]",
    'rcal': "RCAL [Pflop/s]",
    'fastimg': "FastImg [Pflop/s]",
    'ical': "ICAL [Pflop/s]",
    'dprepa': "DPrepA [Pflop/s]",
    'dprepb': "DPrepB [Pflop/s]",
    'dprepc': "DPrepC [Pflop/s]",
    'dprepd': "DPrepD [Pflop/s]",
    'totalrt': "Total RT [Pflop/s]",
    'totalbatch': "Total Batch [Pflop/s]",
    'total': " Total [Pflop/s"
}
PRODUCTS = [
    "-> Image Spectral Fitting ",
    "-> IFFT",
    "-> Reprojection Predict",
    "-> Reprojection",
    "-> Subtract Image Component ",
    "-> Phase Rotation Predict ",
    "-> Degrid",
    "-> Visibility Weighting ",
    "-> Phase Rotation ",
    "-> Identify Component ",
    "-> Gridding Kernel Update ",
    "-> Subtract Visibility ",
    "-> Source Find ",
    "-> Correct ",
    "-> DFT ",
    "-> Solv",
    "-> Flag ",
    "-> Average",
    "-> Receive",
    "-> Demix",
    "-> FFT",
    "-> Grid ",
    "-> Degridding Kernel Update"
]

# test = df[keys['hpso']]
realtime = ["Ingest", "RCAL", "FastImg"]

df_csv = pd.read_csv(
    "/home/rwb/github/sdp-par-model/data/csv/2019-06-20-2998d59_hpsos.csv",
    index_col=0
)
# df_pipelines = pd.read_csv(
#     "/home/rwb/github/sdp-par-model/data/csv/2019-06-20-2998d59_pipelines.csv"
# )

HPSO_SKA_LOW = [
    'hpso01',
    'hpso02a',
    'hpso02b',
    'hpso04a',
    'hpso05a'
]
HPSO_SKA_MID = [
    'hpso04b',
    'hpso04c',
    'hpso05b',
    'hpso13',
    'hpso14',
    'hpso15',
    'hpso18',
    'hpso22',
    'hpso27and33',
    'hpso32',
    'hpso37a',
    'hpso37b',
    'hpso37c',
    'hpso38a',
    'hpso38b'
]
realtime = ['Ingest', 'RCAL', 'FastImg']

ska_low = df_csv.T[df_csv.T['Telescope'] == "SKA1_Low"].T

hpso_data = [
    "HPSO", 'Stations', "Total Time [s]", "Tobs [h]", "Ingest [Pflop/s]",
    "RCAL [Pflop/s]",
    "FastImg [Pflop/s]", "ICAL [Pflop/s]", "DPrepA [Pflop/s]",
    "DPrepB [Pflop/s]", "DPrepC [Pflop/s]", "DPrepD [Pflop/s]",
    "Total RT [Pflop/s]", "Total Batch [Pflop/s]", "Total [Pflop/s]",
    "Ingest Rate [TB/s]"
]
hpso_dict = {header:[] for header in hpso_data}
df_hpso_low = pd.DataFrame(data=hpso_dict)


def translate_sdp_hpso_reports_to_dataframe(csv_path):
    """
    From the output produced by SDP system sizing reports, produce a dataframe
    that collates necessary information.

    Parameters
    ----------
    csv_path : str
        The path to the sdp-par-model report (ensure it is the "hpso" report)

    We are mimicking the results of the SKA1_System_Sizing notebook in the
    sdp-par-model; the intention is, due to our use of Pandas in our analysis
    and pipeline construction, to use it to generate our intermediary data
    selection.

    Additionally, it is useful to remove ourselves from the functions that
    are used in the notebook, as they hide a lot of functionality. Pandas
    makes the data selection and compiling much more explicit, and is (
    hopefully) more accessible to those accessing this codebase.

    Returns
    -------
    final_data_frame : pd.DataFrame object
        A collation of HPSOs, split between Both SKA_LOW and SKA_Mid telescope
    """
    df_csv = pd.read_csv(
        "/home/rwb/github/sdp-par-model/data/csv/2019-06-20-2998d59_hpsos.csv",
        index_col=0
    )


for i, hpso in enumerate(HPSO_SKA_LOW):
    stations = None
    flops = []
    t_exp = 0
    t_obs = 0
    ingest_rate = 0
    df_hpso_low.loc[i, ['HPSO']] = hpso
    rt_flop_total = 0
    rflop_total = 0
    for x in list(ska_low.columns):
        if hpso in x:
            if 'Ingest' in x:
                stations = int(ska_low.loc[['Stations/antennas'], x])
                df_hpso_low.loc[i,['Stations']] = stations
                t_obs = int(ska_low.loc[['Observation Time [s]'], x])
                df_hpso_low.loc[i, "Tobs [h]"] = t_obs / 3600
                t_exp = int(ska_low.loc[['Total Time [s]'], x])
                df_hpso_low.loc[i, "Total Time [s]"] = t_exp
                ingest_rate = float(
                    ska_low.loc[['Total buffer ingest rate [TeraBytes/s]'], x]
                )
                df_hpso_low.loc[i, "Ingest Rate [TB/s]"] = ingest_rate
            compute = float(
                ska_low.loc[['Total Compute Requirement [PetaFLOP/s]'], x]
            )
            pipeline_name = f"{x.strip(hpso).strip('[]').strip('( )')}"
            if pipeline_name in realtime:
                rt_flop_total += compute
            rflop_total += compute
            col_str = f"{pipeline_name} [Pflop/s]"
            df_hpso_low.loc[i, col_str] = compute
        df_hpso_low.loc[i, "Total RT [Pflop/s]"] = rt_flop_total
        df_hpso_low.loc[i, "Total Batch [Pflop/s]"] = rflop_total - rt_flop_total
        df_hpso_low.loc[i, "Total [Pflop/s"] = rflop_total
        # flops.append((x, compute))

ska_mid = df_csv.T[df_csv.T['Telescope'] == "SKA1_Mid"].T


def make_compute_table(tel):
    csv_data = [[
        "HPSO", 'Stations', "Time [%]", "Tobs [h]", "Ingest [Pflop/s]", "RCAL ["
                                                                        "Pflop/s]",
        "FastImg [Pflop/s]", "ICAL [Pflop/s]", "DPrepA [Pflop/s]",
        "DPrepB [Pflop/s]", "DPrepC [Pflop/s]", "DPrepD [Pflop/s]",
        "Total RT [Pflop/s]", "Total Batch [Pflop/s]", "Total [Pflop/s]",
        "Ingest Rate [TB/s]"
    ]]
    # flop_sum = {pip: 0 for pip in Pipelines.available_pipelines}
    # pips = [Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
    #         Pipelines.ICAL, Pipelines.DPrepA, Pipelines.DPrepB,
    #         Pipelines.DPrepC, Pipelines.DPrepD, ]
    # for hpso in sorted(HPSOs.all_hpsos):
    #     if HPSOs.hpso_telescopes[hpso] != tel:
    #         continue
    #     Stations = lookup('Stations/antennas', hpso).get(Pipelines.Ingest, 0)
    #     Tobs = lookup('Observation Time', hpso).get(Pipelines.Ingest, 0)
    #     Texp = lookup('Total Time', hpso).get(Pipelines.Ingest, 0)
    #     flops = lookup('Total Compute Requirement', hpso)
    #     ingest_rate = lookup('Total buffer ingest rate', hpso).get(
    #         Pipelines.Ingest
    #     )
    #
    #     Rflop = sum(flops.values())
    #     Rflop_rt = sum([Rflop for pip, Rflop in flops.items() if
    #                     pip in Pipelines.realtime])
    #     time_frac = Texp / total_time[tel]
    #     for pip, rate in flops.items():
    #         flop_sum[pip] += time_frac * rate
    #     flops_strs = ["{:.2f}".format(flops[pip]) if pip in flops else '-' for
    #                   pip in pips]
    #
    #     csv_data.append([hpso, Stations, time_frac * 100, Tobs / 3600,
    #                      *flops_strs,
    #                      Rflop_rt,
    #                      Rflop - Rflop_rt, Rflop, ingest_rate])
    return csv_data


def make_pipeline_csv(tel):
    # pipeline_csv = [[]]
    # hpso_pipeline_products = {}
    # pips = [Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
    #         Pipelines.ICAL, Pipelines.DPrepA, Pipelines.DPrepB,
    #         Pipelines.DPrepC, Pipelines.DPrepD, ]
    # for hpso in sorted(HPSOs.all_hpsos):
    #     pipeline_products = {}
    #
    #     if HPSOs.hpso_telescopes[hpso] != tel:
    #         continue
    #     for product in PRODUCTS:
    #         tmp = lookup(product, hpso)
    #         product_name = product.lstrip('-> ')
    #         pipeline_products[product_name] = tmp
    #
    #     hpso_pipeline_products[hpso] = pipeline_products
    #
    # # Construct a dataframe
    gldf = pd.DataFrame()
    # for hpso in hpso_pipeline_products:
    #     df = pd.DataFrame(hpso_pipeline_products[hpso])
    #     newcol = [hpso for x in range(0, len(df))]
    #     df.insert(0,'hpso',newcol)
    #     gldf = gldf.append(df)

    return gldf.to_csv('csv/{}_PipelineProducts.csv'.format(tel))

# if __name__ == '__main__':
#
#     tables = {}
#     for tel in Telescopes.available_teles:
#         csvdata = make_compute_table(tel)
#         filename = 'csv/{}_COMPUTE.csv'.format(tel)
#         with open(filename, 'w', newline='') as csvfile:
#             for row in csvdata:
#                 writer = seesv.writer(csvfile, delimiter=',')
#                 writer.writerow(row)
#
#     data = [
#         [
#             "Telescope", "Pipeline", "Data Rate [Gbit/s]",
#             "Daily Growth [TB/day]",
#             "Yearly Growth [PB/year]", "5-year Growth [PB/(5 year)]"
#         ]
#     ]
#
#     for tel in Telescopes.available_teles:
#          make_pipeline_csv(tel)
#
#
#     total_data_rate = 0
#     for tel in Telescopes.available_teles:
#         def mk_projection(rate):
#             day_rate = rate * 3600 * 24 / 8 / 1000  # TB/day
#             year_rate = day_rate * 365 / 1000  # PB/year
#             return [rate, day_rate, year_rate, 5 * year_rate]
#
#
#         subtotal_data_rate = 0
#         for pip in Pipelines.all:
#             data_rate = 0
#             for hpso in HPSOs.all_hpsos:
#                 if HPSOs.hpso_telescopes[hpso] == tel and pip in \
#                         HPSOs.hpso_pipelines[hpso]:
#                     Texp = lookup('Total Time', hpso).get(Pipelines.Ingest, 0)
#                     Tobs = lookup('Observation Time', hpso).get(Pipelines.Ingest, 0)
#                     Mout = lookup('Output size', hpso).get(pip)
#                     Rout = 8000 * Mout / Tobs
#                     time_frac = Texp / total_time[tel]
#                     data_rate += Rout * time_frac
#             if data_rate > 0:
#                 tmp = [tel, pip] + (mk_projection(data_rate))
#                 data.append(tmp)
#                 subtotal_data_rate += data_rate
#                 total_data_rate += data_rate
#         row = [tel] + mk_projection(subtotal_data_rate)
#         data.append(row)
#         filename = 'csv/{}_DATA.csv'.format(tel)
#         with open(filename, 'w', newline='') as csvfile:
#             for row in data:
#                 writer = seesv.writer(csvfile, delimiter=',')
#                 writer.writerow(row)
