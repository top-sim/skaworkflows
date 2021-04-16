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
import argparse

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
    "-> Image Spectral Fitting",
    "-> IFFT",
    "-> Reprojection Predict",
    "-> Reprojection",
    "-> Subtract Image Component",
    "-> Phase Rotation Predict",
    "-> Degrid",
    "-> Visibility Weighting",
    "-> Phase Rotation",
    "-> Identify Component",
    "-> Gridding Kernel Update",
    "-> Subtract Visibility",
    "-> Source Find",
    "-> Correct",
    "-> DFT",
    "-> Solve",
    "-> Flag",
    "-> Average",
    "-> Receive",
    "-> Demix",
    "-> FFT",
    "-> Grid",
    "-> Degridding Kernel Update"
]

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

PIPELINES = [
    "Ingest",
    "RCAL",
    "FastImg",
    "ICAL",
    "DPrepA",
    "DPrepB",
    "DPrepC",
    "DPrepD",
]
TELESCOPE_IDS = ["SKA1_Low", "SKA1_Mid"]

SKA_HPSOS = {
    "SKA1_Low": HPSO_SKA_LOW,
    "SKA1_Mid": HPSO_SKA_MID
}

REALTIME = ['Ingest', 'RCAL', 'FastImg']


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
    final_dicts : dictionary
        A collation of HPSOs, split between Both SKA_LOW and SKA_Mid telescope
    """
    df_csv = pd.read_csv(
        csv_path,
        index_col=0
    )
    hpso_data = [
        "HPSO", 'Stations', "Total Time [s]", "Tobs [h]", "Ingest [Pflop/s]",
        "RCAL [Pflop/s]",
        "FastImg [Pflop/s]", "ICAL [Pflop/s]", "DPrepA [Pflop/s]",
        "DPrepB [Pflop/s]", "DPrepC [Pflop/s]", "DPrepD [Pflop/s]",
        "Total RT [Pflop/s]", "Total Batch [Pflop/s]", "Total [Pflop/s]",
        "Ingest Rate [TB/s]"
    ]
    # hpso_dict = {header: [] for header in hpso_data}
    # df_hpso = pd.DataFrame(data=hpso_dict)
    final_dict = {telescope: None for telescope in TELESCOPE_IDS}
    retval = None
    for telescope in TELESCOPE_IDS:
        df_ska = df_csv.T[df_csv.T['Telescope'] == telescope].T
        hpso_dict = {header: [] for header in hpso_data}
        df_hpso = pd.DataFrame(data=hpso_dict)
        for i, hpso in enumerate(SKA_HPSOS[telescope]):
            df_hpso.loc[i, ['HPSO']] = hpso
            rt_flop_total = 0
            rflop_total = 0
            for x in list(df_ska.columns):
                if hpso in x:
                    # These variables are the same for all pipelines,
                    # so we just take the information from the ingest pipeline
                    if 'Ingest' in x:
                        stations = int(df_ska.loc[['Stations/antennas'], x])
                        df_hpso.loc[i, ['Stations']] = stations
                        t_obs = int(df_ska.loc[['Observation Time [s]'], x])
                        df_hpso.loc[i, "Tobs [h]"] = t_obs / 3600
                        t_exp = int(df_ska.loc[['Total Time [s]'], x])
                        df_hpso.loc[i, "Total Time [s]"] = t_exp
                        ingest_rate = df_ska.loc[
                            ['Total buffer ingest rate [TeraBytes/s]'], x
                        ]
                        df_hpso.loc[i, "Ingest Rate [TB/s]"] = float(
                            ingest_rate
                        )
                    compute = df_ska.loc[
                        ['Total Compute Requirement [PetaFLOP/s]'], x
                    ]
                    compute = float(compute)
                    # Realtime computing is all the 'Ingest' compute pipelines
                    pipeline_name = f"{x.strip(hpso).strip('[]').strip('( )')}"
                    if pipeline_name in REALTIME:
                        rt_flop_total += compute

                    # Total 'real' flops for the entire sequence of pipelines
                    rflop_total += compute
                    col_str = f"{pipeline_name} [Pflop/s]"
                    df_hpso.loc[i, col_str] = compute
                df_hpso.loc[i, "Total RT [Pflop/s]"] = rt_flop_total
                # Batch processing is simply the difference between realtime
                # pipelines flops and all pipelines
                df_hpso.loc[
                    i, "Total Batch [Pflop/s]"
                ] = rflop_total - rt_flop_total
                df_hpso.loc[i, "Total [Pflop/s"] = rflop_total

        final_dict[telescope] = df_hpso

    return final_dict


def _mk_projection(rate):
    day_rate = rate * 3600 * 24 / 8 / 1000  # TB/day
    year_rate = day_rate * 365 / 1000  # PB/year
    return [rate, day_rate, year_rate, 5 * year_rate]


def translate_sdp_data_reports_do_dataframe(csv_path):
    """
    Produce a Pandas DataFrame of data product calculations that replicate
    the approach taken in the SDP parametric model

    The intention here is to (again) reduce the reliance on helper methods in
    the sdp-par-model repository. We use standard pandas functions to
    interrogate the data and copy it into dataframes. This means the output
    format may be converted to any pandas-viable format

    Parameters
    ----------
    csv_path

    Returns
    final_data : dict
        dictionary with Pandas data frames for each telescope
    -------
    """
    final_data = {telescope: None for telescope in TELESCOPE_IDS}
    data = [[
        "Telescope", "Pipeline",
        "Data Rate [Gbit/s]",
        "Daily Growth [TB/day]",
        "Yearly Growth [PB/year]", "5-year Growth [PB/(5 year)]"
    ]]

    df_csv = pd.read_csv(
        csv_path,
        index_col=0
    )

    for tel in TELESCOPE_IDS:
        total_data_rate = 0
        df_ska = df_csv.T[df_csv.T['Telescope'] == tel].T
        for i, hpso in enumerate(SKA_HPSOS[tel]):
            subtotal_data_rate = 0
            for pip in Pipelines.all:
                data_rate = 0
                for hpso in HPSOs.all_hpsos:
                    if HPSOs.hpso_telescopes[hpso] == tel and pip in \
                            HPSOs.hpso_pipelines[hpso]:
                        Texp = lookup('Total Time', hpso).get(Pipelines.Ingest,
                                                              0)
                        Tobs = lookup('Observation Time', hpso).get(
                            Pipelines.Ingest, 0)
                        Mout = lookup('Output size', hpso).get(pip)
                        Rout = 8000 * Mout / Tobs
                        time_frac = Texp / total_time[tel]
                        data_rate += Rout * time_frac
                if data_rate > 0:
                    tmp = [tel, pip] + (_mk_projection(data_rate))
                    data.append(tmp)
                    subtotal_data_rate += data_rate
                    total_data_rate += data_rate
            row = [tel] + _mk_projection(subtotal_data_rate)
            data.append(row)
            filename = 'csv/{}_DATA.csv'.format(tel)
            with open(filename, 'w', newline='') as csvfile:
                for row in data:
                    writer = seesv.writer(csvfile, delimiter=',')
                    writer.writerow(row)

    return final_data


def make_pipeline_csv(csv_path):
    df_csv = pd.read_csv(
        csv_path,
        index_col=0
    )

    final_dict = {}
    for telescope in TELESCOPE_IDS:
        df_ska = (df_csv.T[df_csv.T['Telescope'] == telescope].T).fillna(-1)
        pipeline_df = pd.DataFrame()
        for hpso in sorted(SKA_HPSOS[telescope]):
            pipeline_products = {}
            for pipeline in PIPELINES:
                pipeline_overview ={}
                if f'{hpso} ({pipeline}) []' in df_ska.columns:
                    column = df_ska[f'{hpso} ({pipeline}) []']
                else:
                    continue
                for product in PRODUCTS:
                    entry = column.loc[[f'{product} [PetaFLOP/s]']]
                    if entry[0] is ' ' or '':
                        pipeline_overview[product] = -1
                    else:
                        pipeline_overview[product] = float(entry)
                pipeline_products[pipeline] = pipeline_overview

            df = pd.DataFrame(pipeline_products).T
            newcol = [hpso for x in range(0, len(df))]
            df.insert(0, 'hpso', newcol)
            pipeline_df = pipeline_df.append(df)
        final_dict[telescope] = pipeline_df

    return final_dict


if __name__ == '__main__':
    path = (f"/home/rwb/github/sdp-par-model/"
            + f"data/csv/2019-06-20-2998d59_hpsos.csv")
    #
    # df_dict = translate_sdp_hpso_reports_to_dataframe(
    #     path
    # )
    #
    # tel = TELESCOPE_IDS[0]
    # df_dict[tel].to_csv(f'csv/{tel}_compute_pandas.csv')

    pipeline = make_pipeline_csv(path)
    tel = TELESCOPE_IDS[0]
    pipeline[tel].to_csv(f'csv/{tel}_hpso_pandas.csv')
