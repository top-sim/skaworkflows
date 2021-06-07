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

import os
import logging
import pandas as pd

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
    'Image Spectral Fitting',
    'IFFT',
    'Reprojection Predict',
    'Reprojection',
    'Subtract Image Component',
    'Phase Rotation Predict',
    'Degrid',
    'Visibility Weighting',
    'Phase Rotation',
    'Identify Component',
    'Gridding Kernel Update',
    'Subtract Visibility',
    'Source Find',
    'Correct',
    'DFT',
    'Solve',
    'Flag',
    'Average',
    'Receive',
    'Demix',
    'FFT',
    'Grid',
    'Degridding Kernel Update',
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

BASELINE_LENGTHS = {'short': 4062.5, 'mid': 32500, 'long': 65000}


def csv_to_pandas_total_compute(csv_path):
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
                        t_obs = float(df_ska.loc[['Observation Time [s]'], x])
                        df_hpso.loc[i, "Tobs [h]"] = t_obs / 3600
                        t_exp = float(df_ska.loc[['Total Time [s]'], x])
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


def csv_to_pandas_pipeline_components(csv_path, baseline='long'):
    """
    Produce a Pandas DataFrame of data product calculations that replicate
    the approach taken in the SDP parametric model

    The intention here is to (again) reduce the reliance on helper methods in
    the sdp-par-model repository. We use standard pandas functions to
    interrogate the data and copy it into dataframes. This means the output
    format may be converted to any pandas-viable format.

    Parameters
    ----------
    csv_path


    Notes
    -----
    Our system sizing separates telescopes (LOW and MID), so for each 1
    (one) system report (for Short, Mid, and Long baseline lengths) there
    are 2 (two) pandas-compatible reports.

    Returns
    final_data : dict
        dictionary with Pandas data frames for each telescope
    -------
    """

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
                pipeline_overview = {}
                pipeline_data_overview = {}
                max_str = '[]'
                if baseline != 'long':
                    max_str = f'[Bmax={BASELINE_LENGTHS[baseline]}]'
                if f'{hpso} ({pipeline}) {max_str}' in df_ska.columns:
                    column = df_ska[f'{hpso} ({pipeline}) {max_str}']
                else:
                    continue
                for product in PRODUCTS:
                    compute_entry = column.loc[[f'-> {product} [PetaFLOP/s]']]
                    data_entry = column.loc[[f'-> {product} [Tera/s]']]
                    if compute_entry[0] is ' ' or '':
                        pipeline_overview[product] = -1
                    else:
                        pipeline_overview[product] = float(compute_entry)
                    if data_entry[0] is ' ' or '':
                        pipeline_data_overview[product] = -1
                    else:
                        pipeline_data_overview[product] = float(data_entry)
                pipeline_products[pipeline] = pipeline_overview
                pipeline_products[f'{pipeline}_data'] = pipeline_data_overview

            df = pd.DataFrame(pipeline_products).T
            df.index.name = 'Pipeline'
            newcol = [hpso for x in range(0, len(df))]
            df.insert(0, 'hpso', newcol)
            pipeline_df = pipeline_df.append(df, sort=False)
        final_dict[telescope] = pipeline_df

    return final_dict


logging.basicConfig(level="INFO")
LOGGER = logging.getLogger()
if __name__ == '__main__':
    # System sizing for each baseline:
    data_dir = f'data/parametric_model/'
    files = os.listdir(data_dir)
    LOGGER.info(f'Searching {data_dir} for sdp-par-model reports...')
    for baseline in files:
        length = baseline.split("_")[1]
        total_dict = csv_to_pandas_total_compute(
            f'{data_dir}/{baseline}',
        )
        pipe_dict = csv_to_pandas_pipeline_components(
            f'{data_dir}/{baseline}', length
        )
        for telescope in total_dict:
            fn = f'data/pandas_sizing/total_compute_{telescope}_{length}.csv'
            LOGGER.info(f'Writing Total Compute CSV to {fn}')
            total_dict[telescope].to_csv(fn)
            fn = (f'data/pandas_sizing/component_compute_{telescope}_'
                + f'{length}.csv')
            LOGGER.info(f'Writing Pipeline Compute CSV to {fn}')
            pipe_dict[telescope].to_csv(fn)

    LOGGER.info("Final Output data in data/pandas_sizing/:")
    for file in os.listdir('data/pandas_sizing'):
        LOGGER.info(file)
    LOGGER.info("Pandas system sizing translation complete")



