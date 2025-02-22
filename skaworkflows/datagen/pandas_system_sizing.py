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
import sys
import logging
import datetime
import pandas as pd

# logging.basicConfig(level="INFO")
LOGGER = logging.getLogger()

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
    'total': " Total [Pflop/s]"
}

# PRODUCTS are copies of the sdp-par-model product strings; we remove the
# units and the leading '->' for more effective interaction.

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

HPSO_DATA = [
    "HPSO", "Baseline", 'Stations', "Total Time [s]", "Tobs [h]",
    "Ingest [Pflop/s]",
    "RCAL [Pflop/s]",
    "FastImg [Pflop/s]", "ICAL [Pflop/s]", "DPrepA [Pflop/s]",
    "DPrepB [Pflop/s]", "DPrepC [Pflop/s]", "DPrepD [Pflop/s]",
    "Total RT [Pflop/s]", "Total Batch [Pflop/s]", "Total [Pflop/s]",
    "Ingest Rate [TB/s]"
]


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
        index_col=0,
        na_values=['(undefined)']
    )
    # Undefined values are okay to be treated as 0, as we want to add these
    # up for totals.
    df_csv = df_csv.fillna(0)
    # hpso_dict = {header: [] for header in hpso_data}
    # df_hpso = pd.DataFrame(data=hpso_dict)
    if "Low" in csv_path:
        telescope = TELESCOPE_IDS[0]
    else: 
        telescope = TELESCOPE_IDS[1]
    final_dict = {}
    df_tel = df_csv.T[df_csv.T['Telescope'] == telescope].T
    hpso_dict = {header: [] for header in HPSO_DATA}
    df_from_hpso_dict = pd.DataFrame(data=hpso_dict)
    for i, hpso in enumerate(SKA_HPSOS[telescope]):
        LOGGER.info(f'Processing total sizing for {hpso}')
        tmp_dict = {header: 0 for header in HPSO_DATA}
        rflop_total, rt_flop_total, tmp_dict = (
            _isolate_total_sizing(df_tel, tmp_dict, hpso)
        )
        df_from_hpso_dict = df_from_hpso_dict._append(
            tmp_dict, ignore_index=True)
    final_dict[telescope] = df_from_hpso_dict
    return final_dict


def _isolate_total_sizing(df_tel, hpso_dict, hpso):
    """
    Calculate and store the total compute and data requirements for HPSOs,
    based on baseline-dependent system sizing produced by the sdp-par-model.

    Returns
    -------

    """
    hpso_dict['HPSO'] = hpso
    # df_hpso.loc[i, ['Baseline']] = max_baseline
    rt_flop_total = 0
    rflop_total = 0
    for col in [col for col in df_tel.columns if hpso in col]:
        compute = 0
        hpso_dict = _process_common_values(
            df_tel, col, hpso_dict
        )
        compute = df_tel.loc[
            ['Total Compute Requirement [PetaFLOP/s]'], col
        ]
        compute = float(compute)
        # Realtime computing is all the 'Ingest' compute pipelines
        pipeline_name = f"{col.split()[1].strip('()')}"
        if pipeline_name in REALTIME:
            rt_flop_total += compute

        ingest_rate = float(
            df_tel.loc[['Total buffer ingest rate [TeraBytes/s]'], col]
        )
        # Select the maximum ingest rate from our HPSO's columns
        if "Ingest Rate [TB/s]" in hpso_dict:
            hpso_dict["Ingest Rate [TB/s]"] = max(
                ingest_rate, float(hpso_dict["Ingest Rate [TB/s]"])
            )
        else:
            hpso_dict["Ingest Rate [TB/s]"] = float(ingest_rate)
        # Total 'real' flops for the entire sequence of pipelines
        rflop_total += compute
        col_str = f"{pipeline_name} [Pflop/s]"
        hpso_dict[col_str] = compute
        hpso_dict["Total RT [Pflop/s]"] = rt_flop_total
        # Batch processing is simply the difference between realtime
        # pipelines flops and all pipelines
        hpso_dict["Total Batch [Pflop/s]"] = rflop_total - rt_flop_total
        hpso_dict["Total [Pflop/s]"] = rflop_total

    return rflop_total, rt_flop_total, hpso_dict


def _process_common_values(df_tel, col, hpso_dict):
    """

    For each HPSO, there are a few rows that are the same for each column,
    so we can isolate them with ingest and process them here

    Parameters
    ----------
    df_tel : pd.DataFrame
    pos : int
        Position in the DataFrame

    Returns
    -------

    """
    stations = int(df_tel.loc[['Stations/antennas'], col])
    hpso_dict['Stations'] = stations
    baseline = float(df_tel.loc[['Max Baseline [km]'], col]) * 1000
    hpso_dict['Baseline'] = baseline
    channels = float(df_tel.loc[['Max # of channels'], col])
    hpso_dict['Channels'] = channels
    t_obs = float(df_tel.loc[['Observation Time [s]'], col])
    hpso_dict['Tobs [h]'] = t_obs / 3600
    t_exp = float(df_tel.loc[['Total Time [s]'], col])
    hpso_dict['Total Time [s]'] = t_exp
    return hpso_dict


def csv_to_pandas_pipeline_components(csv_path):
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
    # for telescope in TELESCOPE_IDS:
    if "Low" in csv_path: 
        telescope = TELESCOPE_IDS[0]
    else: 
        telescope = TELESCOPE_IDS[1]
    df_ska = df_csv.T[df_csv.T['Telescope'] == telescope].T.fillna(0)
    pipeline_df = pd.DataFrame()
    max_baseline = max((df_ska.loc['Max Baseline [km]']).astype(float))
    for hpso in sorted(SKA_HPSOS[telescope]):
        LOGGER.info(f'Processing component sizing for {hpso}')
        pipeline_products = _isolate_products(df_ska, hpso)
        df = pd.DataFrame(pipeline_products).T
        df.index.name = 'Pipeline'
        newcol = [hpso for x in range(0, len(df))]
        df.insert(0, 'hpso', newcol)
        # basecol = [max_baseline for x in range(0, len(df))]
        # df.insert(1, 'Baseline', basecol)
        pipeline_df = pipeline_df._append(df, sort=False)
    final_dict[telescope] = pipeline_df
    return final_dict


def _isolate_products(df_tel, hpso):
    """
    Generate product system sizing information for each component

    Notes
    -----
    df_tel is expected to be partial dataframe of the original output
    from the sdp-par-model system sizing, generated by the following:

    >>> df_tel = df_csv.T[df_csv.T['Telescope'] == telescope].T.fillna(-1)
    where `df_csv` is the original data frame

                                        hpso04b (Ingest) [Bmax=4062.5] ...
    -> Image Spectral Fitting [Tera/s]               0.004
    ->  Flag [Tera/s]                                4.5
    ...

    Parameters
    ----------
    df_tel : pd.DataFrame
        Data frame storing telescope specific system sizing from sdp-par-model
    Returns
    -------
    """
    pipeline_products = {}

    for pl in [col for col in df_tel.columns if hpso in col]:
        pipeline_overview = {}
        pipeline_data_overview = {}
        column = df_tel[pl]
        splt = pl.split()
        if '[]' in splt:
            splt.remove('[]')
        pipeline = splt[1].strip('()')
        baseline = float(column['Max Baseline [km]']) * 1000
        channels = float(column["Max # of channels"])
        antennas = float(column['Stations/antennas'])
        pipeline_overview['Baseline'] = baseline
        pipeline_data_overview['Baseline'] = baseline
        pipeline_overview['Antenna stations'] = antennas
        pipeline_data_overview['Antenna stations'] = antennas
        pipeline_overview['Channels'] = channels
        pipeline_data_overview['Channels'] = channels
        buffer_read_rate = float(column['Buffer Read Rate [TeraBytes/s]'])
        pipeline_data_overview['Visibility read rate'] = buffer_read_rate
        pipeline_overview['Visibility read rate'] = buffer_read_rate
        for product in PRODUCTS:
            compute_entry = ['']
            data_entry = ['']
            try: 
                compute_entry = column.loc[[f'-> {product} [PetaFLOP/s]']]
                data_entry = column.loc[[f'-> {product} [Mvis/s]']]
            except KeyError:
                LOGGER.info(f"Product: {product} was not located in generated .csv")
        
            if compute_entry[0] == ' ' or compute_entry[0] == '':
                pipeline_overview[product] = -1
            else:
                pipeline_overview[product] = float(compute_entry)

            if data_entry[0] == ' ' or data_entry[0] == '':
                pipeline_data_overview[product] = -1
            else:
                # Each visibility
                pipeline_data_overview[product] = float(data_entry)
            pipeline_products[pipeline] = pipeline_overview
            pipeline_products[
                f'{pipeline}_data'
            ] = pipeline_data_overview

    return pipeline_products


def generate_pandas_system_sizing(
        total=True,
        components=True,
        in_dir='parametric_model',
        out_dir='skaworkflows/data/pandas_sizing'
):
    """

    Parameters
    ----------
    total
    components
    in_dir
    out_dir

    Returns
    -------

    """

    files = os.listdir(in_dir)
    LOGGER.info(f'Searching {dir} for sdp-par-model reports...')
    for baseline in files:
        if '.archive' in baseline:
            continue
        LOGGER.info(f'Processing {baseline} generated by sdp-par-model:')
        length = baseline.split("_")[1]
        if total:

            total_dict = csv_to_pandas_total_compute(
                f'{dir}/{baseline}',
            )
            for telescope in total_dict:
                fn = f'{OUTPUT_DIR}/total_compute_{telescope}_{length}.csv'
                LOGGER.info(f'Writing Total Compute CSV to {fn}')
                total_dict[telescope].to_csv(fn)
        if components:
            pipe_dict = csv_to_pandas_pipeline_components(
                f'{dir}/{baseline}', length
            )
            fn = (f'{OUTPUT_DIR}/component_compute_{telescope}_'
                  + f'{length}.csv')
            LOGGER.info(f'Writing Pipeline Compute CSV to {fn}')
            pipe_dict[telescope].to_csv(fn)


def compile_sizing(data_paths: list, total=True, component=True):
    """
    Given a directory, produce pandas system sizing for each baseline and
    produce a dataframe that contains all baselines

    Parameters
    ----------
    data_paths: list
        List of paths that contain output from the parametric model 
        system sizing
    total : bool
        If False, do not generate total system sizing data frame
    component : bool
        If False, do not generate component system sizingi data frame

    Returns
    -------
    total_sizing, component_sizing : dict
        Python dicitonary for each sizing, for each telescope
    """
    # files = os.listdir(data_dir)
    LOGGER.info(f'Searching {data_paths} for sdp-par-model reports...')
    total_sizing = {}
    component_sizing = {}
    # Modify this from baseline to channels/antennas? Need to add this to CSV Processing

    for path in data_paths:
        print(path)
        if 'archive' in path:
            continue
        LOGGER.info(f'Processing {path} generated by sdp-par-model:')
        length = path.split("_")[1]
        total_dict = csv_to_pandas_total_compute(
            path
        )
        pipe_dict = csv_to_pandas_pipeline_components(
            path
        )
        for tel in total_dict:
            if tel in total_sizing:
                total_sizing[tel] = total_sizing[tel]._append(
                    total_dict[tel]
                )
            else:
                total_sizing[tel] = pd.DataFrame()
                total_sizing[tel] = total_sizing[tel]._append(
                    total_dict[tel]
                )
            if tel in component_sizing:
                component_sizing[tel] = component_sizing[tel]._append(
                    pipe_dict[tel]
                )
            else:
                component_sizing[tel] = pd.DataFrame()
                component_sizing[tel] = component_sizing[tel]._append(
                    pipe_dict[tel]
                )
                # LOGGER.info(")
    return total_sizing, component_sizing

if __name__ == '__main__':

    if len(sys.argv) > 1:
        x=1
    else:
        sys.exit()

    curr_date = str(datetime.date.today())
    # System sizing for each baseline:
    logging.basicConfig(level='INFO')
    
    SKA_Low_antenna = [32, 64, 128, 256, 512]
    SKA_channels = [64, 128, 256, 512]

    SKA_Mid_antenna = [64, 102, 140, 197]

    IN_DIR = 'skaworkflows/data/sdp-par-model_output/'
    OUTPUT_DIR = 'skaworkflows/data/pandas_sizing/'
    # path_name = f"{IN_DIR}/ParametricOutput_Low_antenna-32_channels-512.csv"
    # print(csv_to_pandas_pipeline_components(path_name))
    paths = []
    for a in SKA_Low_antenna:
        for c in SKA_channels: 
            channels = c*128
            paths.append(f"{IN_DIR}/ParametricOutput_Low_antenna-{a}_channels-{channels}.csv")
    for a in SKA_Mid_antenna:
        for c in SKA_channels: 
            channels = c*128
            paths.append(f"{IN_DIR}/ParametricOutput_Mid_antenna-{a}_channels-{channels}.csv")
    total_sizing, component_sizing = compile_sizing(paths)

    for tel in total_sizing:
        fn_total = f'{OUTPUT_DIR}/total_compute_{tel}_{curr_date}.csv'
        LOGGER.info(f'Writing total system sizing for {tel}to {fn_total}')
        total_sizing[tel].to_csv(fn_total)
    for tel in component_sizing:
        fn_component = f'{OUTPUT_DIR}/component_compute_{tel}_{curr_date}.csv'
        LOGGER.info(
            f'Writing component compute sizing for {tel} to {fn_component}'
        )
        component_sizing[tel].to_csv(fn_component)
    
    LOGGER.info(f"Final Output data in {OUTPUT_DIR}/:")
    LOGGER.info("Pandas system sizing translation complete.")
