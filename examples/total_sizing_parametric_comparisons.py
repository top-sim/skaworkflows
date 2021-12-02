# Copyright (C) 18/10/21 RW Bunney

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
Class implementation of the total system sizing values used in the
https:github.com/ska-telescope/sdp-par-model.

Use for comparisons and sanity checking of itemised componenents.
"""

import os
import logging

import numpy as np
import pandas as pd

from workflow.common import SI

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

hpconcfig_latex = {
    'telescope': "Telescope",
    'num_nodes': '$|M_{\\mathrm{SDP}}$',
    'gpu_per_node': '$|P_m|$',
    'memory_per_node': '$d$ (GB)',
    'storage_per_node': '$s$ (TB)',
    'flops_per_node': ' $p_{i}^m$ (PFLOPS)(est.)',
    'total_storage': '$\\mathrm{TS}_{\\mathrm{SDP}}$ (PB)',
    'total_processing': '$\\mathrm{TP}_{\\mathrm{SDP}}$ (PFLOPS)'
}

parametric_latex = {
    'telescope': "Telescope",
    'total_flops': "Total Flops (PFLOPS)",
    # This is workflow buffer/offline
    'cold_buffer_size': "Cold Buffer (PBytes)",  # Byte
    # This is ingest buffer
    'hot_buffer_size': "Hot Buffer (PBytes)",  # Byte # 2
    'delivery_buffer_size': "Delivery Buffer (PBytes)",  # Byte
    'hot_rate_total': 'Hot Buffer Rate (TeraBytes/s)',
    'cold_rate_total': 'Cold Buffer Rate'
}


def produce_tex_table():
    """
    For the final
    Returns
    -------
    String representation of data frame information
    """
    df = pd.DataFrame
    return df.to_latex()


class TotalSizingLowCDR:
    """
    CDR system sizing data wrapper

    Use this class when printing and interacting with parametric model data.
    Most commonly used to calculate comparisons with the TopSim models we
    are testing against, and to produce tables based on said data.
    """
    # These are the same foro all system sizing classes
    hot_rate_per_size = 3 * SI.giga / 10 / SI.tera
    cold_rate_per_size = 0.5 * SI.giga / 16 / SI.tera
    # Derived from total_compute system sizing
    ingest_rate = 0.46 * SI.tera  # Byte/s -
    delivery_rate = int(100 / 8 * SI.giga)  # Byte

    def __init__(self):
        self.telescope = 'Low'
        self.total_flops = np.array([9.623, SI.peta])
        self.cold_buffer = np.array([43.35, SI.peta])
        self.hot_buffer = np.array([25.5, SI.peta])
        self.delivery_buffer = np.array([0.656, SI.peta])
        self.hot_rate_total = (
            np.prod(self.hot_buffer) * self.hot_rate_per_size / SI.tera,
            SI.tera
        )
        self.cold_rate_total = (
            np.prod(self.hot_buffer) * self.cold_rate_per_size / SI.tera,
            SI.tera
        )

    def to_df(self, human_readable=True):
        """
        Generate a data frame from class attributes

        This allows construction of larger data-frames from other sizing
        sizing, for comparisons, or for generation of latex-compliant tables.
        Parameters
        ----------
        human_readable

        Returns
        -------

        """
        df = pd.DataFrame()
        if human_readable:
            pass
        else:
            tmp = self.__dict__
            row = {'telescope': tmp['telescope']}
            for element in tmp:
                # Ignore non-tuple elements
                if element == 'telescope':
                    continue
                # Calculate the scale factors based on class tuple
                row[element] = np.prod(tmp[element])
            df = df.append(row, ignore_index=True)
        return df


def format_low_parametric():
    """
    Using the Low Adjusted data produced by SDP parametric model.
    We then want to have the resulting configuration expectation for our
    simulator mdodel (that is, what are the specs for each device going to
    look like).

    Returns
    -------
    df
    """
    # SKALOW_nodes = 896
    # gpu_per_node = 2
    # gpu_peak_flops = 31 * SI.tera  # Double precision
    # memory_per_node = 31 * SI.giga

    df = pd.DataFrame()
    # # These values are derived from 'somewhere'.
    # low_adjusted = {
    #     'total_flops': 9.623 * SI.peta,
    #     # This is workflow buffer/offline
    #     'cold_buffer_size': int(43.35 * SI.peta),  # Byte
    #     # This is ingest buffer
    #     'hot_buffer_size': int(25.5 * SI.peta),  # Byte # 2
    #     'delivery_buffer_size': int(0.656 * SI.peta),  # Byte
    #     'hot_rate_total': hot_rate_per_size *
    # }
    # low_cdr = {
    #     'hot_rate_per_size': hot_rate_per_size,
    #     "total_flops": int(13.8 * SI.peta),  # FLOP/s
    #     "input_buffer_size": int((0.5 * 46.0 - 0.6) * SI.peta),  # Byte
    #     "hot_buffer_size": (0.5 * 46.0 * SI.peta),  # Byte
    #     "delivery_buffer_size": (0.656 * SI.peta),  # Byte
    # }
    #
    return df


def generate_pipeline_component_table_baselines(
        hpso,
        pipeline='DPrepA',
        telescope='Low',
        data_dir='data/pandas_sizing',
        search_prefix='component_compute',

):
    """
    For a given Pipeline, generate the per-component compute value for each
    basline

    data_dir : str
        Directory where system sizing is produced

    search_prefix : str
        common prefix for each separate baseline system sizing

    Notes
    -----
    Per-component data is not generated by the sdp-par-model; this is done
    using our data/pandas_system_sizing.py code.

    The `search_prefix` is provided because the SDP Parametric Model only
    produces sizing for one baseline at a time.

    Returns
    -------
    df : pandas.DataFrame
        Dataframe with the components
    """
    baselines = {'short': 4062.5, 'mid': 32500, 'long': 65000}

    if not os.path.exists(data_dir):
        raise RuntimeError(f'{data_dir} does not exist')
    row_df = pd.DataFrame()
    for f in os.listdir(data_dir):
        if all(x in f for x in [telescope, search_prefix]):
            df = pd.read_csv(f'{data_dir}/{f}')
            row = df[
                (df['Pipeline'] == pipeline)
                & (df['hpso'] == 'hpso01')
                ].drop(['Pipeline', 'hpso'],axis=1)
            col_list = list(row)
            col_list.remove('Baseline')
            total = row[row > 0][col_list].T.sum()
            row['Total'] = total
            # row = row.add_suffix(' (PFLOPS)')
            row = row.replace(-1.0, '--')
            bline = 0

            # TODO This becomes a column name
            # row.insert(0, 'Components', f'Baseline ({bline}m)')
            row_df = row_df.append(row, ignore_index=True)
    row_df = row_df.set_index('Components').transpose()
    return row_df


if __name__ == '__main__':
    TEX_OUT = False

    LOGGER.info('Generating data summary for parametric model test comparisons')

    if TEX_OUT:
        LOGGER.info('Generating LaTeX compatible table markup')
    else:
        low_cdr = TotalSizingLowCDR()
        LOGGER.info(f'{low_cdr.to_df(False).columns}')

    LOGGER.info(generate_pipeline_component_table_baselines(
        hpso='hpso01').to_latex(formatters=None, caption='hpso01'))
