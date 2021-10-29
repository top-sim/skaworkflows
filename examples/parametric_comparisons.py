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

import logging

import pandas as pd

from pipelines.common import SI

LOGGER = logging.getLogger(__name__)


def produce_tex_table():
    """
    For the final
    Returns
    -------
    String representation of data frame information
    """
    df = pd.DataFrame
    return df.to_latex()


def format_low_adjusted():
    """
    Using the Low Adjusted data produced by SDP parametric model.
    We then want to have the resulting configuration expectation for our
    simulator mdodel (that is, what are the specs for each device going to
    look like).

    Returns
    -------
    df
    """
    SKALOW_nodes = 896
    gpu_per_node = 2
    gpu_peak_flops = 31 * SI.tera  # Double precision
    memory_per_node = 31 * SI.giga

    df = pd.DataFrame()
    # These values are derived from 'somewhere'.
    low_adjusted = {
        'total_flops': 9.623 * SI.peta,
        # This is workflow buffer/offline
        'cold_buffer_size': int(43.35 * SI.peta),  # Byte
        # This is ingest buffer
        'hot_buffer_size': int(25.5 * SI.peta),  # Byte # 2

        'delivery_buffer_size': int(0.656 * SI.peta),  # Byte
    }
    # Assume the size of the

    return df


if __name__ == '__main__':
    TEX_OUT = False

    LOGGER.info('Generating data summary for parametric model test comparisons')

    if TEX_OUT:
        LOGGER.info('Generating LaTeX compatible table markup')
