# Copyright (C) 29/10/21 RW Bunney

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
import math

from skaworkflows.hpconfig.specs import sdp
from skaworkflows.common import SI

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


def collate_sdp_globaldataframe(adjusted=False, human_readable=True):
    """
    Get the global system compute values for the SDP and produce the data frame

    cdr : bool
        If True, use 'Adjusted' specifications
    Returns
    -------

    """

    if adjusted:
        sdp_obj = sdp.SKA_Adjusted()
    else:
        sdp_obj = sdp.SDP_LOW_CDR()

    if human_readable:
        LOGGER.debug('Human Readable selected')
        sdp_df = sdp_obj.to_df()
        col_fmt = 'c' * len(sdp_df.columns)
        return sdp_df.to_latex(
            index=False, escape=False, column_format=col_fmt
        )
    else:
        return sdp_obj.to_df(human_readable)


def calculate_equivalent_time_sdp(adjusted=True, telescope='low'):
    """

    Parameters
    ----------
    adjusted : bool
        If False, use 'cdr' metrics
    Returns
    -------

    """

    sdpc = sdp.SDP_LOW_CDR()
    required_floprate = (
            sdpc.maximal_use_case / sdpc.maximal_obs_time
    )
    total_nodes = 0
    total_system_flops = 0
    if telescope == 'low':
        # Required flops does not take into account expected system performance
        total_system_flops = required_floprate / sdpc.estimated_efficiency
        compute_per_node = sdpc.gpu_peak_flops * sdpc.gpu_per_node
        additional_nodes = math.ceil(
            (total_system_flops - sdpc.total_compute) / compute_per_node
        )
        total_nodes = additional_nodes+sdpc.total_nodes

    df = sdpc.to_df(human_readable=True)
    row = dict(df.iloc[0])
    row['Telescope'] = 'Low (Equivalent Time)'
    row['$|M_{\\mathrm{SDP}}$'] = (
            '\\textcolor{red}{' +
            f'{total_nodes}'
            + '}'
    )
    row['$\\mathrm{TP}_{\\mathrm{SDP}}$ (PFLOPS)'] = (
            '\\textcolor{red}{'
            + f'{math.ceil(total_system_flops/SI.peta)}'
            + '}'
    )

    df.loc[len(df)] = row
    return df


if __name__ == '__main__':
    LOGGER.info('Producing computer readable system config')
    LOGGER.info(
        f'{collate_sdp_globaldataframe(adjusted=False, human_readable=False)}')
    LOGGER.info('Producing latex_compliant system config')
    LOGGER.info(collate_sdp_globaldataframe(adjusted=False,
                                            human_readable=True))
    LOGGER.info("Number of nodes needed to compute in same time as observation")


    LOGGER.info(calculate_equivalent_time_sdp().to_latex(index=False,
                                                         escape=False))


