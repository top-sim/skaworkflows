# Copyright (C) 15/3/21 RW Bunney

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
Adapted from sdp-par-model, written by Peter Wortmann and availalbe at
https://github.com/ska-telescope/sdp-par-model/
blob/master/sdp_par_model/parameters/definitions.py
"""


class Telescopes:
    """
    Enumerate the possible telescopes to choose from (used in
    :meth:`apply_telescope_parameters`)
    """
    SKA1_Low = 'SKA1_Low'
    SKA1_Mid = 'SKA1_Mid'

    # Currently supported telescopes (will show up in notebooks)
    available_teles = [SKA1_Low, SKA1_Mid]

class Bands:
    """
    Enumerate all possible bands (used in :meth:`apply_band_parameters`)
    """
    # SKA1 Bands
    Low = 'Low'
    Mid1 = 'Mid1'
    Mid2 = 'Mid2'
    Mid5a = 'Mid5a'
    Mid5b = 'Mid5b'

    # group the bands defined above into logically coherent sets
    telescope_bands = {
        Telescopes.SKA1_Low : [ Low ],
        Telescopes.SKA1_Mid : [ Mid1, Mid2, Mid5a, Mid5b ]
    }
    available_bands = telescope_bands[Telescopes.SKA1_Low] + telescope_bands[Telescopes.SKA1_Mid]

class Pipelines:
    """
    Enumerate the SDP pipelines. These must map onto the Products. The HPSOs invoke these.
    """
    Ingest = 'Ingest'             # Ingest data
    RCAL = 'RCAL'                 # Produce calibration solutions in real time
    FastImg = 'FastImg'         # Produce continuum subtracted residual image every 1s or so
    ICAL = 'ICAL'                 # Produce calibration solutions using iterative self-calibration
    DPrepA = 'DPrepA'             # Produce continuum Taylor term images in Stokes I
    DPrepA_Image = 'DPrepA_Image' # Produce continuum Taylor term images in Stokes I as CASA does in images
    DPrepB = 'DPrepB'             # Produce coarse continuum image cubes in I,Q,U,V (with Nf_out channels)
    DPrepC = 'DPrepC'             # Produce fine spectral resolution image cubes un I,Q,U,V (with Nf_out channels)
    DPrepD = 'DPrepD'             # Produce calibrated, averaged (In time and freq) visibility data

    PSS = 'PSS'                   # Sieved Pulsars
    PST = 'PST'                   # Pulsar Timing Solutions
    SinglePulse = 'SinglePulse'   # Transient Buffer Data

    input = [Ingest]
    realtime = [Ingest, RCAL, FastImg]
    imaging = [RCAL, FastImg, ICAL, DPrepA, DPrepA_Image, DPrepB, DPrepC]
    nonimaging = [PSS, PST, SinglePulse]
    output = [FastImg, DPrepA, DPrepA_Image, DPrepB, DPrepC, PSS, PST, SinglePulse]
    all = [Ingest, RCAL, FastImg, ICAL, DPrepA, DPrepA_Image, DPrepB, DPrepC, DPrepD,
           PSS, PST, SinglePulse]
    pure_pipelines = all

    # Pipelines that are currently supported (will show up in notebooks)
    available_pipelines = all

class HPSOs:
    """
    Enumerate the pipelines of each HPSO (used in :meth:`apply_hpso_parameters`)
    """

    # The high-priority science objectives (HPSOs).

    hpso01  = 'hpso01'
    hpso02a = 'hpso02a'
    hpso02b = 'hpso02b'
    hpso04a = 'hpso04a'
    hpso04b = 'hpso04b'
    hpso04c = 'hpso04c'
    hpso05a = 'hpso05a'
    hpso05b = 'hpso05b'
    hpso13  = 'hpso13'
    hpso14  = 'hpso14'
    hpso15  = 'hpso15'
    hpso18  = 'hpso18'
    hpso22  = 'hpso22'
    hpso27and33  = 'hpso27and33'
    hpso32  = 'hpso32'
    hpso37a = 'hpso37a'
    hpso37b = 'hpso37b'
    hpso37c = 'hpso37c'
    hpso38a = 'hpso38a'
    hpso38b = 'hpso38b'

    # Maximal cases for the telescopes. For Mid, define a maximal case
    # for each of the bands. Mid bands 5a and 5b allow a bandwidth of
    # up to 2.5 GHz to be observed simultaneously, so define two cases
    # for each of them corresponding to the lowest and highest 2.5
    # GHz of the band.

    max_low = 'max_low'
    max_mid_band1 = 'max_mid_band1'
    max_mid_band2 = 'max_mid_band2'
    max_mid_band5a_1 = 'max_mid_band5a_1'
    max_mid_band5a_2 = 'max_mid_band5a_2'
    max_mid_band5b_1 = 'max_mid_band5b_1'
    max_mid_band5b_2 = 'max_mid_band5b_2'

    hpso_telescopes = {
        hpso01:  Telescopes.SKA1_Low,
        hpso02a: Telescopes.SKA1_Low,
        hpso02b: Telescopes.SKA1_Low,
        hpso04a: Telescopes.SKA1_Low,
        hpso04b: Telescopes.SKA1_Mid,
        hpso04c: Telescopes.SKA1_Mid,
        hpso05a: Telescopes.SKA1_Low,
        hpso05b: Telescopes.SKA1_Mid,
        hpso13:  Telescopes.SKA1_Mid,
        hpso14:  Telescopes.SKA1_Mid,
        hpso15:  Telescopes.SKA1_Mid,
        hpso18:  Telescopes.SKA1_Mid,
        hpso22:  Telescopes.SKA1_Mid,
        hpso27and33:  Telescopes.SKA1_Mid,
        hpso32:  Telescopes.SKA1_Mid,
        hpso37a: Telescopes.SKA1_Mid,
        hpso37b: Telescopes.SKA1_Mid,
        hpso37c: Telescopes.SKA1_Mid,
        hpso38a: Telescopes.SKA1_Mid,
        hpso38b: Telescopes.SKA1_Mid,
        max_low: Telescopes.SKA1_Low,
        max_mid_band1: Telescopes.SKA1_Mid,
        max_mid_band2: Telescopes.SKA1_Mid,
        max_mid_band5a_1: Telescopes.SKA1_Mid,
        max_mid_band5a_2: Telescopes.SKA1_Mid,
        max_mid_band5b_1: Telescopes.SKA1_Mid,
        max_mid_band5b_2: Telescopes.SKA1_Mid
    }

    # Map each HPSO to its constituent pipelines

    hpso_pipelines = {
        hpso01:  (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
                  Pipelines.ICAL, Pipelines.DPrepA,
                  Pipelines.DPrepB, Pipelines.DPrepC, Pipelines.DPrepD),
        hpso02a: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
                  Pipelines.ICAL, Pipelines.DPrepA,
                  Pipelines.DPrepB, Pipelines.DPrepC, Pipelines.DPrepD),
        hpso02b: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
                  Pipelines.ICAL, Pipelines.DPrepA,
                  Pipelines.DPrepB, Pipelines.DPrepC, Pipelines.DPrepD),
        hpso04a: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
                  Pipelines.PSS),
        hpso04b: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
                  Pipelines.PSS),
        hpso04c: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
                  Pipelines.PSS),
        hpso05a: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
                  Pipelines.PST),
        hpso05b: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
                  Pipelines.PST),
        hpso13:  (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
                  Pipelines.ICAL, Pipelines.DPrepA,
                  Pipelines.DPrepB, Pipelines.DPrepC),
        hpso14:  (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
                  Pipelines.ICAL, Pipelines.DPrepA,
                  Pipelines.DPrepB, Pipelines.DPrepC),
        hpso15:  (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
                  Pipelines.ICAL, Pipelines.DPrepA,
                  Pipelines.DPrepB, Pipelines.DPrepC),
        hpso18:  (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
                  Pipelines.SinglePulse),
        hpso22:  (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
                  Pipelines.ICAL, Pipelines.DPrepA, Pipelines.DPrepB),
        hpso27and33:  (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
                       Pipelines.ICAL, Pipelines.DPrepA, Pipelines.DPrepB),
        hpso32:  (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
                  Pipelines.ICAL, Pipelines.DPrepB),
        hpso37a: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
                  Pipelines.ICAL, Pipelines.DPrepA, Pipelines.DPrepB),
        hpso37b: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
                  Pipelines.ICAL, Pipelines.DPrepA, Pipelines.DPrepB),
        hpso37c: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
                  Pipelines.ICAL, Pipelines.DPrepA,Pipelines.DPrepB),
        hpso38a: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
                  Pipelines.ICAL, Pipelines.DPrepA, Pipelines.DPrepB),
        hpso38b: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
                  Pipelines.ICAL, Pipelines.DPrepA, Pipelines.DPrepB),
        max_low: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg, Pipelines.ICAL, Pipelines.DPrepA,
                  Pipelines.DPrepA_Image, Pipelines.DPrepB, Pipelines.DPrepC, Pipelines.DPrepD),
        max_mid_band1: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg, Pipelines.ICAL, Pipelines.DPrepA,
                        Pipelines.DPrepA_Image, Pipelines.DPrepB, Pipelines.DPrepC, Pipelines.DPrepD),
        max_mid_band2: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg, Pipelines.ICAL, Pipelines.DPrepA,
                        Pipelines.DPrepA_Image, Pipelines.DPrepB, Pipelines.DPrepC, Pipelines.DPrepD),
        max_mid_band5a_1: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg, Pipelines.ICAL, Pipelines.DPrepA,
                           Pipelines.DPrepA_Image, Pipelines.DPrepB, Pipelines.DPrepC, Pipelines.DPrepD),
        max_mid_band5a_2: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg, Pipelines.ICAL, Pipelines.DPrepA,
                           Pipelines.DPrepA_Image, Pipelines.DPrepB, Pipelines.DPrepC, Pipelines.DPrepD),
        max_mid_band5b_1: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg, Pipelines.ICAL, Pipelines.DPrepA,
                           Pipelines.DPrepA_Image, Pipelines.DPrepB, Pipelines.DPrepC, Pipelines.DPrepD),
        max_mid_band5b_2: (Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg, Pipelines.ICAL, Pipelines.DPrepA,
                           Pipelines.DPrepA_Image, Pipelines.DPrepB, Pipelines.DPrepC, Pipelines.DPrepD)
    }

    all_hpsos = {hpso01, hpso02a, hpso02b, hpso04a, hpso04b, hpso04c,
                 hpso05a, hpso05b, hpso13, hpso14, hpso15, hpso18,
                 hpso22, hpso27and33, hpso32, hpso37a, hpso37b,
                 hpso37c, hpso38a, hpso38b}
    all_maxcases = {max_low,
                    max_mid_band1, max_mid_band2,
                    max_mid_band5a_1, max_mid_band5a_2,
                    max_mid_band5b_1, max_mid_band5b_2}
    available_hpsos = all_hpsos | all_maxcases