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
HPSO observation information is stored in a csv file.

Use pandas to read in the information
"""
import pandas as pd

FILENAME = 'hpso_compute_low.csv'
csv = pd.read_csv(FILENAME)
compute_keys = {
	'hpso': "HPSO",
	'time': "Time [%]",
	'tobs': " Tobs [h]",
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

data_keys = {
	'telescope': "Telescope ",
	'pipeline': "Pipeline ",
	'datarate': "Data Rate [Gbit/s]",
	'dailygrowth': "Daily Growth [TB/day]",
	'yearlygrowth': "Yearly Growth [PB/year]",
	'five_yeargrowth': "5-year Growth [PB/(5 year)]"
}
