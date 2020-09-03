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
from IPython.display import display, Markdown

# This requires you to have the sdp-par-model code on your machine; append it
# to your path
sys.path.insert(0, "../sdp-par-model")

# Peter's SDP system sizing notebook code.
from sdp_par_model import reports, config
from sdp_par_model.parameters.definitions import Telescopes, Pipelines, \
	Constants, HPSOs

csv = reports.read_csv("/home/rwb/github/sdp-par-model/data/csv/2019-06-20"
					   "-2998d59_pipelines.csv")

# TODO MOVE THESE INTO DICTIONARY FOR EASIER ACCESS WITH PANDAS DATAFRAME
keys = [
	"| HPSO | Time [%] | Tobs [h] | Ingest [Pflop/s] | RCAL [Pflop/s] | FastImg"
	+ "|[Pflop/s] | ICAL [Pflop/s] "
	+ "| DPrepA [Pflop/s] | DPrepB [Pflop/s] | DPrepC [Pflop/s] "
	+ "| DPrepD [Pflop/s] "
	+ "| Total RT [Pflop/s] | Total Batch [Pflop/s] | Total [Pflop/s] | "
]


def make_compute_table(tel):
	table = [
		"| HPSO | Time [%] | Tobs [h] | Ingest [Pflop/s] | RCAL [Pflop/s] | FastImg [Pflop/s] | ICAL [Pflop/s] " +
		"| DPrepA [Pflop/s] | DPrepB [Pflop/s] | DPrepC [Pflop/s] | DPrepD [Pflop/s] " +
		"| Total RT [Pflop/s] | Total Batch [Pflop/s] | Total [Pflop/s] | "]
	table.append("-".join("|" * table[0].count('|')))
	flop_sum = {pip: 0 for pip in Pipelines.available_pipelines}
	pips = [Pipelines.Ingest, Pipelines.RCAL, Pipelines.FastImg,
			Pipelines.ICAL, Pipelines.DPrepA, Pipelines.DPrepB,
			Pipelines.DPrepC, Pipelines.DPrepD, ]
	for hpso in sorted(HPSOs.all_hpsos):
		if HPSOs.hpso_telescopes[hpso] != tel:
			continue
		Tobs = lookup('Observation Time', hpso).get(Pipelines.Ingest, 0)
		Texp = lookup('Total Time', hpso).get(Pipelines.Ingest, 0)
		flops = lookup('Total Compute Requirement', hpso)
		Rflop = sum(flops.values())
		Rflop_rt = sum([Rflop for pip, Rflop in flops.items() if
						pip in Pipelines.realtime])
		time_frac = Texp / total_time[tel]
		for pip, rate in flops.items():
			flop_sum[pip] += time_frac * rate
		flops_strs = ["{:.2f}".format(flops[pip]) if pip in flops else '-' for
					  pip in pips]
		table.append(
			"|{}|{:.1f}|{:.1f}|{}|{}|{}|{}|{}|{}|{}|{}|{:.2f}|{:.2f}|{:.2f}|".format(
				hpso, time_frac * 100, Tobs / 3600, *flops_strs, Rflop_rt,
					  Rflop - Rflop_rt, Rflop))
	table.append(
		"| **Average** | - | - | {:.2f}|{:.2f}|{:.2f}|{:.2f}|{:.2f}|{:.2f}|{:.2f}|{:.2f}|{:.2f}|{:.2f}|{:.2f}|".format(
			*[flop_sum.get(pip, 0) for pip in pips],
			sum([Rflop for pip, Rflop in flop_sum.items() if
				 pip in Pipelines.realtime]),
			sum([Rflop for pip, Rflop in flop_sum.items() if
				 pip not in Pipelines.realtime]),
			sum(flop_sum.values())))
	return "\n".join(table)


for tel in Telescopes.available_teles:
	#     display(Markdown("##### {}:\n\n".format(tel) + make_compute_table(tel)))
	make_compute_table(tel)
