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
import json
import pandas as pd

compute_keys = {
	'hpso': "HPSO",
	'time': "Time [%]",
	'stations': "Stations",
	'tobs': "Tobs [h]",
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
	'total': "Total [Pflop/s]"
}

compute_units = {
	'hpso': None,
	'time': "%",
	'stations': None,
	'tobs': "h",
	'ingest': "Pflop/s",
	'rcal': "Pflop/s",
	'fastimg': "Pflop/s",
	'ical': "Pflop/s",
	'dprepa': "Pflop/s",
	'dprepb': "Pflop/s",
	'dprepc': "Pflop/s",
	'dprepd': "Pflop/s",
	'totalrt': "Pflop/s",
	'totalbatch': "Pflop/s",
	'total': "Pflop/s"
}

# Keys for DATA csv
data_keys = {
	'telescope': "Telescope",
	'pipeline': "Pipeline ",
	'datarate': "Data Rate [Gbit/s]",
	'dailygrowth': "Daily Growth [TB/day]",
	'yearlygrowth': "Yearly Growth [PB/year]",
	'five_yeargrowth': "5-year Growth [PB/(5 year)]"
}


def convert_systemsizing_csv_to_dict(csv_file):
	csv = pd.read_csv(csv_file)

	# Keys for COMPUTE csv
	reverse_keys = {v: k for k, v in compute_keys.items()}

	df = csv.rename(columns=reverse_keys)

	observations = [dict(x._asdict()) for x in df.itertuples(index=False)]
	for observation in observations:
		for key in observation:
			v = observation.get(key)
			tmp = (v, compute_units[key])
			observation[key] = tmp

	return observations


def observation_plan(hpso_list, per_hpso_count, system_sizing):
	"""
	Given a sequence of HPSOs that are present in the system sizing
	dictionary, generate a plan. of observations from which we can create
	telescope config.

	Parameters
	----------
	hpso_list : list()
		A list of strings describing the HPSOs that are to be placed in the plan

	per_hpso_count : list()
		A list of Integers that describes how many of each HPSO is in the plan

	system_sizing : dict()
		A dictionary containing the system sizing values for the current set
		of HPSOs and the corresponding plan.

	Returns
	-------

	"""


def construct_telescope_config_from_observation_plan(observations):
	"""
	Based on a simplified dictionaries built from the System Sizing for the
	SDP, this function builds a mid-term plan based on the HPSO sizes.

	The structure of an observation.json file takes the following:

	{
	  "telescope": {
	  	"total_arrays": # This is adapted from the maximum number of
	  	'stations' in the system_sizing csv.

	  	"pipelines": {
	  		# Pipelines are taken from the HPSOs that are outlined in the plan;
	  		"pipeline_name": {
	  		   "demand": demand is taken from 'totalrt', which is the total
	  		   Real-time compute required for the observation (in PFlops/s).
	  		}
	  	}
	  	"observations": [
			# a list of observations in the order of the 'plan'
			"name": # this is a place holder
			"start": This is relative to the first, in hours from first
			observation start time
			"duration": Tobs, from system sizing
			"demand": this is the number of stations it requires
			"workflow": where the workflow file is stored
			"type": what pipeline it is
			"data_product_rate": This is
	  	]
	  }
	}

	Returns
	-------
	config : dict()
		A dictionary that is in the form required of the TOpSim simulator to
		configure a Telescope (see topsim.core.telescope.Telescope).

	"""
	# Find max number of arrays:

	max_stations, unit = max([x['stations'] for x in observations])
	pipelines = [x['hpso'] for x in observations]
	config = {
		'telescope': {
			'total_arrays': max_stations
		},
		'observations': [

		]
	}
	return config


if __name__ == '__main__':
	observations = convert_systemsizing_csv_to_dict('SKA1_Low_COMPUTE.csv')
	config = construct_telescope_config_from_observation_plan(observations)
	filename = 'config.json'
	with open(filename, 'w') as jfile:
		json.dumps(jfile, indent=4)
