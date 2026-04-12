.. _telescope_overview:

=========================
Telescope configuration
=========================

The telescope in the SKA pipelines project is a greatly simplified version of
what the SKA (Low and Mid) are. The model is simple, as the intention is to
focus on the workflows, and have the telescope act as a 'logical instrument'
rather than a significant component of its own. The main details of the
telescope in the characterisation of workflows, and in the configuration, is
in the maximum number arrays that are available, and the maximum ingest rate
expected from observations::

	"telescope": {
		"total_arrays": 36,
		max_ingest_resources
		"pipelines:{
			"hpso4a_0": {
				"workflow": "test/data/config/workflow_config.json",
				"ingest_demand": 64
			},
		...
		}
		...


Arrays
------

The SKA-SDP architecture and SKA Baseline documentation stipulates that the
telescope must support up to 16 concurrent observations at any given time.
Hence, this means that there is a minimum telescope demand of
1/16*MAX_ARRAYs, for whatever maximum that exists for the telescope.

Ingest
------
The telescope configuration for a simulation will have a maximum ingest
requirement, as well as individual ingest requirements for each
observation workflow.


As described in the Data origins section, we use the full system sizing to
generate configuration for Telescope