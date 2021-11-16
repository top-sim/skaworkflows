.. _observations_overview:

=============
Observations
=============

Observations are a key component of TopSim. They form the basis for the
mid-term schedule, and are the basis for workflow generation and composition.
In the simulation configuration, they are a sub-component of the `telescope`
key, due to the Instrument actor needing to know its observation schedule.
However, observations in the SKA workflows modules have a variety of
components to themselves, and so it is necessary to create a separate series
of pages for them.

	"observations": [
	{
		"name": "hpso4a_0",
		"start": 0,
		"duration": 120,
		"instrument_demand": 128,
		"data_product_rate": 256
	},

-----
Name
-----

The name of an observation