.. _generating_config:

==================================
Complete simulation configuration
==================================

-----------------------------------
Bringing it all together
-----------------------------------

The function that is likely to be used most is
`compile_observations_and_workflows`. This collates all the information
required to produce a simulation configuration file and produces it, along
with subdirectories containing each observations' workflow file.


.. currentmodule:: pipelines.hpso_to_observation

.. autofunction:: compile_observations_and_workflows

.. autoclass:: pipelines.hpso_to_observation.Observation
	:members:

--------------------
Where is everything?
--------------------

------------------------
Recommended user 'flow'
-------------------------

There are two approaches one can use to generate configuration files for the

