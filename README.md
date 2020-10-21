# Topsim Pipelines

This repository stores data and translator scripts to convert HPSO data produced
by the SDP Parametric model into a mid-term Observation Schedule, for use
 during a TOpSim simulation. 
 
 `sdp_system_sizing.py` adapts code from the SDP Parametric model to produce
  a reduced data set for selected HPSOs. 
 `hpso_to_observation.py` then takes this CSV output and converts it to a
  JSON file in the `observations.json` format required by the TOpSim
   simulator.  