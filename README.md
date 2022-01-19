# Topsim pipelines

[![Build Status](https://app.travis-ci.com/top-sim/skaworkflows.svg?branch=master)](https://app.travis-ci.com/top-sim/skaworkflows) 

[![Coverage Status](https://coveralls.io/repos/github/top-sim/skaworkflows/badge.svg)](https://coveralls.io/github/top-sim/skaworkflows) 

This repository stores data and translator scripts to convert HPSO data produced
by the SDP Parametric model into a mid-term Observation Schedule, for use
 during a TOpSim simulation. 
 
   
To do so, this provides the following modules, which may work separately or in conjunction with others to produce a complete Telsescope Observation Plan, complete with observation-specific workflows translated from ICRAR's [EAGLE editor](http://eagle.icrar.org). 

`data/`
- data/pandas_system_sizing.py`
    - The `sdp-par-model` code produces a human readable .csv file with a significant amount of data. The purpose of this code is to filter this information and collate only what is necessary for component-based sizing, which will allow us to produce per-task FLOPs weights for each observation workflow. 
    - The most up-to-date runs of `pandas_system_sizing.py` is stored in `data/pandas_sizing`.
- data/csv  

 - `sdp_system_sizing.py` adapts code from the SDP Parametric model to produce
  a reduced data set for selected HPSOs. 
 - `hpso_to_observation.py` then takes this CSV output and converts it to a
  JSON file in the `observations.json` format required by the TOpSim
   simulator.  
   

