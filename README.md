# Topsim Pipelines

This repository stores data and translator scripts to convert HPSO data produced
by the SDP Parametric model into a mid-term Observation Schedule, for use
 during a TOpSim simulation. 
 
 `sdp_system_sizing.py` adapts code from the SDP Parametric model to produce
  a reduced data set for selected HPSOs. 
 `hpso_to_observation.py` then takes this CSV output and converts it to a
  JSON file in the `observations.json` format required by the TOpSim
   simulator.  
   

## Steps to generating provisional workflow characterisations

In order to produce an observation workflow with appropriate node and edge costs (tasks and data transfer, respectively), the following is required:


- [x] Relevant Parametric model output for Telescope (low or mid) and the baseline length (short, mid, long)
    - This is (ideally) pre-calculated and stored in `data/pandas_sizing` directory. 
- Compute system config
    - Stored in JSON format
    - Systems generated for tests and experiments using this set of modules use the `hpconfig` library.
- Telescope configuration

- Observation(s) , including information on 
    - Baseline length (short, mid, long)
    - Channels (binned channels is what we use) 
    - Observation duration (used to tie break) 
    - Maximum number arrays  
 
- Using observation details, we can derive:
    - Ingest requirements for the observation (in number of resources) based on compute provided by the compute system configuration 
        - Iteratively store the maximum ingest requirements for a simulation to determine the maximum number of ingest pipeliens needed. 
    - [ ] Number of channels is used to construct the final observation path
        - [ ] Get number of channels from observation and pass to workflow generator
        - [ ] Use this to modify the number of channels in the logical graph template for unrolling
        - [ ] HPSOs linked to EAGLE graphs opened and `unrolled_nx` passed as data 
    - Get the component values for each workflow tasks
        - Baseline provides provisional global estimate for entire observation workflow
        - Number of arrays as a % of total telescope arrays creates a fraction of this
        - Divide this by number of each tasks 
        - Assign the final value to each task name 
            - Option to have stochastic error on tasks cost (i.e. subtract from one task and add to another). 
    - After each component is stored, save final worklow as JSON and return path to the observation
- Doing the above for each observation creates the necessary details required to generate a complete simulation configuration, consisting of: 
    - Telescope configuration


## Generating HPSO data

The HPSO data described in the sdp-par-model is exported from the code into CSV
files using the `sdp_par_model.reports` code. An example of this is used in
the `notebooks/SKA1_Export.ipynb` notebook in the sdp-par-model repository. 

