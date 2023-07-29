
## Features necessary to produce a TopSim configuration file

In order to produce an observation workflow with appropriate node and edge costs (tasks and data transfer, respectively), the following is required:


- [x] Relevant Parametric model output for Telescope (low or mid) and the baseline length (short, mid, long)
    - This is (ideally) pre-calculated and stored in `skaworkflows/data/pandas_sizing` directory.
- [ ] Compute system config
    - Stored in JSON format
    - ~~Systems generated for tests and experiments using this set of modules use the `hpconfig` library.~~
        - Compute system config is located in hpconfig, with SDP specifications having a `to_topsim` function.
        - Specifications are stored as classes, as their details are unlikely to change and it gives us an opportunity to specify multiple instances, and have spec-specific class methods for data transformation (e.g. `to_topsim`, `to_latex`).

- [ ] Telescope configuration
    - Needs TOTAL system sizing
    - Get Ingest Rate from observation (Baseline-dependent)
    - Get total number of stations (Gives MAX telescope demand; can use up to 16* )


- Observation(s) , including information on
    - Baseline length (short, mid, long)
    - Channels (binned channels is what we use)
    - Observation duration (used to tie break)
    - Maximum number arrays

- Using observation details, we can derive:
    - Ingest requirements for the observation (in number of resources) based on compute provided by the compute system configuration
        - Iteratively store the maximum ingest requirements for a simulation to determine the maximum number of ingest pipeliens needed.
    - [x] Number of channels is used to construct the final observation path
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


### Using the parametric model data

#### Calculating edge requirements for the data

e.g. DeGridding for an entire imaging pipeline for long baseline is 0.1Terrabytes/second

For an application that takes 1 minute, this is 6 terrabytes of data; not taking data transfer time into account, existing system provisioning shows that for a single node the memory is 31GB. The data input into a node's memory is far greater, which requires the application to process data in chunks, write to disk, and then process the data again.
