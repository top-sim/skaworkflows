# Changelog

From 2022-01-20, all changes will described in this file. 

This project takes inspiration from [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

 ## [Unreleased]
 Outline of planned changes.
 
 ### 0.3.0 
 - CLI interface with JSON-header component that describes flags used.  
 
 ### 0.2.1 
 - Testing and integration hpconfig within `skaworkflows`
 
 ### 0.2.0
 - Minimal-viable workflow generation 
 
 ### 0.1.1-x
 - Generate folder with workflow files & correct costings. 
 - Update costs in worklow to ensure it completely represents the total flops 
 
 ## [0.1.0] 2022-01-19
 
 Previous changes are going to be subsumed within this section. Thus, version
  0.1.0 is going to include all development up until [the most recent commit as of 2022-01-19
  ](https://github.com/top-sim/skaworkflows/commit/577a431f66aa6c2dfc566f1026da7bd33fe07a36).
  This is when we have established CI/CD infrastructure, and can keep track of the build status and test coverage moving forward. We will continue with 0.1.x until we have a minimal complete simulation configuration generated. 
  
Functionality that as of 0.1.0:
- `skaworkflows.datagen` is feature complete. It is possible we will need to make (small) updates to its functionality, but producing data from this is possible now. 
- `skaworkflows.eagle_daliuge_translation` is also feature complete. It may be necessary to update functionality moving forward, but these will become unreleased/planned changes that will be signposted and updated in the future. Currently, we can produce a translated, concatenated workflow, and have good test coverage (>90%). 
 - `skaworkflows.hpso_to_observation` continues to be in development, with failing tests and incomplete coverage. This the motivation for release 0.1.2-x is to fix errors and get a minimal viable skaworkflows running, with 0.2.0 to be a minimal-viable generator.  
 