.. TopSim Pipelines documentation master file, created by
   sphinx-quickstart on Wed May  5 13:01:01 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

SKA workflow characterisation
============================================

This set of pages describes the processes underlying the generation of TopSim
configuration files and workflows. These workflows form the basis of
experiments used to evaluate different scheduling approaches' performance when
applied to workflow scheduling for the Square Kilometre Array Sciene Data
Processor (SKA SDP). 

Workflows are generated based on information including the observation
schedule, theoretical system sizing of pipeline components, and a Logical Graph
Template produced by the `EAGLE <https://eagle.icrar.org/>`_ online graph editor. 

The source code repository is found under the
http://github/top-sim/ organisation. 


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   data/origins
   telescope/telescope_overview
   generating_config
