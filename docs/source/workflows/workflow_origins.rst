.. _workflow_origins:

===================
Workflow structures
===================

Basic terminology
-----------------

The SKA refers to workflows as pipelines, which is the term typically used in astronomy. We will be referring to them as workflows, as this is the terminology that is consistent with the DAG structure we employ, and is how the literature refers to the scheduling approaches.

We will split the SKA workflows into three categories, based in part on the SDP Parametric model (Bolton 2019), and in part from the Pipeline designs in Nijboer et al. 2016 and Wortmann et al. 2017: Ingest, Imaging, and Non-imaging.

**Ingest** workflows include all the real-time workflows that occur during observation (Ingest, Real-time calibration, and Fast Imaging). In TopSim, and in this TopSim SKA Characterisation, Ingest pipelines are modelled as 'blocks' of computing; for the duration of an observation, we calculate the number of required to produce the predicted total FLOPs of the real-time workflows, and reserve these as 'ingest' machines. Hence, observations have impact on the ability for the compute system to provide resources to post-observation data processing.

**Imaging** workflows are the major scheduling focus within the SKA SDP, as they are not directly associated with running observations, and so have the potential to be moved and reorganised within the mid-term plan. The imaging workflows (also known as Data Product preparation (DPrep) workflows) produce visibility data using either continuum (DPrepA,B) , spectral (DPrepC), orcalibrated (DPrepD) solutions. For the purpose of this workflow library, we also include iterative calibration (ICAL) as an imaging pipeline, as it is included in the SDP Parametric model as an 'offline' workflow (Bolton 2019). There is a significant similarity in the structure of the workflows; as noted in Nijboer, Continuum imaging (DPrepA) and ICAL will likely have similar workflows. A spectral imaging workflow will also have similarities, as it is a requirement that DPrepA/B is run prior to the execution of the spectral line in order to produce the continuum visibilities. Hence, for the sake of demonstration, we will use the EAGLE continuum imaging graph for all imaging pipelines. The differences between worklows is then determined by the frequency split, and the computational/data costs for the subcomponents in each graph.

**Non-imaging** worklows refer to Pulsar search/timing workflows. These are smaller workflows with much reduced computational load (relative to ICAL or DPrepA). Within the parametric model, these workflows have no components, and are instead modelled as a single 'Total FLOP/s and TB/s output'.