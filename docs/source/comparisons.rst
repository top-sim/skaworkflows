.. comparisons:

========================================
Comparing with the SDP Parametric model
========================================

The task compute and data values are derived from the SDP parametric model, which attempted to quantify the required FLOPs and Bytes for each radio astronomy (RA) application used in radio data reduction and analysis. We are applying these costs to the concept of a science workflow, where we are interested in organising the logical sequence of tasks required to reach an end-user science data product. Hence when constructing the workflows in EAGLE (described in Workflows section) we lean heavily on these costed 'components' - the tasks - as a basis for the workflow composition.

Part of the motivation of this data generation is in comparing existing predictions for post-observation workflow runtime, and how a workflow representation affects this. This is why we distribute the FLOPs and data across tasks. This section explains where we find the values in the parametric model and how we use it in the `skaworkflows` modules.


Infrastructure config
---------------------

The infrastructure component of `skaworkflows` and TopSim is necessarily different to the structure of the parametric model; as we are interested in scheduling workflows, we need to deconstruct the compute infrastructure to individual nodes such that we can perform task-->machine allocations.

`skaworkflows.hpconfig.specs.sdp` is where we store these materials, which is adapted from the updated SDP cost estimates [1]_. This document describes an idealised hardware composition for the SDP compute infrastructure, detailing the number of compute nodes, GPUs/CPUs-per-node, bandwidth etc. The cumulative capacity of these (compute, memory, storage etc.) is used to provide the total capacity of both SKA-Low and -Mid SDP compute centres. These cumulative values are used, with some variations, in the parametric model [2]_.

For comparative models, we use the following cumulative totals, subsequently derived per-node-capacities. These are mostly taken from the parametric model and then calculated for individual nodes.

* **Compute**: This is derived from the cost estimates model, which gives number of nodes (896 for Low, 786 for Mid), number of GPUs, and TFLOPs/GPU. The total compute capcity is the cumulative sum of these individual nodes. However, the achieved compute value - which is used to estimate total workflow runtime on the system - is less than this. The parametric model uses a value that is ~17.3% of the total capacity; this is  between an original estimation of 20% efficiency and a revised value of 10% [3]_. When we calculated how many FLOPS/sec each compute node provides, we use that value so:

    **Node FLOPS/Sec = GPU peak flops * No. GPUs * Estimated Efficiency.**

* Storage

* **I/O**: I/O is very complex, and we lean heavily on the assumptions made in the parametric model - this is in-part to make sure that we can replicate their results to form a baseline prior to experimenting. There are two approaches to I/O we are concerned with:
    * Transfer (inter-node): this happens between nodes, e.g. transfer data from one part of the system to another.
    * Task (intra-node): This happens when an application reads data from disk, and thus the CPU/GPU experiences bottlenecks from the speed of the disk it is reading from.

    For our simulation purposes, we focus on two of the parametric model values - ingest rates, input buffer rates, and offline rates.
    * Ingest rates: these are used to determine the maximum amount of data that can be ingest into the system. An `observation` in our simulation config will have a `data_product_rate`, which is a TB/s value indicating ingest rates. The buffer stores the limit as `['hot']['max_ingest_rate']`.
    * Task transfer rates: These are described in the `graph` construction in the parametric model. These are used when determining the total offline workflow runtime.


.. code-block::
    :caption: A cool example

        testing
        testing


The output of this line starts with four spaces.

At :numref:`label` you can see

Compute values
---------------

Compute valuates



References
-----------

.. [1] Alexander, P., Bolton, R., Graser, F., & Taylor, J. (2016). SDP Memo 025: Updated SDP Cost Basis of Estimate June 2016. SKA SDP Consortium. http://ska-sdp.org/sites/default/files/attachments/ska-tel-sdp-0000091_c_rep_updatedsdpcostbasisofestimatejune2016.pdf

.. [2] https://github.com/ska-telescope/sdp-par-model/blob/master/notebooks/SKA1_Scheduling_New.ipynb

.. [3] Nikolic, B. (2016). SDP Memo: Estimating the SDPComputational Efficiency (Memo SKA-TEL-SDP-0000086). http://ska-sdp.org/sites/default/files/attachments/ska-tel-sdp-0000086_c_rep_sdpmemoefficiencyprototyping_-_signed.pdf
