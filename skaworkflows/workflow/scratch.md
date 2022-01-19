Task: Test scatter is okay

Two steps for this:

    Make eagle_lgt_scatter_channel-upate.graph the same as lgt_scatter.graph, and ensure the loops are all the same (Major and minor).
    Unroll channel-update to a PGT, then double-check the two are the same after channel update and translation


Details for the Major/Minor loops:

Major loop has 2 loops
Minor loop as 2 loops

The Scatter has 4 'scatters' - i.e. we are 'binning' to 4 separate frequency channels. This means the Gather also requires 'num_copies' of 4, to 'gather the existing copies' back together (modelling a transition into the data cube).

    * Number of nodes in the PGT produced by the templates  is 283


    