# Test data overview


## `eagle_lgt_scatter.graph`
Details for the Major/Minor loops:

* Major loop has 2 loops
* Minor loop as 2 loops


## `eagle_lgt_scatter_channel-update.graph`

The Scatter has 4 'scatters' - i.e. we are 'binning' to 4 separate frequency channels. This means the Gather also requires 'num_copies' of 4, to 'gather the existing copies' back together (modelling a transition into the data cube).
