========
Data origins
========

System sizing
--------------

The data for system sizing used in the parametric model for the scheduling
work is spread throughout the SDP design documentation. The following is a
list of where the information for certain values may be found.

**Average FLOP-rate**:
- SKA1 SDP System Sizing, 3.1 (Table 2).
	- Weighted average FLOPs are put at 13.6 PFLOPs
- SDP Memo 025; Updated SDP Cost basis of Estimate June 2016.
	- Based on this document, which produces the system specification, this
is reflecting a ~20% efficiency.
	- This efficiency is reported in Broekma et al 2015
	- This efficiency was updated in 2016 due to further analysis (~10%).
	- However, the parametric model uses adjusted 9.623 (~13% efficiency).
- SKA1 Scheduling and Archive Constraints, 3.3 pg. 13
	- 10% efficiency estimations are used here.

**Total buffer size**:
- SKA SDP system sizing states 46PB for Low (Section 4)
- SKA Memo 025 states ~67PB (and also 60.7)
- Parametric model uses ~69PB