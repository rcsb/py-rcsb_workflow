TCRD v6.11.0
Release Date: 20210422
======================
This README describes changes from v6.10.0


JensenLab PubMed Scores
=======================
v6.10.0 had a problem with many 'JensenLab PubMed Score' values in the
tdl_info table. The year by year scores in the pmscore table were
correct and remain unchanged in v6.11.0. However, since the values in
tdl_info are used for TDL calculation, many targets were assigned Tdark
that should be Tbio. I have corrected the data in tdl_info where itype =
'JensenLab PubMed Score' and TDLs were recalculated and updated.





