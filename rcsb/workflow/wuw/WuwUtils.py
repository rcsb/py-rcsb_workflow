"""Hashing functions for file storage."""
##
# File: WuwUtils.py
# Date: 04-Jun-2025  mjt
#
#  Shared hashing function  - for file generation
#
#  Updates:
#  04-Jun-2025 mjt Created hashing function
#
##
__docformat__ = "google en"
__author__ = "Michael Trumbull"
__email__ = "michael.trumbull@rcsb.org"
__license__ = "Apache 2.0"


def idHash(structure_id):
    if structure_id.lower().startswith("af_") or structure_id.lower().startswith("ma_"):
        return f"{structure_id[0:2]}/{structure_id[-6:-4]}/{structure_id[-4:-2]}/"
    if structure_id.lower().startswith("pdb_") or len(structure_id) == 4:
        return f"{structure_id[-3:-1]}/"
    raise RuntimeError("Unsupported structure id %s" % structure_id)
