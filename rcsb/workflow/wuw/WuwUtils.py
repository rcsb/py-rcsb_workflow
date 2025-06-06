"""Hashing functions for file storage."""
##
# File: hashDirectories.py
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


def idHash(name):
    if "_" in name:
        return f"{name[0:2]}/{name[-6:-4]}/{name[-4:-2]}/"
    return f"{name[-3:-1]}/"
