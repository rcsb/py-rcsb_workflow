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


def hash(id):
    return id[1:3]
