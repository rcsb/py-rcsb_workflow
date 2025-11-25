##
# File:    BcifWorkflow.py
# Author:  James Smith
# Date:    21-Feb-2025
##

"""
Run workflow for pdb entries specified in input list.
"""

__docformat__ = "google en"
__author__ = "James Smith"
__email__ = "james.smith@rcsb.org"
__license__ = "Apache 2.0"

import os
import logging
import tempfile
import sys
from rcsb.workflow.bcif.task_functions import (
    convertCifFilesToBcif,
    convert,
    deconvert,
    getDictionaryApi,
)

logger = logging.getLogger(__name__)


class BcifWorkflow:

    def __init__(self, args):

        # mode
        self.mode = args.mode
        # paths and files
        if args.mode == "full_file_list":
            self.listFileBase = args.listFileBase
            self.listFileName = args.listFileName
            self.remotePath = args.remotePath
            self.outputPath = args.outputPath
        elif args.mode in ["cif_to_bcif", "bcif_to_cif"]:
            self.infile = args.infile
            self.outfile = args.outfile
        # settings
        if args.mode == "full_file_list":
            self.contentType = args.contentType
            self.nfiles = int(args.nfiles)
            self.outfileSuffix = args.outfileSuffix
            self.outputContentType = bool(args.outputContentType)
            self.outputHash = bool(args.outputHash)
            self.inputHash = bool(args.inputHash)
            self.batchSize = int(args.batchSize)
        # config
        self.pdbxDict = args.pdbxDict
        self.maDict = args.maDict
        self.rcsbDict = args.rcsbDict
        self.ihmDict = args.ihmDict
        self.flrDict = args.flrDict
        #
        self.validate()

    # public method required by pylint
    def validate(self):

        assert self.mode in [
            "full_file_list",
            "cif_to_bcif",
            "bcif_to_cif"
        ], "error - require that mode is one of full_file_list, cif_to_bcif, or bcif_to_cif"

        if self.mode == "full_file_list":

            assert self.outfileSuffix in [
                ".bcif",
                ".bcif.gz",
            ], "error - require either .bcif or .bcif.gz output file"

            assert self.contentType in [
                "pdb",
                "csm",
                "ihm",
            ], "error - content type must be pdb, csm, or ihm"

        elif self.mode in ["cif_to_bcif", "bcif_to_cif"]:

            if not os.path.exists(self.infile):
                sys.exit("error - input file %s not found" % self.infile)

    def __call__(self):

        if self.mode == "full_file_list":
            logger.info(
                "running bcif workflow on %s and %s",
                os.path.join(self.listFileBase, self.listFileName),
                self.remotePath,
            )
            convertCifFilesToBcif(
                self.listFileName,
                self.listFileBase,
                self.remotePath,
                self.outputPath,
                self.outfileSuffix,
                self.contentType,
                self.outputContentType,
                self.outputHash,
                self.inputHash,
                self.batchSize,
                self.nfiles,
                self.pdbxDict,
                self.maDict,
                self.rcsbDict,
                self.ihmDict,
                self.flrDict,
            )
        elif self.mode in ["cif_to_bcif", "bcif_to_cif"]:
            workpath = tempfile.mkdtemp()
            dictionaryApi = getDictionaryApi(
                self.pdbxDict, self.maDict, self.rcsbDict, self.ihmDict, self.flrDict
            )
            if self.mode == "cif_to_bcif":
                convert(self.infile, self.outfile, workpath, dictionaryApi)
                logger.info("converted %s to %s", self.infile, self.outfile)
            elif self.mode == "bcif_to_cif":
                deconvert(self.infile, self.outfile, workpath, dictionaryApi)
                logger.info("deconverted %s to %s", self.infile, self.outfile)
