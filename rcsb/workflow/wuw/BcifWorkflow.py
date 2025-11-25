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
        if args.mode in ["wuw", "workflow"]:
            self.listFileBase = args.listFileBase
            self.listFileName = args.listFileName
            self.remotePath = args.remotePath
            self.outputPath = args.outputPath
        elif args.mode in ["convert", "deconvert"]:
            self.infile = args.infile
            self.outfile = args.outfile
        # settings
        if args.mode in ["wuw", "workflow"]:
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
            "wuw",
            "workflow",
            "convert",
            "deconvert",
        ], "error - require that mode is one of wuw, workflow, convert, or deconvert"

        if self.mode in ["wuw", "workflow"]:

            assert self.outfileSuffix in [
                ".bcif",
                ".bcif.gz",
            ], "error - require either .bcif or .bcif.gz output file"

            assert self.contentType in [
                "pdb",
                "csm",
                "ihm",
            ], "error - content type must be pdb, csm, or ihm"

        elif self.mode in ["convert", "deconvert"]:

            if not os.path.exists(self.infile):
                sys.exit("error - input file %s not found" % self.infile)

    def __call__(self):

        if self.mode in ["wuw", "workflow"]:
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
        elif self.mode in ["convert", "deconvert"]:
            workpath = tempfile.mkdtemp()
            dictionaryApi = getDictionaryApi(
                self.pdbxDict, self.maDict, self.rcsbDict, self.ihmDict, self.flrDict
            )
            if self.mode == "convert":
                convert(self.infile, self.outfile, workpath, dictionaryApi)
                logger.info("converted %s to %s", self.infile, self.outfile)
            elif self.mode == "deconvert":
                deconvert(self.infile, self.outfile, workpath, dictionaryApi)
                logger.info("deconverted %s to %s", self.infile, self.outfile)
