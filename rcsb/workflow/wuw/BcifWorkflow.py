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
from rcsb.workflow.bcif.task_functions import convertCifFilesToBcif

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    fmt="%(asctime)s @%(process)s [%(levelname)s]-%(module)s: %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


class BcifWorkflow:

    def __init__(self, args):

        # settings
        self.batchSize = int(args.batchSize)
        self.nfiles = int(args.nfiles)
        self.outfileSuffix = args.outfileSuffix
        self.contentType = args.contentType
        self.outputContentType = bool(args.outputContentType)
        self.outputHash = bool(args.outputHash)
        self.inputHash = bool(args.inputHash)
        # paths and files
        self.listFileBase = args.listFileBase
        self.listFileName = args.listFileName
        self.remotePath = args.remotePath
        self.outputPath = args.outputPath
        # config
        self.pdbxDict = args.pdbxDict
        self.maDict = args.maDict
        self.rcsbDict = args.rcsbDict
        #
        self.validate()

    # public method required by pylint
    def validate(self):

        assert (
            self.outfileSuffix == ".bcif" or self.outfileSuffix == ".bcif.gz"
        ), "error - require either .bcif or .bcif.gz output file"

        assert self.contentType in [
            "pdb",
            "csm",
            "ihm",
        ], "error - content type must be pdb, csm, or ihm"

    def __call__(self):

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
        )
