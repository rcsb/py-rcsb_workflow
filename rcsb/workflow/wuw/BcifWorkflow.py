##
# File:    BcifWorkflow.py
# Author:  James Smith
# Date:    21-Feb-2025
##

"""
Run workflow for tasks requested on command line.
"""

__docformat__ = "google en"
__author__ = "James Smith"
__email__ = "james.smith@rcsb.org"
__license__ = "Apache 2.0"

import os
import shutil
import tempfile
import logging
from rcsb.workflow.bcif.task_functions import localTaskMap

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter(fmt="%(asctime)s @%(process)s [%(levelname)s]-%(module)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


class BcifWorkflow:

    def __init__(self, args):

        # settings
        self.batch = int(args.batch)
        self.nfiles = int(args.nfiles)
        self.maxTempFiles = int(args.maxTempFiles)
        self.outfileSuffix = args.outfileSuffix
        self.outputContentType = bool(args.outputContentType)
        self.outputHash = bool(args.outputHash)
        # paths and files
        self.outputPath = args.outputPath
        self.tempPath = tempfile.mkdtemp()
        self.listFileBase = args.listFileBase
        self.listFileName = args.listFileName
        self.missingFileBase = args.missingFileBase
        # config
        self.pdbxDict = args.pdbxDict
        self.maDict = args.maDict
        self.rcsbDict = args.rcsbDict
        self.prereleaseFtpFileBasePath = args.prereleaseFtpFileBasePath
        self.pdbIdsTimestampFilePath = args.pdbIdsTimestampFilePath
        self.csmFileRepoBasePath = args.csmFileRepoBasePath
        self.compModelFileHoldingsList = args.compModelFileHoldingsList
        self.structureFilePath = args.structureFilePath

    def report(self, msg):
        logger.info(msg)

    def __call__(self):

        localTaskMap(
            self.listFileBase,
            self.listFileName,
            self.prereleaseFtpFileBasePath,
            self.csmFileRepoBasePath,
            self.structureFilePath,
            self.tempPath,
            self.outputPath,
            self.outfileSuffix,
            self.batch,
            self.nfiles,
            self.pdbxDict,
            self.maDict,
            self.rcsbDict,
            self.outputContentType,
            self.outputHash,
            self.maxTempFiles,
        )

        if os.path.exists(self.tempPath) and os.path.isdir(self.tempPath):
            shutil.rmtree(self.tempPath)

        missingFile = os.path.join(self.missingFileBase)
        removedFile = os.path.join(self.missingFileBase)
        self.report("missing files, if any, were written to %s" % missingFile)
        self.report(
            "removed obsoleted entries, if any, were written to %s" % removedFile
        )
