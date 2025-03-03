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
import multiprocessing
import glob
import logging
from rcsb.workflow.bcif.task_functions import (
    statusStart,
    makeDirs,
    splitRemoteTaskLists,
    makeTaskListFromLocal,
    localTaskMap,
    validateOutput,
    removeTempFiles,
    removeRetractedEntries,
    tasksDone,
    statusComplete,
)

logger = logging.getLogger(__name__)


class BcifWorkflow:

    def __init__(self, args):

        self.subtasks = None
        self.loadType = None
        self.batch = None
        self.maxTempFiles = None
        self.nfiles = None
        if "subtasks" in args:
            self.subtasks = int(args.subtasks)
        if "loadType" in args:
            self.loadType = args.loadType
        if "batch" in args:
            self.batch = int(args.batch)
        if "maxTempFiles" in args:
            self.maxTempFiles = int(args.maxTempFiles)
        if "nfiles" in args:
            self.nfiles = int(args.nfiles)
        self.outputPath = args.outputPath
        self.tempPath = args.tempPath
        self.inputPath = args.inputPath
        self.listFileBase = args.listFileBase
        self.localInputsOrRemote = args.localInputsOrRemote
        self.statusStartFile = args.statusStartFile
        self.statusCompleteFile = args.statusCompleteFile
        self.missingFileBase = args.missingFileBase
        self.missingFileName = args.missingFileName
        self.removedFileName = args.removedFileName
        self.pdbxDict = args.pdbxDict
        self.maDict = args.maDict
        self.rcsbDict = args.rcsbDict
        self.prereleaseFtpFileBasePath = args.prereleaseFtpFileBasePath
        self.pdbIdsTimestampFilePath = args.pdbIdsTimestampFilePath
        self.csmFileRepoBasePath = args.csmFileRepoBasePath
        self.compModelFileHoldingsList = args.compModelFileHoldingsList
        self.structureFilePath = args.structureFilePath
        self.outfileSuffix = args.outfileSuffix
        self.configPath = args.configPath
        self.outputContentType = bool(args.outputContentType)
        self.outputHash = bool(args.outputHash)

    def logException(self, msg):
        raise RuntimeError("bcif workflow reporting %s" % msg)

    def __call__(self):

        if self.subtasks is not None:
            if not statusStart(self.listFileBase, self.statusStartFile):
                self.logException("status start failed")

            if not makeDirs(self.outputPath, self.outputContentType):
                self.logException("make dirs failed")

            if self.localInputsOrRemote == "remote":

                pdbHoldingsFilePath = os.path.join(
                    self.prereleaseFtpFileBasePath, self.pdbIdsTimestampFilePath
                )
                csmHoldingsFilePath = os.path.join(
                    self.csmFileRepoBasePath, self.compModelFileHoldingsList
                )
                incrementalUpdate = self.loadType == "incremental"
                if not splitRemoteTaskLists(
                    pdbHoldingsFilePath,
                    csmHoldingsFilePath,
                    self.listFileBase,
                    self.outputPath,
                    incrementalUpdate,
                    self.outfileSuffix,
                    self.subtasks,
                    self.configPath,
                    self.outputContentType,
                    self.outputHash,
                ):
                    self.logException("make task list from remote failed")

            elif not makeTaskListFromLocal(self.inputPath):
                self.logException("make task list from local failed")

        elif self.batch is not None:
            fileglobs = [
                "%s/pdbx_core_ids-*.txt" % self.listFileBase,
                "%s/pdbx_comp_model_core_ids-*.txt" % self.listFileBase,
            ]
            procs = []
            # load balancing
            for fileglob in fileglobs:
                logger.info("reading from %s", fileglob)
                for path in glob.glob(fileglob):
                    if not os.path.exists(path):
                        logger.warning("could not read from %s", path)
                        continue
                    params = (
                        path,
                        self.prereleaseFtpFileBasePath,
                        self.csmFileRepoBasePath,
                        self.structureFilePath,
                        self.listFileBase,
                        self.tempPath,
                        self.outputPath,
                        self.outfileSuffix,
                        self.localInputsOrRemote,
                        self.batch,
                        self.nfiles,
                        self.pdbxDict,
                        self.maDict,
                        self.rcsbDict,
                        self.outputContentType,
                        self.outputHash,
                        self.maxTempFiles,
                    )
                    procs.append(
                        multiprocessing.Process(target=localTaskMap, args=params)
                    )
                logger.info("running %d tasks", len(procs))
                for p in procs:
                    p.start()
                for p in procs:
                    p.join()
                procs.clear()

            params = {
                "listFileBase": self.listFileBase,
                "updateBase": self.outputPath,
                "outfileSuffix": self.outfileSuffix,
                "missingFileBase": self.missingFileBase,
                "missingFileName": self.missingFileName,
                "maxFiles": self.nfiles,
                "outputContentType": self.outputContentType,
                "outputHash": self.outputHash,
            }
            if not validateOutput(**params):
                self.logException("validate output failed")

            params = {
                "listFileBase": self.listFileBase,
                "updateBase": self.outputPath,
                "missingFileBase": self.missingFileBase,
                "removedFileName": self.removedFileName,
                "outputContentType": self.outputContentType,
                "outputHash": self.outputHash,
            }

            if not removeRetractedEntries(**params):
                self.logException("remove retracted entries failed")

            if not removeTempFiles(self.tempPath, self.listFileBase):
                self.logException("remove temp files failed")

            if not tasksDone():
                self.logException("tasks done failed")

            if not statusComplete(self.listFileBase, self.statusCompleteFile):
                self.logException("status complete failed")

            missingFile = os.path.join(self.missingFileBase, self.missingFileName)
            removedFile = os.path.join(self.missingFileBase, self.removedFileName)
            logging.info("missing files, if any, were written to %s", missingFile)
            logging.info(
                "removed obsoleted entries, if any, were written to %s", removedFile
            )
