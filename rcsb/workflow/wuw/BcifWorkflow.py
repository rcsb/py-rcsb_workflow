##
# File:    BcifWorkflow.py
# Author:  James Smith
# Date:    21-Feb-2025
##

"""
Workflow middleware at intersection of command line interface and task functions.
"""

__docformat__ = "google en"
__author__ = "James Smith"
__email__ = "james.smith@rcsb.org"
__license__ = "Apache 2.0"

import os
from rcsb.workflow.bcif.task_functions import (
    statusStart,
    makeDirs,
    splitRemoteTaskLists,
    makeTaskListFromLocal,
    localTaskMap,
    validateOutput,
    removeTempFiles,
    tasksDone,
    statusComplete,
)


class BcifWorkflow:

    def __init__(self, args):

        self.nfiles = int(args.nfiles)
        self.outputPath = args.outputPath
        self.tempPath = args.tempPath
        self.inputPath = args.inputPath
        self.listFileBase = args.listFileBase
        self.subtasks = int(args.subtasks)
        self.batch = int(args.batch)
        self.localInputsOrRemote = args.localInputsOrRemote
        self.loadType = args.loadType
        self.statusStartFile = args.statusStartFile
        self.statusCompleteFile = args.statusCompleteFile
        self.missingFileBase = args.missingFileBase
        self.missingFileName = args.missingFileName
        self.pdbxDict = args.pdbxDict
        self.maDict = args.maDict
        self.rcsbDict = args.rcsbDict
        self.prereleaseFtpFileBasePath = args.prereleaseFtpFileBasePath
        self.pdbIdsTimestampFilePath = args.pdbIdsTimestampFilePath
        self.csmFileRepoBasePath = args.csmFileRepoBasePath
        self.csmHoldingsUrl = args.csmHoldingsUrl
        self.structureFilePath = args.structureFilePath
        self.compress = bool(args.compress)

    def logException(self, msg):
        raise RuntimeError("bcif workflow reporting %s" % msg)

    def __call__(self):

        if not statusStart(self.listFileBase, self.statusStartFile):
            self.logException("status start failed")

        if not makeDirs(self.outputPath):
            self.logException("make dirs failed")

        if self.localInputsOrRemote == "remote":

            pdbHoldingsFilePath = os.path.join(
                self.prereleaseFtpFileBasePath, self.pdbIdsTimestampFilePath
            )
            csmHoldingsFilePath = os.path.join(
                self.csmFileRepoBasePath, self.csmHoldingsUrl
            )
            incrementalUpdate = self.loadType == "incremental"
            if not splitRemoteTaskLists(
                pdbHoldingsFilePath,
                csmHoldingsFilePath,
                self.listFileBase,
                self.tempPath,
                self.outputPath,
                incrementalUpdate,
                self.compress,
                self.subtasks,
            ):
                self.logException("make task list from remote failed")

        elif not makeTaskListFromLocal(self.inputPath):
            self.logException("make task list from local failed")

        index = 0
        params = {
            "prereleaseFtpFileBasePath": self.prereleaseFtpFileBasePath,
            "structureFilePath": self.structureFilePath,
            "listFileBase": self.listFileBase,
            "tempPath": self.tempPath,
            "updateBase": self.outputPath,
            "compress": self.compress,
            "localInputsOrRemote": self.localInputsOrRemote,
            "batch": self.batch,
            "maxFiles": self.nfiles,
            "pdbxDict": self.pdbxDict,
            "maDict": self.maDict,
            "rcsbDict": self.rcsbDict,
        }
        if not localTaskMap(index, **params):
            self.logException("local task map failed")

        params = {
            "listFileBase": self.listFileBase,
            "updateBase": self.outputPath,
            "compress": self.compress,
            "missingFileBase": self.missingFileBase,
            "missingFileName": self.missingFileName,
            "maxFiles": self.nfiles,
        }
        if not validateOutput(**params):
            self.logException("validate output failed")

        if not removeTempFiles(self.tempPath, self.listFileBase):
            self.logException("remove temp files failed")

        if not tasksDone():
            self.logException("tasks done failed")

        if not statusComplete(self.listFileBase, self.statusCompleteFile):
            self.logException("status complete failed")
