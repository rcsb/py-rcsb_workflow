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

from rcsb.workflow.bcif.task_functions import (
    statusStart,
    makeDirs,
    getPdbList,
    getCsmList,
    makeTaskListFromRemote,
    makeTaskListFromLocal,
    splitTasks,
    localTaskMap,
    validateOutput,
    removeTempFiles,
    tasksDone,
    statusComplete,
)
from rcsb.workflow.bcif.bcif_workflow_utilities import BcifWorkflowUtilities


class BcifWorkflow:

    def __init__(self, args):

        self.nfiles = int(args.nfiles)
        self.outputPath = args.outputPath
        self.tempPath = args.tempPath
        self.inputPath = args.inputPath
        self.subtasks = int(args.subtasks)
        self.batch = int(args.batch)
        self.localInputsOrRemote = args.localInputsOrRemote
        self.loadType = args.loadType
        self.listFileBase = args.listFileBase
        self.pdbListFileName = args.pdbListFileName
        self.csmListFileName = args.csmListFileName
        self.inputListFileName = args.inputListFileName
        self.inputList2d = args.inputList2d
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

        workflowUtility = BcifWorkflowUtilities(
            updateBase=self.outputPath,
            tempPath=self.tempPath,
            prereleaseFtpFileBasePath=self.prereleaseFtpFileBasePath,
            pdbIdsTimestampFilePath=self.pdbIdsTimestampFilePath,
            csmFileRepoBasePath=self.csmFileRepoBasePath,
            csmHoldingsUrl=self.csmHoldingsUrl,
            structureFilePath=self.structureFilePath,
        )

        if not makeDirs(workflowUtility):
            self.logException("make dirs failed")

        if self.localInputsOrRemote == "remote":

            if (
                not getPdbList(
                    workflowUtility,
                    self.loadType,
                    self.listFileBase,
                    self.pdbListFileName,
                )
                or not getCsmList(
                    workflowUtility,
                    self.loadType,
                    self.listFileBase,
                    self.csmListFileName,
                )
                or not makeTaskListFromRemote(
                    self.listFileBase,
                    self.pdbListFileName,
                    self.csmListFileName,
                    self.inputListFileName,
                    self.nfiles,
                    workflowUtility,
                )
            ):
                self.logException("make task list from remote failed")

        elif not makeTaskListFromLocal(
            self.inputPath, self.listFileBase, self.inputListFileName
        ):
            self.logException("make task list from local failed")

        if not splitTasks(
            self.listFileBase,
            self.inputListFileName,
            self.inputList2d,
            self.nfiles,
            self.subtasks,
        ):
            self.logException("split tasks failed")

        index = 0
        params = {
            "listFileBase": self.listFileBase,
            "inputList2d": self.inputList2d,
            "tempPath": self.tempPath,
            "updateBase": self.outputPath,
            "compress": self.compress,
            "localInputsOrRemote": self.localInputsOrRemote,
            "batch": self.batch,
            "pdbxDict": self.pdbxDict,
            "maDict": self.maDict,
            "rcsbDict": self.rcsbDict,
            "workflowUtility": workflowUtility,
        }
        if not localTaskMap(index, **params):
            self.logException("local task map failed")

        params = {
            "listFileBase": self.listFileBase,
            "inputListFileName": self.inputListFileName,
            "updateBase": self.outputPath,
            "compress": self.compress,
            "missingFileBase": self.missingFileBase,
            "missingFileName": self.missingFileName,
            "workflowUtility": workflowUtility,
        }
        if not validateOutput(**params):
            self.logException("validate output failed")

        if not removeTempFiles(self.tempPath, self.listFileBase):
            self.logException("remove temp files failed")

        if not tasksDone():
            self.logException("tasks done failed")

        if not statusComplete(self.listFileBase, self.statusCompleteFile):
            self.logException("status complete failed")
