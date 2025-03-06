##
# File:    testBcifWorkflow.py
# Author:  James Smith
# Date:    6-Mar-2025
##

"""
Test bcif workflow with python unit test framework.
"""

__docformat__ = "google en"
__author__ = "James Smith"
__email__ = "james.smith@rcsb.org"
__license__ = "Apache 2.0"

import os
import shutil
import multiprocessing
import glob
import unittest
import tempfile
import logging
from rcsb.workflow.bcif.task_functions import (
    statusStart,
    makeDirs,
    localTaskMap,
    validateOutput,
    removeTempFiles,
    removeRetractedEntries,
    tasksDone,
    statusComplete,
)
import pathlib
import datetime
from typing import List
import time
import requests
from mmcif.api.DictionaryApi import DictionaryApi
from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter
from rcsb.utils.io.MarshalUtil import MarshalUtil

logging.basicConfig(level=logging.INFO)

def splitRemoteTaskLists(
    pdbHoldingsFilePath: str,
    csmHoldingsFilePath: str,
    loadFileListDir: str,
    targetFileDir: str,
    incrementalUpdate: bool,
    outfileSuffix: str,
    numSublistFiles: int,
    configPath: str,
    outputContentType: bool,
    outputHash: bool,
) -> bool:
    holdingsFilePath = pdbHoldingsFilePath
    databaseName = "pdbx_core"
    result1 = splitRemoteTaskList(
        loadFileListDir,
        holdingsFilePath,
        targetFileDir,
        databaseName,
        incrementalUpdate,
        outfileSuffix,
        numSublistFiles,
        configPath,
        outputContentType,
        outputHash,
    )
    holdingsFilePath = csmHoldingsFilePath
    databaseName = "pdbx_comp_model_core"
    result2 = splitRemoteTaskList(
        loadFileListDir,
        holdingsFilePath,
        targetFileDir,
        databaseName,
        incrementalUpdate,
        outfileSuffix,
        numSublistFiles,
        configPath,
        outputContentType,
        outputHash,
    )
    if not result1:
        logger.error("exp list failed to load")
    if not result2:
        logger.error("comp list failed to load")
    # enable skipping one or the other
    if result1 or result2:
        return True
    return False


def splitRemoteTaskList(
    loadFileListDir: str,
    holdingsFilePath: str,
    targetFileDir: str,
    databaseName: str,
    incrementalUpdate: bool,
    outfileSuffix: str,
    numSublistFiles: int,
    configPath: str,
    outputContentType: bool,
    outputHash: bool,
) -> bool:
    op = "pdbx_id_list_splitter"
    loadFileListPrefix = databaseName + "_ids"
    if numSublistFiles == 0:
        numSublistFiles = multiprocessing.cpu_count()
    incremental = ""
    outContentType = ""
    outHash = ""
    if incrementalUpdate:
        incremental = "--incremental_update"
    if outputContentType:
        outContentType = "--prepend_output_content_type"
    if outputHash:
        outHash = "--prepend_output_hash"
    cmd = f"python3 -m rcsb.db.cli.RepoLoadExec --op {op} --database {databaseName} --load_file_list_dir {loadFileListDir} --holdings_file_path {holdingsFilePath} --num_sublists {numSublistFiles} {incremental} --target_file_dir {targetFileDir} --target_file_suffix {outfileSuffix} --config_path {configPath} {outContentType} {outHash}"
    status = os.system(cmd)
    if status == 0:
        return True
    return False


def computeBcif(outputPath, listFileBase, tempPath, outputContentType, outputHash, batch, nfiles, maxTempFiles):
    outContentType = ""
    outHash = ""
    if outputContentType:
        outContentType = "--outputContentType"
    if outputHash:
        outHash = "--outputHash"
    cmd = f"python3 -m rcsb.workflow.cli.BcifExec --outputPath {outputPath} --listFileBase {listFileBase} --tempPath {tempPath} {outContentType} {outHash} compute --batch {batch} --nfiles {nfiles} --maxTempFiles {maxTempFiles}"
    status = os.system(cmd)
    if status == 0:
        return True
    return False


def makeTaskListFromLocal(
    localDataPath: str, outputContentType: bool, outputHash: bool
) -> bool:
    """
    requires cif files in source folder with no subdirs
    writes to target folder with no subdirs
    """
    # traverse local folder
    """
    tasklist = glob.glob(os.path.join(localDataPath, "*.cif.gz"))
    nfiles = len(tasklist)
    if nfiles == 0:
        tasklist = glob.glob(os.path.join(localDataPath, "*.cif"))
        nfiles = len(tasklist)
    logger.info("found %d cif files", nfiles)
    """
    return True

class TestBcif(unittest.TestCase):

    def setUp(self):
        self.subtasks = 1
        self.batch = 0
        self.loadType = "incremental"
        self.maxTempFiles = 10
        self.nfiles = 20
        self.outputPath = tempfile.mkdtemp()
        self.tempPath = tempfile.mkdtemp()
        self.inputPath = self.tempPath
        self.listFileBase = self.tempPath
        self.localInputsOrRemote = "remote"
        self.statusStartFile = "status.start"
        self.statusCompleteFile = "status.complete"
        self.missingFileBase = self.tempPath
        self.missingFileName = "missing.txt"
        self.removedFileName = "removed.txt"
        self.outfileSuffix = ".bcif.gz"
        self.configPath = os.path.join(os.path.dirname(__file__), "bcifConfig.yml")
        self.outputContentType = False
        self.outputHash = False
        # from sandbox_config.py/MasterConfig
        self.pdbxDict = "https://raw.githubusercontent.com/wwpdb-dictionaries/mmcif_pdbx/master/dist/mmcif_pdbx_v5_next.dic"
        self.maDict = "https://raw.githubusercontent.com/ihmwg/ModelCIF/master/dist/mmcif_ma_ext.dic"
        self.rcsbDict = "https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/master/dictionary_files/dist/rcsb_mmcif_ext.dic"
        self.prereleaseFtpFileBasePath = "http://prereleaseftp-external-east.rcsb.org/pdb"
        self.pdbIdsTimestampFilePath = "holdings/released_structures_last_modified_dates.json.gz"
        self.csmFileRepoBasePath = "http://computed-models-external-east.rcsb.org/staging"
        self.compModelFileHoldingsList = "holdings/computed-models-holdings-list.json"
        self.structureFilePath = "data/structures/divided/mmCIF/"
        logging.info("making temp dir %s", self.outputPath)
        logging.info("making temp dir %s", self.tempPath)

    def tearDown(self):
        if os.path.exists(self.tempPath):
            shutil.rmtree(self.tempPath)
        if os.path.exists(self.outputPath):
            shutil.rmtree(self.outputPath)

    def test_workflow(self):

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

            self.assertTrue(computeBcif(self.outputPath, self.listFileBase, self.tempPath, self.outputContentType, self.outputHash, self.batch, self.nfiles, self.maxTempFiles))

            self.assertTrue(len(os.listdir(self.outputPath)) == self.nfiles * 2)

            logging.info(str(os.listdir(self.outputPath)))

            self.assertTrue(os.path.exists(os.path.join(self.tempPath, self.statusCompleteFile)))

            logging.info(str(os.listdir(self.tempPath)))

def run_test_suite():
    suite = unittest.TestSuite()
    suite.addTest(TestBcif("test_workflow"))
    return suite

if __name__ == "__main__":
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(run_test_suite())

