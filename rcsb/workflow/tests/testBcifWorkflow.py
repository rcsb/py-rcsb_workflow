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
import tempfile
import unittest
from typing import List
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")


def computeBcif(
    listFileBase,
    listFileName,
    outputPath,
    outfileSuffix,
    outputContentType,
    outputHash,
    batch,
    nfiles,
    maxTempFiles,
):
    outContentType = ""
    outHash = ""
    if outputContentType:
        outContentType = "--outputContentType"
    if outputHash:
        outHash = "--outputHash"
    cmd = f"python3 -m rcsb.workflow.cli.BcifExec --batch {batch} --nfiles {nfiles} --maxTempFiles {maxTempFiles} --listFileBase {listFileBase} --listFileName {listFileName} --outputPath {outputPath} --outfileSuffix {outfileSuffix} {outContentType} {outHash}"
    status = os.system(cmd)
    if status == 0:
        return True
    return False


def getList(cifFilePath, outFilePath) -> List[str]:
    if not os.path.exists(cifFilePath):
        raise FileNotFoundError("cifFilePath")
    pdbids = []
    for filename in os.listdir(cifFilePath):
        name = filename.replace(".cif.gz", "").replace(".cif", "").upper()
        pdbids.append(name)
    if len(pdbids) == 0:
        raise ValueError("failed to read pdb ids")
    with open(outFilePath, "w", encoding="utf-8") as w:
        for pdbid in pdbids:
            w.write(pdbid)
            w.write('\n') 
    assert os.path.exists(outFilePath), "error - failed to write %s" % outFilePath
    return pdbids

def splitList(nfiles: int, subtasks: int, tasklist: List[str]) -> List[List[str]]:
    step = nfiles // subtasks
    if step < 1:
        step = 1
    steps = nfiles // step
    logging.info(
        "splitting %d files into %d steps with step %d", nfiles, steps, step
    )
    if not isinstance(tasklist[0], str):
        tasklist = [str(task) for task in tasklist]
    tasks = [
        (
            tasklist[index * step : step + index * step]
            if index < steps - 1
            else tasklist[index * step : nfiles]
        )
        for index in range(0, steps)
    ]
    return tasks

def writeLists(lists, contentType, outfileDir):
    for index in range(0,len(lists)):
        if contentType == "pdb":
            outfileName = "pdbx_core_ids-%d.txt" % (index + 1)
        elif contentType == "csm":
            outfileName = "pdbx_comp_model_core_ids-%d.txt" % (index + 1)
        outfilePath = os.path.join(outfileDir, outfileName)
        with open(outfilePath, "w", encoding="utf-8") as w:
            for pdbid in lists[index]:
                w.write(pdbid)
                w.write('\n')

class TestBcif(unittest.TestCase):

    def setUp(self):
        # paths
        self.outputPath = tempfile.mkdtemp()
        self.listFileBase = tempfile.mkdtemp()
        # local
        self.prereleaseFtpFileBasePath = (
            os.path.abspath(os.path.join(os.path.dirname(__file__), "test-data", "bcif", "pdb"))
        )
        self.csmFileRepoBasePath = (
            os.path.abspath(os.path.join(os.path.dirname(__file__), "test-data", "bcif", "csm"))
        )
        # settings
        self.batch = 0
        self.nfiles = len(os.listdir(self.prereleaseFtpFileBasePath))
        self.maxTempFiles = self.nfiles
        self.outfileSuffix = ".bcif.gz"
        self.outputContentType = False
        self.outputHash = False
        #
        self.nresults = len(os.listdir(self.prereleaseFtpFileBasePath)) + len(os.listdir(self.csmFileRepoBasePath))
        #
        logging.info("making temp dir %s", self.outputPath)
        logging.info("making temp dir %s", self.listFileBase)

    def tearDown(self):
        if os.path.exists(self.listFileBase):
            shutil.rmtree(self.listFileBase)
        if os.path.exists(self.outputPath):
            shutil.rmtree(self.outputPath)

    def test_batch_workflow(self):
        self.batch = 0
        self.nlists = 2

        getList(self.prereleaseFtpFileBasePath, os.path.join(self.listFileBase, "pdbx_core_ids-1.txt"))
        getList(self.csmFileRepoBasePath, os.path.join(self.listFileBase, "pdbx_comp_model_core_ids-1.txt"))

        filepaths = [
            "%s/pdbx_core_ids-*.txt" % self.listFileBase,
            "%s/pdbx_comp_model_core_ids-*.txt" % self.listFileBase,
        ]
        procs = []
        for filepath in filepaths:
            logging.info("reading from %s", filepath)
            for path in glob.glob(filepath):
                if not os.path.exists(path):
                    logging.warning("could not read from %s", path)
                    continue
                listFileName = os.path.basename(path)
                params = (
                    self.listFileBase,
                    listFileName,
                    self.outputPath,
                    self.outfileSuffix,
                    self.outputContentType,
                    self.outputHash,
                    self.batch,
                    self.nfiles,
                    self.maxTempFiles,
                )
                procs.append(multiprocessing.Process(target=computeBcif, args=params))
            logging.info("running %d tasks", len(procs))
            for p in procs:
                p.start()
            for p in procs:
                p.join()
            procs.clear()

        self.assertTrue(len(os.listdir(self.outputPath)) == self.nresults)

        logging.info("bcif file conversion complete")

        logging.info(str(os.listdir(self.outputPath)))

    def test_splitlist_workflow(self):
        self.batch = 1
        self.nlists = 4

        pdblist = getList(self.prereleaseFtpFileBasePath, os.path.join(self.listFileBase, "pdbx_core_ids-1.txt"))
        csmlist = getList(self.csmFileRepoBasePath, os.path.join(self.listFileBase, "pdbx_comp_model_core_ids-1.txt"))

        pdblists = splitList(self.nfiles, self.nlists, pdblist)
        writeLists(pdblists, "pdb", self.listFileBase)
        csmlists = splitList(self.nfiles, self.nlists, csmlist)
        writeLists(csmlists, "csm", self.listFileBase)

        filepaths = [
            "%s/pdbx_core_ids-*.txt" % self.listFileBase,
            "%s/pdbx_comp_model_core_ids-*.txt" % self.listFileBase,
        ]
        procs = []
        for filepath in filepaths:
            logging.info("reading from %s", filepath)
            for path in glob.glob(filepath):
                if not os.path.exists(path):
                    logging.warning("could not read from %s", path)
                    continue
                listFileName = os.path.basename(path)
                params = (
                    self.listFileBase,
                    listFileName,
                    self.outputPath,
                    self.outfileSuffix,
                    self.outputContentType,
                    self.outputHash,
                    self.batch,
                    self.nfiles,
                    self.maxTempFiles,
                )
                procs.append(multiprocessing.Process(target=computeBcif, args=params))
            logging.info("running %d tasks", len(procs))
            for p in procs:
                p.start()
            for p in procs:
                p.join()
            procs.clear()

        self.assertTrue(len(os.listdir(self.outputPath)) == self.nresults)

        logging.info("bcif file conversion complete")

        logging.info(str(os.listdir(self.outputPath)))


if __name__ == "__main__":
    unittest.main()
