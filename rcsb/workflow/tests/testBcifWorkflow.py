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
import time
from mmcif.api.DictionaryApi import DictionaryApi
from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter
from rcsb.workflow.bcif.task_functions import convert, deconvert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s",
)


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
            w.write("\n")
    assert os.path.exists(outFilePath), "error - failed to write %s" % outFilePath
    return pdbids


def splitList(nfiles: int, subtasks: int, tasklist: List[str]) -> List[List[str]]:
    step = nfiles // subtasks
    if step < 1:
        step = 1
    steps = nfiles // step
    logging.info("splitting %d files into %d steps with step %d", nfiles, steps, step)
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
    for index in range(0, len(lists)):
        if contentType == "pdb":
            outfileName = "pdbx_core_ids-%d.txt" % (index + 1)
        elif contentType == "csm":
            outfileName = "pdbx_comp_model_core_ids-%d.txt" % (index + 1)
        outfilePath = os.path.join(outfileDir, outfileName)
        with open(outfilePath, "w", encoding="utf-8") as w:
            for pdbid in lists[index]:
                w.write(pdbid)
                w.write("\n")


def distributeComputation(
    listFileBase,
    outputPath,
    outfileSuffix,
    outputContentType,
    outputHash,
    batch,
    nfiles,
    maxTempFiles,
):
    filepaths = [
        "%s/pdbx_core_ids-*.txt" % listFileBase,
        "%s/pdbx_comp_model_core_ids-*.txt" % listFileBase,
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
                listFileBase,
                listFileName,
                outputPath,
                outfileSuffix,
                outputContentType,
                outputHash,
                batch,
                nfiles,
                maxTempFiles,
            )
            procs.append(multiprocessing.Process(target=computeBcif, args=params))
        logging.info("running %d tasks", len(procs))
        for p in procs:
            p.start()
        for p in procs:
            p.join()
        procs.clear()


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


def getDictionaryApi():
    # form dictionary object
    pdbxDict = "https://raw.githubusercontent.com/wwpdb-dictionaries/mmcif_pdbx/master/dist/mmcif_pdbx_v5_next.dic"
    maDict = (
        "https://raw.githubusercontent.com/ihmwg/ModelCIF/master/dist/mmcif_ma_ext.dic"
    )
    rcsbDict = "https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/master/dictionary_files/dist/rcsb_mmcif_ext.dic"
    dictionaryApi = None
    paths = [pdbxDict, maDict, rcsbDict]
    try:
        adapter = IoAdapter(raiseExceptions=True)
        containers = []
        for path in paths:
            containers += adapter.readFile(inputFilePath=path)
        dictionaryApi = DictionaryApi(containerList=containers, consolidate=True)
    except Exception as e:
        raise FileNotFoundError("failed to create dictionary api: %s" % str(e))
    return dictionaryApi


class TestBcif(unittest.TestCase):

    def setUp(self):
        # paths
        self.outputPath = tempfile.mkdtemp()
        self.listFileBase = tempfile.mkdtemp()
        # local
        self.prereleaseFtpFileBasePath = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "test-data", "bcif", "pdb")
        )
        self.csmFileRepoBasePath = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "test-data", "bcif", "csm")
        )
        # settings
        self.batch = 0
        self.nlists = 2
        self.nfiles = len(os.listdir(self.prereleaseFtpFileBasePath))
        self.maxTempFiles = self.nfiles
        self.outfileSuffix = ".bcif.gz"
        self.outputContentType = False
        self.outputHash = False
        #
        self.nresults = len(os.listdir(self.prereleaseFtpFileBasePath)) + len(
            os.listdir(self.csmFileRepoBasePath)
        )
        #
        logging.info("making temp dir %s", self.outputPath)
        logging.info("making temp dir %s", self.listFileBase)

    def tearDown(self):
        if os.path.exists(self.listFileBase):
            shutil.rmtree(self.listFileBase)
        if os.path.exists(self.outputPath):
            shutil.rmtree(self.outputPath)

    def test_splitlist_workflow(self):
        t = time.time()

        self.batch = 1
        self.nlists = 4

        pdblist = getList(
            self.prereleaseFtpFileBasePath,
            os.path.join(self.listFileBase, "pdbx_core_ids-1.txt"),
        )
        csmlist = getList(
            self.csmFileRepoBasePath,
            os.path.join(self.listFileBase, "pdbx_comp_model_core_ids-1.txt"),
        )

        pdblists = splitList(self.nfiles, self.nlists, pdblist)
        writeLists(pdblists, "pdb", self.listFileBase)
        csmlists = splitList(self.nfiles, self.nlists, csmlist)
        writeLists(csmlists, "csm", self.listFileBase)

        distributeComputation(
            self.listFileBase,
            self.outputPath,
            self.outfileSuffix,
            self.outputContentType,
            self.outputHash,
            self.batch,
            self.nfiles,
            self.maxTempFiles,
        )

        self.assertTrue(len(os.listdir(self.outputPath)) == self.nresults)

        logging.info("bcif file conversion completed in %.2f s", (time.time() - t))

        logging.info(str(os.listdir(self.outputPath)))

    def test_batch_workflow(self):
        t = time.time()

        self.batch = 0
        self.nlists = 2

        getList(
            self.prereleaseFtpFileBasePath,
            os.path.join(self.listFileBase, "pdbx_core_ids-1.txt"),
        )
        getList(
            self.csmFileRepoBasePath,
            os.path.join(self.listFileBase, "pdbx_comp_model_core_ids-1.txt"),
        )

        distributeComputation(
            self.listFileBase,
            self.outputPath,
            self.outfileSuffix,
            self.outputContentType,
            self.outputHash,
            self.batch,
            self.nfiles,
            self.maxTempFiles,
        )

        self.assertTrue(len(os.listdir(self.outputPath)) == self.nresults)

        logging.info("bcif file conversion completed in %.2f s", (time.time() - t))

        logging.info(str(os.listdir(self.outputPath)))

    def test_deconvert(self):
        infiles = []
        maxfiles = 3
        for filename in os.listdir(self.prereleaseFtpFileBasePath):
            infiles.append(os.path.join(self.prereleaseFtpFileBasePath, filename))
            if len(infiles) >= maxfiles:
                break
        out = tempfile.mkdtemp()
        outfiles = []
        for filename in infiles:
            outfiles.append(
                os.path.join(
                    out, os.path.basename(filename).replace(".cif.gz", ".bcif.gz")
                )
            )
        tmp = tempfile.mkdtemp()
        api = getDictionaryApi()
        for index in range(0, len(infiles)):
            convert(infiles[index], outfiles[index], tmp, api)
        for filename in os.listdir(out):
            filepath = os.path.join(out, filename)
            outfile = os.path.join(out, filename.replace(".bcif.gz", ".cif.gz"))
            deconvert(filepath, outfile, tmp, api)
        self.assertTrue(len(os.listdir(out)) == (maxfiles * 2))
        for bciffile in glob.glob(os.path.join(out, "*.bcif.gz")):
            ciffile = bciffile.replace(".bcif.gz", ".cif.gz")
            self.assertTrue(os.path.exists(ciffile))
        shutil.rmtree(out)
        shutil.rmtree(tmp)
        logging.info("deconverted %d bcif files", maxfiles)


if __name__ == "__main__":
    unittest.main()
