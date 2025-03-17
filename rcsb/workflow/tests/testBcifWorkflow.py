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
import gzip
import tempfile
import unittest
from typing import List
from itertools import chain
import logging
import time
import requests  # noqa: F401 pylint: disable=W0611
from mmcif.api.DictionaryApi import DictionaryApi
from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter
from rcsb.workflow.bcif.task_functions import convert, deconvert, splitList


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s",
)


pdbTestPath = "http://prereleaseftp-external-east.rcsb.org/pdb/data/structures/divided/mmCIF/00/100d.cif.gz"


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


def distributeComputation(
    listFileBase,
    pdbLocalPath,
    csmLocalPath,
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
            remotePath = pdbLocalPath
            if listFileName.startswith("pdbx_comp_model"):
                remotePath = csmLocalPath
            params = (
                listFileBase,
                listFileName,
                remotePath,
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
    remotePath,
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
    options = [
        "python3 -m rcsb.workflow.cli.BcifExec",
        f"--batch {batch}",
        f"--nfiles {nfiles}",
        f"--maxTempFiles {maxTempFiles}",
        f"--listFileBase {listFileBase}",
        f"--listFileName {listFileName}",
        f"--remotePath {remotePath}",
        f"--outputPath {outputPath}",
        f"--outfileSuffix {outfileSuffix}",
        f"{outContentType}",
        f"{outHash}",
    ]
    cmd = " ".join(options)
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
        raise FileNotFoundError("failed to create dictionary api: %s" % str(e)) from e
    return dictionaryApi


class TestBcif(unittest.TestCase):

    def setUp(self):
        # paths
        self.outputPath = tempfile.mkdtemp()
        self.listFileBase = tempfile.mkdtemp()
        # local
        self.pdbLocalPath = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "test-data", "bcif", "pdb")
        )
        self.csmLocalPath = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "test-data", "bcif", "csm")
        )
        self.pdbFullListFile = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__), "test-data", "bcif", "pdbx_core_ids-1.txt"
            )
        )
        # remote (from sandbox_config.py/MasterConfig)
        self.pdbRemotePath = "http://prereleaseftp-external-east.rcsb.org/pdb/data/structures/divided/mmCIF/"
        self.csmRemotePath = "http://computed-models-external-east.rcsb.org/staging"
        self.pdbIdsTimestampFilePath = (
            "holdings/released_structures_last_modified_dates.json.gz"
        )
        self.compModelFileHoldingsList = "holdings/computed-models-holdings-list.json"
        # settings
        self.batch = 0
        self.nlists = 2
        self.nfiles = len(os.listdir(self.pdbLocalPath))
        self.maxTempFiles = self.nfiles
        self.outfileSuffix = ".bcif.gz"
        self.outputContentType = False
        self.outputHash = False
        #
        self.nresults = len(os.listdir(self.pdbLocalPath)) + len(
            os.listdir(self.csmLocalPath)
        )
        #
        logging.info("making temp dir %s", self.outputPath)
        logging.info("making temp dir %s", self.listFileBase)

    def tearDown(self):
        if os.path.exists(self.listFileBase):
            shutil.rmtree(self.listFileBase)
        if os.path.exists(self.outputPath):
            shutil.rmtree(self.outputPath)

    def test_local_workflow(self):
        t = time.time()

        batch = 1
        outfileSuffix = ".bcif.gz"

        pdblist = getList(
            self.pdbLocalPath,
            os.path.join(self.listFileBase, "pdbx_core_ids-1.txt"),
        )
        csmlist = getList(
            self.csmLocalPath,
            os.path.join(self.listFileBase, "pdbx_comp_model_core_ids-1.txt"),
        )

        distributeComputation(
            self.listFileBase,
            self.pdbLocalPath,
            self.csmLocalPath,
            self.outputPath,
            outfileSuffix,
            self.outputContentType,
            self.outputHash,
            batch,
            self.nfiles,
            self.maxTempFiles,
        )

        self.assertTrue(len(os.listdir(self.outputPath)) == self.nresults)

        for pdbid in pdblist:
            pdbid = pdbid.lower()
            self.assertTrue(
                os.path.exists(os.path.join(self.outputPath, "%s.bcif.gz" % pdbid))
            )

        for csmid in csmlist:
            csmid = csmid.upper()
            self.assertTrue(
                os.path.exists(os.path.join(self.outputPath, "%s.bcif.gz" % csmid))
            )

        logging.info("test local workflow completed in %.2f s", (time.time() - t))

        logging.info(str(os.listdir(self.outputPath)))

    @unittest.skip("requires local server authorization")
    def test_remote_workflow(self):
        t = time.time()

        batch = 1
        outfileSuffix = ".bcif.gz"

        # get lists for local files but then download them
        pdblist = getList(
            self.pdbLocalPath,
            os.path.join(self.listFileBase, "pdbx_core_ids-1.txt"),
        )
        csmlist = getList(
            self.csmLocalPath,
            os.path.join(self.listFileBase, "pdbx_comp_model_core_ids-1.txt"),
        )

        distributeComputation(
            self.listFileBase,
            self.pdbRemotePath,
            self.csmRemotePath,
            self.outputPath,
            outfileSuffix,
            self.outputContentType,
            self.outputHash,
            batch,
            self.nfiles,
            self.maxTempFiles,
        )

        self.assertTrue(len(os.listdir(self.outputPath)) == self.nresults)

        for pdbid in pdblist:
            pdbid = pdbid.lower()
            self.assertTrue(
                os.path.exists(os.path.join(self.outputPath, "%s.bcif.gz" % pdbid))
            )

        for csmid in csmlist:
            csmid = csmid.upper()
            self.assertTrue(
                os.path.exists(os.path.join(self.outputPath, "%s.bcif.gz" % csmid))
            )

        logging.info("test remote workflow completed in %.2f s", (time.time() - t))

        logging.info(str(os.listdir(self.outputPath)))

    def test_expanded_files(self):
        t = time.time()

        batch = 1
        outfileSuffix = ".bcif"

        pdbdir = tempfile.mkdtemp()
        csmdir = tempfile.mkdtemp()
        for filename in os.listdir(self.pdbLocalPath):
            filepath = os.path.join(self.pdbLocalPath, filename)
            outpath = os.path.join(pdbdir, filename.replace(".gz", ""))
            with gzip.open(filepath, "rb") as r:
                with open(outpath, "wb") as w:
                    shutil.copyfileobj(r, w)
        for filename in os.listdir(self.csmLocalPath):
            filepath = os.path.join(self.csmLocalPath, filename)
            outpath = os.path.join(csmdir, filename.replace(".gz", ""))
            with gzip.open(filepath, "rb") as r:
                with open(outpath, "wb") as w:
                    shutil.copyfileobj(r, w)

        pdblist = getList(
            pdbdir,
            os.path.join(self.listFileBase, "pdbx_core_ids-1.txt"),
        )
        csmlist = getList(
            csmdir,
            os.path.join(self.listFileBase, "pdbx_comp_model_core_ids-1.txt"),
        )

        distributeComputation(
            self.listFileBase,
            pdbdir,
            csmdir,
            self.outputPath,
            outfileSuffix,
            self.outputContentType,
            self.outputHash,
            batch,
            self.nfiles,
            self.maxTempFiles,
        )

        self.assertTrue(len(os.listdir(self.outputPath)) == self.nresults)

        for pdbid in pdblist:
            pdbid = pdbid.lower()
            self.assertTrue(
                os.path.exists(os.path.join(self.outputPath, "%s.bcif" % pdbid))
            )

        for csmid in csmlist:
            csmid = csmid.upper()
            self.assertTrue(
                os.path.exists(os.path.join(self.outputPath, "%s.bcif" % csmid))
            )

        logging.info("test expanded files completed in %.2f s", (time.time() - t))

        logging.info(str(os.listdir(self.outputPath)))

        shutil.rmtree(pdbdir)
        shutil.rmtree(csmdir)

    def test_hashed_storage(self):
        t = time.time()

        batch = 1
        outfileSuffix = ".bcif.gz"
        outputContentType = True
        outputHash = True

        pdblist = getList(
            self.pdbLocalPath,
            os.path.join(self.listFileBase, "pdbx_core_ids-1.txt"),
        )
        csmlist = getList(
            self.csmLocalPath,
            os.path.join(self.listFileBase, "pdbx_comp_model_core_ids-1.txt"),
        )

        distributeComputation(
            self.listFileBase,
            self.pdbLocalPath,
            self.csmLocalPath,
            self.outputPath,
            outfileSuffix,
            outputContentType,
            outputHash,
            batch,
            self.nfiles,
            self.maxTempFiles,
        )

        self.assertFalse(len(os.listdir(self.outputPath)) == self.nresults)

        for pdbid in pdblist:
            pdbid = pdbid.lower()
            self.assertTrue(
                os.path.exists(
                    os.path.join(
                        self.outputPath, "pdb", pdbid[1:3], "%s.bcif.gz" % pdbid
                    )
                )
            )

        for csmid in csmlist:
            csmid = csmid.upper()
            self.assertTrue(
                os.path.exists(
                    os.path.join(
                        self.outputPath,
                        "csm",
                        csmid[0:2],
                        csmid[-6:-4],
                        csmid[-4:-2],
                        "%s.bcif.gz" % csmid,
                    )
                )
            )

        logging.info(str(os.listdir(self.outputPath)))

        for root, _, files in os.walk(self.outputPath):
            logging.info("%s %s", root, _)
            for f in files:
                logging.info(f)

        logging.info("test hashed storage completed in %.2f s", (time.time() - t))

    def test_batch_workflow(self):
        t = time.time()

        batch = 0
        outfileSuffix = ".bcif.gz"

        pdblist = getList(
            self.pdbLocalPath,
            os.path.join(self.listFileBase, "pdbx_core_ids-1.txt"),
        )
        csmlist = getList(
            self.csmLocalPath,
            os.path.join(self.listFileBase, "pdbx_comp_model_core_ids-1.txt"),
        )

        distributeComputation(
            self.listFileBase,
            self.pdbLocalPath,
            self.csmLocalPath,
            self.outputPath,
            outfileSuffix,
            self.outputContentType,
            self.outputHash,
            batch,
            self.nfiles,
            self.maxTempFiles,
        )

        self.assertTrue(len(os.listdir(self.outputPath)) == self.nresults)

        for pdbid in pdblist:
            pdbid = pdbid.lower()
            self.assertTrue(
                os.path.exists(os.path.join(self.outputPath, "%s.bcif.gz" % pdbid))
            )

        for csmid in csmlist:
            csmid = csmid.upper()
            self.assertTrue(
                os.path.exists(os.path.join(self.outputPath, "%s.bcif.gz" % csmid))
            )

        logging.info("test batch workflow completed in %.2f s", (time.time() - t))

        logging.info(str(os.listdir(self.outputPath)))

    def test_split_list(self):
        # read list of 230000
        entries = []
        r = open(self.pdbFullListFile, "r", encoding="utf-8")
        for line in r:
            entries.append(line.strip())
        r.close()
        print("%d entries" % len(entries))

        # split into 4 lists
        nfiles = len(entries)
        subtasks = 4
        sublists = splitList(nfiles, subtasks, entries)
        print("%d sublists" % len(sublists))

        # count results
        flatlist = list(chain(*sublists))
        resultcount = len(flatlist)
        print("%d results" % resultcount)
        assert resultcount == nfiles, "error - %d results %d files" % (
            resultcount,
            nfiles,
        )

        # verify unique
        resultset = set(flatlist)
        print("%d unique results" % len(resultset))
        assert len(resultset) == nfiles, "error - %d unique results %d files" % (
            len(resultset),
            nfiles,
        )

    def test_deconvert(self):
        infiles = []
        maxfiles = 3
        for filename in os.listdir(self.pdbLocalPath):
            infiles.append(os.path.join(self.pdbLocalPath, filename))
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
