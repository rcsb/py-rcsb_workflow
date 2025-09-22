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
import glob
import gzip
import tempfile
import unittest
from typing import List
import logging
import time
from rcsb.workflow.bcif.task_functions import (
    convert,
    deconvert,
    getDictionaryApi,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s",
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

ON_LOCAL_SERVER = False


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


def computeBcif(
    listFileBase,
    listFileName,
    remotePath,
    outputPath,
    outfileSuffix,
    contentType,
    outputContentType,
    outputHash,
    inputHash,
    batchSize,
    nfiles,
) -> bool:
    outContentType = ""
    outHash = ""
    inHash = ""
    if outputContentType:
        outContentType = "--outputContentType"
    if outputHash:
        outHash = "--outputHash"
    if inputHash:
        inHash = "--inputHash"
    options = [
        "python3 -m rcsb.workflow.cli.BcifExec",
        f"--batchSize {batchSize}",
        f"--nfiles {nfiles}",
        f"--listFileBase {listFileBase}",
        f"--listFileName {listFileName}",
        f"--remotePath {remotePath}",
        f"--outputPath {outputPath}",
        f"--outfileSuffix {outfileSuffix}",
        f"--contentType {contentType}",
        f"{outContentType}",
        f"{outHash}",
        f"{inHash}",
    ]
    cmd = " ".join(options)
    time.sleep(3)
    status = os.system(cmd)
    if status == 0:
        return True
    return False


class TestBcif(unittest.TestCase):

    def setUp(self):
        # paths
        self.outputPath = tempfile.mkdtemp()
        self.listFileBase = tempfile.mkdtemp()
        # local
        testPath = os.path.join(
            os.path.dirname(__file__), "../../mock-data/MOCK_CIF_FILES"
        )
        self.pdbLocalPath = os.path.abspath(os.path.join(testPath, "pdb"))
        self.csmLocalPath = os.path.abspath(os.path.join(testPath, "csm"))
        self.ihmLocalPath = os.path.abspath(os.path.join(testPath, "ihm"))
        # remote
        self.pdbRemotePath = "http://prereleaseftp-external-east.rcsb.org/pdb/data/structures/divided/mmCIF/"
        self.csmRemotePath = "http://computed-models-external-east.rcsb.org/staging"
        self.ihmRemotePath = (
            "http://prereleaseftp-external-east.rcsb.org/pdb_ihm/data/entries"
        )
        self.pdbxDict = "https://raw.githubusercontent.com/wwpdb-dictionaries/mmcif_pdbx/master/dist/mmcif_pdbx_v5_next.dic"
        self.maDict = "https://raw.githubusercontent.com/ihmwg/ModelCIF/master/dist/mmcif_ma_ext.dic"
        self.rcsbDict = "https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/master/dictionary_files/dist/rcsb_mmcif_ext.dic"
        self.ihmDict = "https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/master/dictionary_files/reference/mmcif_ihm_ext.dic"
        self.flrDict = "https://raw.githubusercontent.com/ihmwg/flrCIF/refs/heads/master/dist/mmcif_ihm_flr_ext.dic"
        # settings
        self.batchSize = 4
        self.nlists = 2
        self.nfiles = len(os.listdir(self.pdbLocalPath))
        self.outfileSuffix = ".bcif.gz"
        self.outputContentType = False
        self.outputHash = False
        self.inputHash = False
        #
        self.nresults = (
            len(os.listdir(self.pdbLocalPath))
            + len(os.listdir(self.csmLocalPath))
            + len(os.listdir(self.ihmLocalPath))
        )
        #
        logger.info("making temp dir %s", self.outputPath)
        logger.info("making temp dir %s", self.listFileBase)

    def tearDown(self):
        if os.path.exists(self.listFileBase):
            shutil.rmtree(self.listFileBase)
        if os.path.exists(self.outputPath):
            shutil.rmtree(self.outputPath)

    def test_local_workflow(self):
        t = time.time()

        batchSize = 1
        outfileSuffix = ".bcif.gz"

        contentType = "pdb"
        listFileName = "pdbx_core_ids-1.txt"
        pdblist = getList(
            self.pdbLocalPath,
            os.path.join(self.listFileBase, listFileName),
        )
        ok = computeBcif(
            self.listFileBase,
            listFileName,
            self.pdbLocalPath,
            self.outputPath,
            outfileSuffix,
            contentType,
            self.outputContentType,
            self.outputHash,
            self.inputHash,
            batchSize,
            self.nfiles,
        )
        self.assertTrue(ok)

        contentType = "csm"
        listFileName = "pdbx_comp_model_core_ids-1.txt"
        csmlist = getList(
            self.csmLocalPath,
            os.path.join(self.listFileBase, listFileName),
        )
        ok = computeBcif(
            self.listFileBase,
            listFileName,
            self.csmLocalPath,
            self.outputPath,
            outfileSuffix,
            contentType,
            self.outputContentType,
            self.outputHash,
            self.inputHash,
            batchSize,
            self.nfiles,
        )
        self.assertTrue(ok)

        contentType = "ihm"
        listFileName = "pdbx_ihm_ids-1.txt"
        ihmlist = getList(
            self.ihmLocalPath,
            os.path.join(self.listFileBase, listFileName),
        )
        ok = computeBcif(
            self.listFileBase,
            listFileName,
            self.ihmLocalPath,
            self.outputPath,
            outfileSuffix,
            contentType,
            self.outputContentType,
            self.outputHash,
            self.inputHash,
            batchSize,
            self.nfiles,
        )
        self.assertTrue(ok)

        self.assertTrue(len(os.listdir(self.outputPath)) == self.nresults)

        for pdbid in pdblist:
            pdbid = pdbid.lower()
            self.assertTrue(
                os.path.exists(os.path.join(self.outputPath, "%s.bcif.gz" % pdbid))
            )

        for csmid in csmlist:
            csmid = csmid.lower()
            # on mac the following test is case-insensitive and will pass regardless of upper-case/lower-case pdb id
            self.assertTrue(
                os.path.exists(os.path.join(self.outputPath, "%s.bcif.gz" % csmid))
            )

        for filename in os.listdir(self.outputPath):
            filename = filename.replace(".bcif.gz", "").replace(".bcif", "")
            # on mac the following test will fail if this line is commented out
            filename = filename.upper()
            if filename.startswith("AF_") or filename.startswith("MA_"):
                # case-sensitive on any computer
                self.assertTrue(filename in csmlist)

        for ihmid in ihmlist:
            ihmid = ihmid.lower()
            self.assertTrue(
                os.path.exists(os.path.join(self.outputPath, "%s.bcif.gz" % ihmid))
            )

        logger.info("test local workflow completed in %.2f s", (time.time() - t))

        logger.info(str(os.listdir(self.outputPath)))

    @unittest.skipUnless(ON_LOCAL_SERVER, "requires local server authorization")
    def test_remote_workflow(self):
        t = time.time()

        batchSize = 1
        outfileSuffix = ".bcif.gz"

        # get lists for local files but then download them
        listFileName = "pdbx_core_ids-1.txt"
        contentType = "pdb"
        pdblist = getList(
            self.pdbLocalPath, os.path.join(self.listFileBase, listFileName)
        )
        ok = computeBcif(
            self.listFileBase,
            listFileName,
            self.pdbRemotePath,
            self.outputPath,
            outfileSuffix,
            contentType,
            self.outputContentType,
            self.outputHash,
            self.inputHash,
            batchSize,
            self.nfiles,
        )
        self.assertTrue(ok)

        listFileName = "pdbx_comp_model_core_ids-1.txt"
        contentType = "csm"
        csmlist = getList(
            self.csmLocalPath, os.path.join(self.listFileBase, listFileName)
        )
        ok = computeBcif(
            self.listFileBase,
            listFileName,
            self.csmRemotePath,
            self.outputPath,
            outfileSuffix,
            contentType,
            self.outputContentType,
            self.outputHash,
            self.inputHash,
            batchSize,
            self.nfiles,
        )
        self.assertTrue(ok)

        listFileName = "pdbx_ihm_ids-1.txt"
        contentType = "ihm"
        ihmlist = getList(
            self.ihmLocalPath, os.path.join(self.listFileBase, listFileName)
        )
        ok = computeBcif(
            self.listFileBase,
            listFileName,
            self.ihmRemotePath,
            self.outputPath,
            outfileSuffix,
            contentType,
            self.outputContentType,
            self.outputHash,
            self.inputHash,
            batchSize,
            self.nfiles,
        )
        self.assertTrue(ok)

        self.assertTrue(len(os.listdir(self.outputPath)) == self.nresults)

        for pdbid in pdblist:
            pdbid = pdbid.lower()
            self.assertTrue(
                os.path.exists(os.path.join(self.outputPath, "%s.bcif.gz" % pdbid))
            )

        for csmid in csmlist:
            csmid = csmid.lower()
            self.assertTrue(
                os.path.exists(os.path.join(self.outputPath, "%s.bcif.gz" % csmid))
            )

        for ihmid in ihmlist:
            ihmid = ihmid.lower()
            self.assertTrue(
                os.path.exists(os.path.join(self.outputPath, "%s.bcif.gz" % ihmid))
            )

        logger.info("test remote workflow completed in %.2f s", (time.time() - t))

        logger.info(str(os.listdir(self.outputPath)))

    def test_expanded_files(self):
        t = time.time()

        batchSize = 1
        outfileSuffix = ".bcif"

        pdbdir = tempfile.mkdtemp()
        csmdir = tempfile.mkdtemp()
        ihmdir = tempfile.mkdtemp()
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
        for filename in os.listdir(self.ihmLocalPath):
            filepath = os.path.join(self.ihmLocalPath, filename)
            outpath = os.path.join(ihmdir, filename.replace(".gz", ""))
            with gzip.open(filepath, "rb") as r:
                with open(outpath, "wb") as w:
                    shutil.copyfileobj(r, w)

        contentType = "pdb"
        listFileName = "pdbx_core_ids-1.txt"
        pdblist = getList(
            pdbdir,
            os.path.join(self.listFileBase, listFileName),
        )
        ok = computeBcif(
            self.listFileBase,
            listFileName,
            pdbdir,
            self.outputPath,
            outfileSuffix,
            contentType,
            self.outputContentType,
            self.outputHash,
            self.inputHash,
            batchSize,
            self.nfiles,
        )
        self.assertTrue(ok)

        contentType = "csm"
        listFileName = "pdbx_comp_model_core_ids-1.txt"
        csmlist = getList(
            csmdir,
            os.path.join(self.listFileBase, listFileName),
        )
        ok = computeBcif(
            self.listFileBase,
            listFileName,
            csmdir,
            self.outputPath,
            outfileSuffix,
            contentType,
            self.outputContentType,
            self.outputHash,
            self.inputHash,
            batchSize,
            self.nfiles,
        )
        self.assertTrue(ok)

        contentType = "ihm"
        listFileName = "pdbx_ihm_ids-1.txt"
        ihmlist = getList(
            ihmdir,
            os.path.join(self.listFileBase, listFileName),
        )
        ok = computeBcif(
            self.listFileBase,
            listFileName,
            ihmdir,
            self.outputPath,
            outfileSuffix,
            contentType,
            self.outputContentType,
            self.outputHash,
            self.inputHash,
            batchSize,
            self.nfiles,
        )
        self.assertTrue(ok)

        self.assertTrue(len(os.listdir(self.outputPath)) == self.nresults)

        for pdbid in pdblist:
            pdbid = pdbid.lower()
            self.assertTrue(
                os.path.exists(os.path.join(self.outputPath, "%s.bcif" % pdbid))
            )

        for csmid in csmlist:
            csmid = csmid.lower()
            self.assertTrue(
                os.path.exists(os.path.join(self.outputPath, "%s.bcif" % csmid))
            )

        for ihmid in ihmlist:
            ihmid = ihmid.lower()
            self.assertTrue(
                os.path.exists(os.path.join(self.outputPath, "%s.bcif" % ihmid))
            )

        logger.info("test expanded files completed in %.2f s", (time.time() - t))

        logger.info(str(os.listdir(self.outputPath)))

        shutil.rmtree(pdbdir)
        shutil.rmtree(csmdir)
        shutil.rmtree(ihmdir)

    def test_hashed_storage(self):
        t = time.time()

        batchSize = 1
        outfileSuffix = ".bcif.gz"
        outputContentType = True
        outputHash = True

        contentType = "pdb"
        listFileName = "pdbx_core_ids-1.txt"
        pdblist = getList(
            self.pdbLocalPath,
            os.path.join(self.listFileBase, listFileName),
        )
        ok = computeBcif(
            self.listFileBase,
            listFileName,
            self.pdbLocalPath,
            self.outputPath,
            outfileSuffix,
            contentType,
            outputContentType,
            outputHash,
            self.inputHash,
            batchSize,
            self.nfiles,
        )
        self.assertTrue(ok)

        contentType = "csm"
        listFileName = "pdbx_comp_model_core_ids-1.txt"
        csmlist = getList(
            self.csmLocalPath,
            os.path.join(self.listFileBase, listFileName),
        )
        ok = computeBcif(
            self.listFileBase,
            listFileName,
            self.csmLocalPath,
            self.outputPath,
            outfileSuffix,
            contentType,
            outputContentType,
            outputHash,
            self.inputHash,
            batchSize,
            self.nfiles,
        )
        self.assertTrue(ok)

        contentType = "ihm"
        listFileName = "pdbx_ihm_ids-1.txt"
        ihmlist = getList(
            self.ihmLocalPath,
            os.path.join(self.listFileBase, listFileName),
        )
        ok = computeBcif(
            self.listFileBase,
            listFileName,
            self.ihmLocalPath,
            self.outputPath,
            outfileSuffix,
            contentType,
            outputContentType,
            outputHash,
            self.inputHash,
            batchSize,
            self.nfiles,
        )
        self.assertTrue(ok)

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
            csmid = csmid.lower()
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

        for ihmid in ihmlist:
            ihmid = ihmid.lower()
            self.assertTrue(
                os.path.exists(
                    os.path.join(
                        self.outputPath,
                        "pdb",
                        ihmid[1:3],
                        "%s.bcif.gz" % ihmid,
                    )
                )
            )

        logger.info(str(os.listdir(self.outputPath)))

        for root, _, files in os.walk(self.outputPath):
            logger.info("%s %s", root, _)
            for f in files:
                logger.info(f)

        logger.info("test hashed storage completed in %.2f s", (time.time() - t))

    def test_batch_workflow(self):
        t = time.time()

        batchSize = 4
        outfileSuffix = ".bcif.gz"

        contentType = "pdb"
        listFileName = "pdbx_core_ids-1.txt"
        pdblist = getList(
            self.pdbLocalPath,
            os.path.join(self.listFileBase, listFileName),
        )
        ok = computeBcif(
            self.listFileBase,
            listFileName,
            self.pdbLocalPath,
            self.outputPath,
            outfileSuffix,
            contentType,
            self.outputContentType,
            self.outputHash,
            self.inputHash,
            batchSize,
            self.nfiles,
        )
        self.assertTrue(ok)

        contentType = "csm"
        listFileName = "pdbx_comp_model_core_ids-1.txt"
        csmlist = getList(
            self.csmLocalPath,
            os.path.join(self.listFileBase, listFileName),
        )
        ok = computeBcif(
            self.listFileBase,
            listFileName,
            self.csmLocalPath,
            self.outputPath,
            outfileSuffix,
            contentType,
            self.outputContentType,
            self.outputHash,
            self.inputHash,
            batchSize,
            self.nfiles,
        )
        self.assertTrue(ok)

        contentType = "ihm"
        listFileName = "pdbx_ihm_ids-1.txt"
        ihmlist = getList(
            self.ihmLocalPath,
            os.path.join(self.listFileBase, listFileName),
        )
        ok = computeBcif(
            self.listFileBase,
            listFileName,
            self.ihmLocalPath,
            self.outputPath,
            outfileSuffix,
            contentType,
            self.outputContentType,
            self.outputHash,
            self.inputHash,
            batchSize,
            self.nfiles,
        )
        self.assertTrue(ok)

        self.assertTrue(len(os.listdir(self.outputPath)) == self.nresults)

        for pdbid in pdblist:
            pdbid = pdbid.lower()
            self.assertTrue(
                os.path.exists(os.path.join(self.outputPath, "%s.bcif.gz" % pdbid))
            )

        for csmid in csmlist:
            csmid = csmid.lower()
            self.assertTrue(
                os.path.exists(os.path.join(self.outputPath, "%s.bcif.gz" % csmid))
            )

        for ihmid in ihmlist:
            ihmid = ihmid.lower()
            self.assertTrue(
                os.path.exists(os.path.join(self.outputPath, "%s.bcif.gz" % ihmid))
            )

        logger.info("test batch workflow completed in %.2f s", (time.time() - t))

        logger.info(str(os.listdir(self.outputPath)))

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
        api = getDictionaryApi(
            self.pdbxDict, self.maDict, self.rcsbDict, self.ihmDict, self.flrDict
        )
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
        logger.info("deconverted %d bcif files", maxfiles)


if __name__ == "__main__":
    unittest.main()
