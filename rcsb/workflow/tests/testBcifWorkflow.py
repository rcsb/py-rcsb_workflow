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
import pathlib
import datetime
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")


def statusStart(listFileBase: str) -> bool:
    startFile = os.path.join(listFileBase, "status.start")
    dirs = os.path.dirname(startFile)
    if not os.path.exists(dirs):
        os.makedirs(dirs, mode=0o777)
    with open(startFile, "w", encoding="utf-8") as w:
        w.write("Binary cif run started at %s." % str(datetime.datetime.now()))
    return True


def makeDirs(listFileBase: str, updateBase: str, outputContentType: bool) -> bool:
    try:
        if not os.path.exists(listFileBase):
            os.mkdir(listFileBase)
            os.chmod(listFileBase, 0o777)
        if not os.path.exists(updateBase):
            os.mkdir(updateBase)
            os.chmod(updateBase, 0o777)
        if outputContentType:
            for contentType in ["pdb", "csm"]:
                path = os.path.join(updateBase, contentType)
                if not os.path.exists(path):
                    os.mkdir(path)
                    os.chmod(path, 0o777)
    except Exception as e:
        raise Exception(str(e))
    return True


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
        logging.error("exp list failed to load")
    if not result2:
        logging.error("comp list failed to load")
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


def computeBcif(
    listFileBase,
    listFileName,
    outputPath,
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
    cmd = f"python3 -m rcsb.workflow.cli.BcifExec --batch {batch} --nfiles {nfiles} --maxTempFiles {maxTempFiles} --listFileBase {listFileBase} --listFileName {listFileName} --outputPath {outputPath} {outContentType} {outHash}"
    status = os.system(cmd)
    if status == 0:
        return True
    return False


def validateOutput(
    *,
    listFileBase: str,
    updateBase: str,
    outfileSuffix: str,
    missingFileBase: str,
    maxFiles: int,
    outputContentType: bool,
    outputHash: bool,
) -> bool:
    missing = []
    for path in glob.glob(os.path.join(listFileBase, "*core_ids*.txt")):
        count = 0
        f = open(path, "r", encoding="utf-8")
        for line in f:
            count += 1
            if count > maxFiles:
                break
            pdbId = line.strip()
            # list files have upper case for all model types
            # experimental models stored with lower case file name and hash
            if path.find("comp_model") < 0:
                pdbId = line.strip().lower()
            contentType = "pdb"
            dividedPath = pdbId[-3:-1]
            # csms stored with upper case file name and hash
            if path.find("comp_model") >= 0:
                contentType = "csm"
                dividedPath = os.path.join(pdbId[0:2], pdbId[-6:-4], pdbId[-4:-2])
            if outputContentType and outputHash:
                out = os.path.join(
                    updateBase,
                    contentType,
                    dividedPath,
                    "%s%s" % (pdbId, outfileSuffix),
                )
            elif outputContentType:
                out = os.path.join(
                    updateBase, contentType, "%s%s" % (pdbId, outfileSuffix)
                )
            elif outputContentType and outputHash:
                out = os.path.join(
                    updateBase, dividedPath, "%s%s" % (pdbId, outfileSuffix)
                )
            else:
                out = os.path.join(updateBase, "%s%s" % (pdbId, outfileSuffix))
            if not os.path.exists(out):
                missing.append(out)
        f.close()
    if len(missing) > 0:
        missingFile = os.path.join(missingFileBase, "missing.txt")
        with open(missingFile, "w", encoding="utf-8") as w:
            for line in missing:
                w.write(line)
                w.write("\n")
    return True


def removeRetractedEntries(
    *,
    listFileBase: str,
    updateBase: str,
    missingFileBase: str,
    outputContentType: bool,
    outputHash: bool,
) -> bool:
    t = time.time()
    # list of upper case ids
    infiles = []
    for filepath in glob.glob(os.path.join(listFileBase, "*core_ids*.txt")):
        """uncomment to test
        if filepath.find("comp_model") >= 0:
            os.unlink(filepath)
            continue
        """
        with open(filepath, "r", encoding="utf-8") as r:
            infiles.extend(r.read().split("\n"))
    infiles = [file for file in infiles if file != ""]
    inkeys = set(infiles)
    # {id : file path}
    # normalize id to upper case
    outfiles = {
        os.path.basename(path)
        .replace(".bcif.gz", "")
        .replace(".bcif", "")
        .upper(): str(path)
        for path in pathlib.Path(updateBase).rglob("*.bcif*")
    }
    outkeys = set(outfiles.keys())
    # keys from outfiles that are not in infiles
    obsoleted = outkeys.difference(inkeys)
    # paths for those keys
    filepaths = [outfiles[key] for key in obsoleted if key in outfiles]
    removed = []
    for filepath in filepaths:
        try:
            os.unlink(filepath)
            removed.append(filepath)
            logging.info("removed %s", filepath)
        except Exception as e:
            logging.error(str(e))
    if len(removed) > 0:
        removedFile = os.path.join(missingFileBase, "removed.txt")
        with open(removedFile, "w", encoding="utf-8") as w:
            for line in removed:
                w.write(line)
                w.write("\n")
    logging.info("removed retracted entries in %.2f s", time.time() - t)
    return True


def removeFiles(tempPath: str, listFileBase: str) -> bool:
    try:
        # once at application close
        if (
            listFileBase
            and os.path.exists(listFileBase)
            and os.path.isdir(listFileBase)
        ):
            if tempPath and os.path.exists(tempPath) and os.path.isdir(tempPath):
                shutil.rmtree(tempPath)
            for filename in os.listdir(listFileBase):
                path = os.path.join(listFileBase, filename)
                if os.path.isfile(path):
                    os.unlink(path)
            for path in glob.glob("/tmp/config-util*"):
                try:
                    shutil.rmtree(path)
                except Exception as e:
                    logging.error(str(e))
    except Exception as e:
        logging.warning(str(e))
    return True


def tasksDone() -> bool:
    logging.info("task maps completed")
    return True


def statusComplete(listFileBase: str) -> bool:
    """
    must occur after end_task
    """
    completeFile = os.path.join(listFileBase, "status.complete")
    dirs = os.path.dirname(completeFile)
    if not os.path.exists(dirs):
        os.makedirs(dirs, mode=0o777)
    with open(completeFile, "w", encoding="utf-8") as w:
        w.write(
            "Binary cif run completed successfully at %s."
            % str(datetime.datetime.now())
        )
    return True


class TestBcif(unittest.TestCase):

    def setUp(self):
        # settings
        self.batch = 0
        self.nfiles = 20
        self.maxTempFiles = 10
        self.outfileSuffix = ".bcif.gz"
        self.outputContentType = False
        self.outputHash = False
        # paths
        self.outputPath = tempfile.mkdtemp()
        self.listFileBase = tempfile.mkdtemp()
        self.missingFileBase = self.listFileBase
        # from sandbox_config.py/MasterConfig
        self.pdbxDict = "https://raw.githubusercontent.com/wwpdb-dictionaries/mmcif_pdbx/master/dist/mmcif_pdbx_v5_next.dic"
        self.maDict = "https://raw.githubusercontent.com/ihmwg/ModelCIF/master/dist/mmcif_ma_ext.dic"
        self.rcsbDict = "https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/master/dictionary_files/dist/rcsb_mmcif_ext.dic"
        self.prereleaseFtpFileBasePath = (
            "http://prereleaseftp-external-east.rcsb.org/pdb"
        )
        self.pdbIdsTimestampFilePath = (
            "holdings/released_structures_last_modified_dates.json.gz"
        )
        self.csmFileRepoBasePath = (
            "http://computed-models-external-east.rcsb.org/staging"
        )
        self.compModelFileHoldingsList = "holdings/computed-models-holdings-list.json"
        self.structureFilePath = "data/structures/divided/mmCIF/"
        #
        self.numSublistFiles = 1
        self.incrementalUpdate = True
        self.configPath = os.path.join(os.path.dirname(__file__), "bcifConfig.yml")
        #
        nsplits = self.numSublistFiles
        if self.numSublistFiles == 0:
            nsplits = multiprocessing.cpu_count()
        self.nresults = self.nfiles * 2 * nsplits
        logging.info("making temp dir %s", self.outputPath)
        logging.info("making temp dir %s", self.listFileBase)

    def tearDown(self):
        if os.path.exists(self.listFileBase):
            shutil.rmtree(self.listFileBase)
        if os.path.exists(self.outputPath):
            shutil.rmtree(self.outputPath)

    def test_workflow(self):

        statusStart(self.listFileBase)

        makeDirs(
            self.listFileBase,
            self.outputPath,
            self.outputContentType,
        )

        pdbHoldingsFilePath = os.path.join(
            self.prereleaseFtpFileBasePath, self.pdbIdsTimestampFilePath
        )
        csmHoldingsFilePath = os.path.join(
            self.csmFileRepoBasePath, self.compModelFileHoldingsList
        )

        self.assertTrue(
            splitRemoteTaskLists(
                pdbHoldingsFilePath,
                csmHoldingsFilePath,
                self.listFileBase,
                self.outputPath,
                self.incrementalUpdate,
                self.outfileSuffix,
                self.numSublistFiles,
                self.configPath,
                self.outputContentType,
                self.outputHash,
            )
        )

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

        params = {
            "listFileBase": self.listFileBase,
            "updateBase": self.outputPath,
            "outfileSuffix": self.outfileSuffix,
            "missingFileBase": self.missingFileBase,
            "maxFiles": self.nfiles,
            "outputContentType": self.outputContentType,
            "outputHash": self.outputHash,
        }
        validateOutput(**params)

        params = {
            "listFileBase": self.listFileBase,
            "updateBase": self.outputPath,
            "missingFileBase": self.missingFileBase,
            "outputContentType": self.outputContentType,
            "outputHash": self.outputHash,
        }

        removeRetractedEntries(**params)

        removeFiles(None, self.listFileBase)

        tasksDone()

        statusComplete(self.listFileBase)

        missingFile = os.path.join(self.missingFileBase)
        removedFile = os.path.join(self.missingFileBase)
        logging.info("missing files, if any, were written to %s", missingFile)
        logging.info(
            "removed obsoleted entries, if any, were written to %s", removedFile
        )

        self.assertTrue(len(os.listdir(self.outputPath)) == self.nresults)

        logging.info("bcif file conversion complete")

        logging.info(str(os.listdir(self.outputPath)))

        self.assertTrue(
            os.path.exists(os.path.join(self.listFileBase, "status.complete"))
        )

        logging.info(str(os.listdir(self.listFileBase)))


def runTestSuite():
    suite = unittest.TestSuite()
    suite.addTest(TestBcif("test_workflow"))
    return suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(runTestSuite())
