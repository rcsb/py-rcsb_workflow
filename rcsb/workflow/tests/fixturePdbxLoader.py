##
# File:    PdbxLoaderFixture.py
# Author:  J. Westbrook
# Date:    4-Sep-2019
# Version: 0.001
#
# Updates:
#
##
"""
Fixture for loading the chemical reference and pdbx_core collections in a loca mongo instance.

"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

# import glob
import logging
import os
import platform
import resource
import time
import unittest

from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.mongo.PdbxLoader import PdbxLoader
from rcsb.utils.config.ConfigUtil import ConfigUtil
# from rcsb.utils.io.FileUtil import FileUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class PdbxLoaderFixture(unittest.TestCase):

    def __init__(self, methodName="runTest"):
        super(PdbxLoaderFixture, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        #
        self.__isMac = platform.system() == "Darwin"
        self.__excludeTypeL = None if self.__isMac else ["optional"]
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        # configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example-local.yml")
        # To Do: Investigate why GitUtil sometimes gives divergence error when using 'DISCOVERY_MODE: remote', but not with 'local':
        #            stderr: 'fatal: Need to specify how to reconcile divergent branches.'
        #        Behavior isn't entirely predictable, since it happens sometimes but not all the time.
        #        To fully debug, will need to add more logging statements to GitUtil, StashableBase, & StashUtil (in rcsb.utils.io)
        #        Or, can try to resolve error directly by specifying how to reconcile diverent branches in git.Repo class.
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        #
        self.__resourceName = "MONGO_DB"
        self.__failedFilePath = os.path.join(HERE, "test-output", "failed-list.txt")
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__readBackCheck = True
        self.__numProc = 1
        self.__chunkSize = 2
        self.__fileLimit = 38
        self.__documentStyle = "rowwise_by_name_with_cardinality"
        #
        self.__birdChemCompCoreIdList = [
            "PRD_000010",
            "PRD_000060",
            "PRD_000220",
            "PRD_000882",
            "PRD_000154",
            "PRD_000877",
            "PRD_000198",
            "PRD_000009",
            "PRD_000979",
            "PRDCC_000010",
            "PRDCC_000220",
            "PRDCC_000882",
            "PRDCC_000154",
            "PRDCC_000198",
            "PRDCC_000009",
            "FAM_000010",
            "FAM_000210",
            "FAM_000220",
            "FAM_000001",
            "FAM_000391",
            "FAM_000093",
            "FAM_000084",
            "FAM_000016",
            "FAM_000336",
            "1G1",
            "2RT",
            "2XL",
            "2XN",
            "ATP",
            "BJA",
            "BM3",
            "CNC",
            "DAL",
            "DDZ",
            "DHA",
            "DSN",
            "GTP",
            "HKL",
            "NAC",
            "NAG",
            "NND",
            "PTR",
            "SEP",
            "SMJ",
            "STL",
            "UNK",
            "UNX",
            "UVL",
        ]
        #
        self.__pdbIdList = [
            "1AH1",
            "1B5F",
            "1BMV",
            "1C58",
            "1DSR",
            "1DUL",
            "1KQE",
            "1O3Q",
            "1SFO",
            "2HW3",
            "2HYV",
            "2OSL",
            "2VOO",
            "2WMG",
            "3AD7",
            "3HYA",
            "3IYD",
            "3MBG",
            "3RER",
            "3VD8",
            "3VFJ",
            "3X11",
            "3ZTJ",
            "4E2O",
            "4EN8",
            "4MEY",
            "5EU8",
            "5KDS",
            # "5TM0",
            "5VH4",
            # "5VP2",
            # "6FSZ",
            "6LU7",
            "6NN7",
            # "6Q20",
            "6RFK",
            "6RKU",
            "6YRQ",
        ]
        self.__ldList = [
            {
                "databaseName": "bird_chem_comp_core",
                "collectionNameList": None,
                "loadType": "full",
                "mergeContentTypes": None,
                "validationLevel": "min",
                "inputIdCodeList": self.__birdChemCompCoreIdList
            },
            {
                "databaseName": "pdbx_core",
                "collectionNameList": None,
                "loadType": "replace",
                "mergeContentTypes": ["vrpt"],
                "validationLevel": "min",
                "inputIdCodeList": self.__pdbIdList
            },
            # {
            #     "databaseName": "pdbx_comp_model_core",
            #     "collectionNameList": None,
            #     "loadType": "full",
            #     "mergeContentTypes": None,
            #     "validationLevel": "min",
            #     "inputIdCodeList": None
            # },
        ]
        #
        # self.__modelFixture()
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    # def __modelFixture(self):
    #     fU = FileUtil()
    #     modelSourcePath = os.path.join(self.__mockTopPath, "AF")
    #     for iPath in glob.iglob(os.path.join(modelSourcePath, "*.cif.gz")):
    #         fn = os.path.basename(iPath)
    #         uId = fn.split("-")[1]
    #         h3 = uId[-2:]
    #         h2 = uId[-4:-2]
    #         h1 = uId[-6:-4]
    #         oPath = os.path.join(self.__cachePath, "computed-models", h1, h2, h3, fn)
    #         fU.put(iPath, oPath)

    def testPdbxLoader(self):
        #
        for ld in self.__ldList:
            ok = self.__pdbxLoaderWrapper(**ld)
            self.assertTrue(ok)

    def __pdbxLoaderWrapper(self, **kwargs):
        """Wrapper for the PDBx loader module"""
        ok = False
        try:
            logger.info("Loading %s", kwargs["databaseName"])
            mw = PdbxLoader(
                self.__cfgOb,
                cachePath=self.__cachePath,
                resourceName=self.__resourceName,
                numProc=self.__numProc,
                chunkSize=self.__chunkSize,
                fileLimit=kwargs.get("fileLimit", self.__fileLimit),
                verbose=self.__verbose,
                readBackCheck=self.__readBackCheck,
                maxStepLength=1000,
                useSchemaCache=True,
                rebuildSchemaFlag=False,
            )
            ok = mw.load(
                kwargs["databaseName"],
                collectionLoadList=kwargs["collectionNameList"],
                loadType=kwargs["loadType"],
                inputPathList=None,
                inputIdCodeList=kwargs["inputIdCodeList"],
                styleType=self.__documentStyle,
                dataSelectors=["PUBLIC_RELEASE"],
                failedFilePath=self.__failedFilePath,
                saveInputFileListPath=None,
                pruneDocumentSize=None,
                logSize=False,
                validationLevel=kwargs["validationLevel"],
                mergeContentTypes=kwargs["mergeContentTypes"],
                useNameFlag=False,
                providerTypeExcludeL=self.__excludeTypeL,
                restoreUseGit=True,
                restoreUseStash=False,
            )
            self.assertTrue(ok)
            ok = self.__loadStatus(mw.getLoadStatus())
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()
        return ok

    def __loadStatus(self, statusList):
        sectionName = "data_exchange_configuration"
        dl = DocumentLoader(
            self.__cfgOb,
            self.__cachePath,
            resourceName=self.__resourceName,
            numProc=self.__numProc,
            chunkSize=self.__chunkSize,
            documentLimit=None,
            verbose=self.__verbose,
            readBackCheck=self.__readBackCheck,
        )
        #
        databaseName = self.__cfgOb.get("DATABASE_NAME", sectionName=sectionName)
        collectionName = self.__cfgOb.get("COLLECTION_UPDATE_STATUS", sectionName=sectionName)
        ok = dl.load(databaseName, collectionName, loadType="append", documentList=statusList, indexAttributeList=["update_id", "database_name", "object_name"], keyNames=None)
        return ok


def mongoLoadPdbxSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(PdbxLoaderFixture("testPdbxLoader"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = mongoLoadPdbxSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
