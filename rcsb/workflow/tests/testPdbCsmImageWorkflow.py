##
# File:    testPdbCsmImageWorkflow.py
# Author:  M. Trumbull
# Date:    12-Dec-2024
# Version: 0.001
#
# Updates:
#
##

__docformat__ = "google en"
__author__ = "Michael Trumbull"
__email__ = "michael.trumbull@rcsb.org"
__license__ = "Apache 2.0"


import logging
import os
import platform
import resource
import time
import unittest


# from rcsb.workflow.wuw.PdbCsmImageWorkflow import PdbCsmImageWorkflow

#
#
#                  WARNING
#             NOT YET IMPLEMENTED
# This is a temporary test framework for PdbCsmImageWorkflow.
#
# We need to decide on how to include node and
# molstar in our testing framework.
#
#

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class TestPdbCsmImageWorkflow(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(TestPdbCsmImageWorkflow, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        logger.info(self.__verbose)
        self.__isMac = platform.system() == "Darwin"
        self.__excludeTypeL = None if self.__isMac else ["optional"]
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(mockTopPath, "config", "dbload-setup-example.yml")
        configName = "site_info_configuration"
        cachePath = os.path.join(TOPDIR, "CACHE")
        self.__dataPath = os.path.join(HERE, "test-data")
        #
        self.__commonD = {
            "configPath": configPath,
            "mockTopPath": mockTopPath,
            "configName": configName,
            "cachePath": cachePath,
            "rebuildCache": False,
            "providerTypeExcludeL": self.__excludeTypeL,
            "restoreUseGit": True,
            "restoreUseStash": False,
        }
        logger.info(self.__commonD)
        self.__loadCommonD = {"readBackCheck": True, "numProc": 2, "chunkSize": 5, "refChunkSize": 5, "loadType": "full", "useFilteredLists": True}
        logger.info(self.__loadCommonD)
        #
        # These are test source files for chemical component/BIRD indices
        ccUrlTarget = os.path.join(self.__dataPath, "components-abbrev.cif")
        birdUrlTarget = os.path.join(self.__dataPath, "prdcc-abbrev.cif")
        ccFileNamePrefix = "cc-abbrev"
        self.__chemEtlD = {
            "fetchLimit": 4,
            "numProc": 1,
            "chunkSize": 20,
            "loadType": "full",
            "ccUrlTarget": ccUrlTarget,
            "birdUrlTarget": birdUrlTarget,
            "ccFileNamePrefix": ccFileNamePrefix,
        }
        logger.info(self.__chemEtlD)
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def doNothing(self):
        pass

    def testIdListGeneration(self):
        """Test id list file generation ..."""
        try:
            pass
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testJpgGeneration(self):
        """Test jpg file generation ..."""
        try:
            pass
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def workflowLoadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(TestPdbCsmImageWorkflow("testIdListGeneration"))
    suiteSelect.addTest(TestPdbCsmImageWorkflow("testJpgGeneration"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = workflowLoadSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
