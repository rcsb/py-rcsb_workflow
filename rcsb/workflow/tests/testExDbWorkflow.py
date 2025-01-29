##
# File:    ExDbWorkflowTests.py
# Author:  J. Westbrook
# Date:    17-Dec-2019
# Version: 0.001
#
# Updates:

#
##
"""
Tests for simple workflows to excute ExDB loading operations.
"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import platform
import resource
import time
import unittest

from rcsb.workflow.wuw.ExDbWorkflow import ExDbWorkflow

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ExDbWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.__isMac = platform.system() == "Darwin"
        self.__excludeTypeL = None if self.__isMac else ["optional"]
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(mockTopPath, "config", "dbload-setup-example.yml")
        configName = "site_info_configuration"
        cachePath = os.path.join(TOPDIR, "CACHE")
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
        self.__loadCommonD = {"readBackCheck": True, "numProc": 2, "chunkSize": 5, "refChunkSize": 5, "loadType": "full", "useFilteredLists": True}
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testExDbLoaderWorkflows(self):
        """Test run workflow steps ..."""
        try:
            # opL = ["etl_chemref", "upd_ref_seq", "etl_tree_node_lists"]
            opL = ["etl_chemref", "etl_tree_node_lists"]
            rlWf = ExDbWorkflow(**self.__commonD)
            for op in opL:
                ok = rlWf.load(op, **self.__loadCommonD)
                self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

        # Above needs to run first in order for below to work; will need to investigate why
        # logger.info("Now running ExDbWorkflow for computed-models")
        # try:
        #     opL = ["upd_ref_seq_comp_models"]
        #     rlWf = ExDbWorkflow(**self.__commonD)
        #     for op in opL:
        #         ok = rlWf.load(op, **self.__loadCommonD)
        #         self.assertTrue(ok)
        # except Exception as e:
        #     logger.exception("Failing with %s", str(e))
        #     self.fail()

    @unittest.skip("Troubleshooting test")
    def testExDbLoaderWorkflowsWithCacheCheck(self):
        """Test run sequence reference data update workflow step ..."""
        #
        try:
            self.__commonD["rebuildCache"] = False
            rlWf = ExDbWorkflow(**self.__commonD)
            #
            ok = rlWf.load("upd_ref_seq", minMatchPrimaryPercent=50.0, **self.__loadCommonD)
            logger.info("Cache status is %r", ok)
            self.assertTrue(ok)
            if ok:
                self.__commonD["rebuildCache"] = False
                rlWf = ExDbWorkflow(**self.__commonD)
                ok1 = rlWf.load("upd_ref_seq", **self.__loadCommonD)
                self.assertTrue(ok1)
                ok3 = rlWf.load("etl_tree_node_lists", **self.__loadCommonD)
                self.assertTrue(ok3)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def workflowLoadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ExDbWorkflowTests("testExDbLoaderWorkflows"))
    suiteSelect.addTest(ExDbWorkflowTests("testExDbLoaderWorkflowsWithCacheCheck"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = workflowLoadSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
