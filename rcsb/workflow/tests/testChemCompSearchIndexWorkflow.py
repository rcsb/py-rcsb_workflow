##
#
# File:    testChemCompSearchIndexWorkflow.py
# Author:  jdw
# Date:    10-Mar-2020
# Version: 0.001
#
# Updates:
##
"""
A collection of tests chemical component index generation workflows

"""
import logging
import os
import platform
import resource
import time
import unittest

from importlib.metadata import version as get_package_version
from rcsb.workflow.chem.ChemCompSearchIndexWorkflow import ChemCompSearchIndexWorkflow

__version__ = get_package_version("rcsb.workflow")

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


class ChemCompSearchIndexWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.__startTime = time.time()
        self.__copyPath = os.path.join(HERE, "test-output", "COPY")
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")
        self.__dataPath = os.path.join(HERE, "test-data")
        abbrevTest = True
        self.__ccUrlTarget = os.path.join(self.__dataPath, "components-abbrev.cif") if abbrevTest else None
        self.__birdUrlTarget = os.path.join(self.__dataPath, "prdcc-abbrev.cif") if abbrevTest else None
        self.__ccFileNamePrefix = "cc-abbrev" if abbrevTest else "cc-full"
        logger.debug("Running tests on version %s", __version__)
        logger.info("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testMakeAndStashIndices(self):
        """Test case -  generate chemical component and BIRD search indices

        Maximum resident memory size 7126 MB   6 procs on macbook pro (1374 seconds)
        Maximum resident memory size 7479 MB   4 procs on macbook pro (1528 seconds)

        """
        try:
            ccidxWf = ChemCompSearchIndexWorkflow(cachePath=self.__cachePath, ccFileNamePrefix=self.__ccFileNamePrefix)
            ok = ccidxWf.makeIndices(self.__ccUrlTarget, self.__birdUrlTarget, numProc=4)
            self.assertTrue(ok)
            ok = ccidxWf.stashIndices(None, self.__copyPath, bundleLabel="A", userName=None, pw=None)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def suiteIndexGeneration():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ChemCompSearchIndexWorkflowTests("testMakeAndStashIndices"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = suiteIndexGeneration()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
