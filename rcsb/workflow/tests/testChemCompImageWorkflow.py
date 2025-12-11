##
#
# File:    ChemCompImageWorkflowTests.py
# Author:  jdw
# Date:    10-Mar-2020
# Version: 0.001
#
# Updates:
##
"""
A collection of tests chemical component image generation workflows

"""
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"

import logging
import os
import platform
import resource
import time
import unittest

from importlib.metadata import version as get_package_version
from rcsb.workflow.chem.ChemCompImageWorkflow import ChemCompImageWorkflow

__version__ = get_package_version("rcsb.workflow")

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


class ChemCompImageWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.__startTime = time.time()
        self.__cachePath = os.path.join(HERE, "test-data")
        self.__workPath = os.path.join(HERE, "test-output")
        logger.debug("Running tests on version %s", __version__)
        logger.info("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testMakeImages(self):
        """Test case -  generate all images for chemical component defintions."""
        try:
            # Example uses default urls for public chemical dictionaries
            cciWf = ChemCompImageWorkflow(imagePath=self.__workPath, cachePath=self.__cachePath)
            ok = cciWf.testCache()
            if not ok:
                logger.error("Image generation dependency generation failure")
            self.assertTrue(ok)
            #
            ok = cciWf.makeImages()
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def suiteImageGeneration():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ChemCompImageWorkflowTests("testMakeImages"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = suiteImageGeneration()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
