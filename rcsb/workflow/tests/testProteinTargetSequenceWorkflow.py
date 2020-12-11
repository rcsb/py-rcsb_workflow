##
# File:    ProteinTargetSequenceTests.py
# Author:  J. Westbrook
# Date:    8-Dec-2020
#
# Updates:
#
##
"""
Tests for protein target data ETL operations.
"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import platform
import resource
import time
import unittest

from rcsb.workflow.targets.ProteinTargetSequenceWorkflow import ProteinTargetSequenceWorkflow
from rcsb.utils.config.ConfigUtil import ConfigUtil


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ProteinTargetSequenceWorkflowTests(unittest.TestCase):
    skipFull = True

    def setUp(self):
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info_remote_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)\n", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    @unittest.skipIf(skipFull, "Database dependency")
    def testProteinEntityData(self):
        """Test case - export protein entity sequence Fasta, taxonomy, and sequence details"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.exportProteinEntityFasta()
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testExportFastaAbbrev(self):
        """Test case - export FAST target files (short test w/o pharos)"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.exportTargets(useCache=True, addTaxonomy=False, reloadPharos=False, resourceNameList=["sabdab", "card", "drugbank", "chembl"])
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    @unittest.skipIf(skipFull, "Very long test")
    def testFetchUniProtTaxonomy(self):
        """Test case - fetch UniProt taxonomy mapping"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.fetchUniProtTaxonomy()
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    @unittest.skipIf(skipFull, "Very long test")
    def testExportFasta(self):
        """Test case - export FAST target files (short test w/o pharos)"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.exportTargets(useCache=True, addTaxonomy=False, reloadPharos=False, resourceNameList=["sabdab", "card", "drugbank", "chembl"])
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def abbrevSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testProteinEntityData"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testExportFastaAbbrev"))
    return suiteSelect


def fullSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testProteinEntityData"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testFetchUniProtTaxonomy"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testExportFasta"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = abbrevSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
