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

    @unittest.skipIf(skipFull, "Very long test")
    def testFetchUniProtTaxonomy(self):
        """Test case - fetch UniProt taxonomy mapping"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.initUniProtTaxonomy()
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

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

    @unittest.skipIf(skipFull, "Database dependency")
    def testChemicalReferenceMappingData(self):
        """Test case - export chemical reference identifier mapping details"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.exportChemRefMapping()
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    @unittest.skipIf(skipFull, "Stash dependency")
    def testExportFastaAbbrev(self):
        """Test case - export FASTA target files"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.exportTargets(useCache=True, addTaxonomy=False, reloadPharos=False, resourceNameList=["sabdab", "card", "drugbank", "chembl", "pharos"])
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    @unittest.skipIf(skipFull, "Very long test")
    def testExportFasta(self):
        """Test case - export FASTA target files"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.exportTargets(useCache=True, addTaxonomy=True, reloadPharos=False, resourceNameList=["sabdab", "card", "drugbank", "chembl", "pharos"])
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    @unittest.skipIf(skipFull, "Very long test")
    def testCreateSearchDatabases(self):
        """Test case - create search databases"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.createSearchDatabases(resourceNameList=["sabdab", "card", "drugbank", "chembl", "pharos", "pdbprent"], timeOutSeconds=3600, verbose=False)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    @unittest.skipIf(skipFull, "Very long test")
    def testSearchDatabases(self):
        """Test case - search sequence databases"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.search(referenceResourceName="pdbprent", resourceNameList=["sabdab", "card", "drugbank", "chembl", "pharos"], identityCutoff=0.95, sensitivity=4.5, timeOut=300)
            self.assertTrue(ok)
            ok = ptsW.search(referenceResourceName="pdbprent", resourceNameList=["card"], identityCutoff=0.95, sensitivity=4.5, timeOut=100, useBitScore=True)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    @unittest.skipIf(skipFull, "Very long test")
    def testBuildFeatures(self):
        """Test case - build features from search results"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.buildFeatureData(referenceResourceName="pdbprent", resourceNameList=["sabdab", "card"], backup=True, remotePrefix="T")
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    @unittest.skipIf(skipFull, "Very long test")
    def testBuildActivityData(self):
        """Test case - build features from search results"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.buildActivityData(referenceResourceName="pdbprent", resourceNameList=["chembl", "pharos"], backup=True, remotePrefix="T")
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    @unittest.skipIf(skipFull, "Very long test")
    def testBuildCofactorData(self):
        """Test case - build features from search results"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.buildCofactorData(referenceResourceName="pdbprent", resourceNameList=["chembl", "pharos"], backup=True, remotePrefix="T")
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    #
    # --- --- --- ---
    @unittest.skipIf(skipFull, "Very long test")
    def testUpdateUniProtTaxonomy(self):
        """Test case - initialize the UniProt taxonomy provider (from scratch ~3482 secs)"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.updateUniProtTaxonomy()
            self.assertTrue(ok)
            ok = ptsW.initUniProtTaxonomy()
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
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testFetchUniProtTaxonomy"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testProteinEntityData"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testExportFasta"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testCreateSearchDatabases"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testSearchDatabases"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testBuildFeatures"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testBuildActivityData"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testBuildCofactorData"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = fullSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
