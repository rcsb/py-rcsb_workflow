##
# File:    ProteinTargetSequenceTests.py
# Author:  J. Westbrook
# Date:    25-Jun-2021
#
# Updates:
#  13-Feb-2025 dwp Remove IMGT from feature building after service became unavailable February 2025
#  19-Feb-2025 dwp Bring back IMGT
#   6-Jan-2026 dwp Re-exclude IMGT from testing (consumes a lot of disk space by fetching 1.6 GB file,
#                  and this is already tested by rcsb.utils.targets);
#                  Exclude chembl and drugbank from testing to save on disk space
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
import shutil

from rcsb.workflow.targets.ProteinTargetSequenceWorkflow import ProteinTargetSequenceWorkflow
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.FileUtil import FileUtil


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ProteinTargetSequenceWorkflowTests(unittest.TestCase):
    skipFull = True

    def setUp(self):
        self.__isMac = platform.system() == "Darwin"
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")
        #
        self._disk_before = shutil.disk_usage(HERE).used
        logger.info("Filesystem disk usage start: %.2f MB", self._disk_before / (1024 ** 2))
        #
        self.__workflowFixture()
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        disk_after = shutil.disk_usage(HERE).used
        disk_delta = disk_after - self._disk_before
        logger.info("Filesystem disk usage delta: %.2f MB", disk_delta / (1024 ** 2))
        logger.info("Completed %s at %s (%.4f seconds)\n", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def __workflowFixture(self):
        try:
            ok = False
            fU = FileUtil()
            dataPath = os.path.join(HERE, "test-data")
            srcPath = os.path.join(dataPath, "Pharos")
            dstPath = os.path.join(self.__cachePath, "Pharos-targets")
            for fn in ["drug_activity", "cmpd_activity", "target", "protein", "t2tc"]:
                inpPath = os.path.join(srcPath, fn + ".tdd.gz")
                outPath = os.path.join(dstPath, fn + ".tdd.gz")
                fU.get(inpPath, outPath)
                fU.uncompress(outPath, outputDir=dstPath)
                fU.remove(outPath)
            #
            fU.put(os.path.join(srcPath, "pharos-readme.txt"), os.path.join(dstPath, "pharos-readme.txt"))
            #
            fastaPath = os.path.join(self.__cachePath, "FASTA")
            outPath = os.path.join(fastaPath, "pdbprent-targets.fa.gz")
            fU.mkdir(fastaPath)
            fU.put(os.path.join(dataPath, "pdbprent-targets.fa.gz"), outPath)
            fU.uncompress(outPath, outputDir=fastaPath)
            #
            crPath = os.path.join(self.__cachePath, "chemref-mapping")
            outPath = os.path.join(crPath, "chemref-mapping-data.json")
            fU.mkdir(crPath)
            fU.put(os.path.join(dataPath, "chemref-mapping-data.json"), outPath)
            ok = True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            ok = False
        return ok

    @unittest.skipIf(skipFull, "Very long test")
    def testFetchUniProtTaxonomy(self):
        """Test case - reload UniProt taxonomy mapping"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.reloadUniProtTaxonomy()
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    @unittest.skipIf(skipFull, "Database dependency")
    def testProteinEntityData(self):
        """Test case - export RCSB protein entity sequence FASTA, taxonomy, and sequence details"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.exportRCSBProteinEntityFasta()
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    @unittest.skipIf(skipFull, "Database dependency")
    def testChemicalReferenceMappingData(self):
        """Test case - export RCSB chemical reference identifier mapping details"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.exportRCSBChemRefMapping()
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    @unittest.skipIf(skipFull, "Database dependency")
    def testLigandNeighborMappingData(self):
        """Test case - export RCSB ligand neighbor mapping details"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.exportRCSBLigandNeighborMapping()
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testAAExportFastaAbbrev(self):
        """Test case - export FASTA target files"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            # ok = ptsW.exportTargetsFasta(useCache=True, addTaxonomy=False, reloadPharos=False, fromDbPharos=False, resourceNameList=["sabdab", "card", "chembl", "pharos"])
            ok = ptsW.exportTargetsFasta(useCache=True, addTaxonomy=False, reloadPharos=False, fromDbPharos=False, resourceNameList=["sabdab", "card", "pharos"])
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    @unittest.skipIf(skipFull, "Very long test")
    def testExportFasta(self):
        """Test case - export FASTA target files (and load Pharos from source)"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            # ok = ptsW.exportTargetsFasta(useCache=True, addTaxonomy=True, reloadPharos=True, fromDbPharos=True, resourceNameList=["sabdab", "card", "drugbank", "chembl", "pharos"])
            ok = ptsW.exportTargetsFasta(useCache=True, addTaxonomy=True, reloadPharos=True, fromDbPharos=True, resourceNameList=["sabdab", "card", "pharos"])
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testBBCreateSearchDatabases(self):
        """Test case - create search databases"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            # ok = ptsW.createSearchDatabases(resourceNameList=["sabdab", "card", "chembl", "pharos", "pdbprent"], addTaxonomy=False, timeOutSeconds=3600, verbose=False)
            ok = ptsW.createSearchDatabases(resourceNameList=["sabdab", "pharos", "pdbprent"], addTaxonomy=False, timeOutSeconds=3600, verbose=False)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testCCSearchDatabases(self):
        """Test case - search sequence databases"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            formatOutput = "query,target,pident,alnlen,mismatch,gapopen,qstart,qend,tstart,tend,evalue,raw,bits,qlen,tlen,qaln,taln,cigar"
            ok = ptsW.search(
                referenceResourceName="pdbprent",
                # resourceNameList=["sabdab", "chembl", "pharos"],
                resourceNameList=["sabdab", "pharos"],
                identityCutoff=0.95,
                sensitivity=4.5,
                timeOutSeconds=1000,
                formatOutput=formatOutput,
            )
            self.assertTrue(ok)
            ok = ptsW.search(
                referenceResourceName="pdbprent", resourceNameList=["sabdab"], identityCutoff=0.95, sensitivity=4.5, timeOutSeconds=1000, useBitScore=True, formatOutput=formatOutput
            )
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testDDBuildFeatures(self):
        """Test case - build features from search results"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.buildFeatureData(referenceResourceName="pdbprent", resourceNameList=["sabdab"], useTaxonomy=False, backup=False)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testEEBuildActivityData(self):
        """Test case - build features from search results"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            if self.__isMac:
                # ok = ptsW.buildActivityData(referenceResourceName="pdbprent", resourceNameList=["chembl", "pharos"], backup=False, maxTargets=50)
                ok = ptsW.buildActivityData(referenceResourceName="pdbprent", resourceNameList=["pharos"], backup=False, maxTargets=50)
            else:
                ok = ptsW.buildActivityData(referenceResourceName="pdbprent", resourceNameList=["pharos"], backup=False, maxTargets=50)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    @unittest.skip("Skipping until proper fixture is created to provide necessary chembl and pharos cofactor data files in starting cache")
    def testFFBuildCofactorData(self):
        """Test case - build features from search results"""
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            # ok = ptsW.buildCofactorData(referenceResourceName="pdbprent", resourceNameList=["chembl", "pharos"], backup=False)
            ok = ptsW.buildCofactorData(referenceResourceName="pdbprent", resourceNameList=["pharos"], backup=False)
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
            ok = ptsW.updateUniProtTaxonomy()
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def abbrevSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testAAExportFastaAbbrev"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testBBCreateSearchDatabases"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testCCSearchDatabases"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testDDBuildFeatures"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testEEBuildActivityData"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testFFBuildCofactorData"))
    return suiteSelect


def fullSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testFetchUniProtTaxonomy"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testChemicalReferenceMappingData"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testLigandNeighborMappingData"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testProteinEntityData"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testExportFasta"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testBBCreateSearchDatabases"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testCCSearchDatabases"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testDDBuildFeatures"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testEEBuildActivityData"))
    suiteSelect.addTest(ProteinTargetSequenceWorkflowTests("testFFBuildCofactorData"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = abbrevSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
