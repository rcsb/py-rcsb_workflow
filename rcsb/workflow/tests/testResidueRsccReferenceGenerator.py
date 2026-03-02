##
#  File:           testResidueRsccReferenceGenerator.py
#  Date:           2025-12-25 Chenghua Shao
#
#  Update:
##
"""
Unit tests for ResidueRsccReferenceGenerator.py
"""
import json
import logging
import os
import platform
import resource
import time
import unittest
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.db.mongo.Connection import Connection
from rcsb.workflow.refstats.ResidueRsccReferenceGenerator import ResidueRsccReferenceGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ResidueRsccReferenceGeneratorTests(unittest.TestCase):
    debugFlag = False

    def setUp(self):
        self.__isMac = platform.system() == "Darwin"
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")
        os.makedirs(self.__cachePath, exist_ok=True)
        self.__resourceName = "MONGO_DB"
        if self.__isMac:  # for CS Mac with a temp DB, kept for testing and debugging
            self.__databaseName = "dw"
            self.__cfgOb = ConfigUtil(
                configPath=configPath,
                defaultSectionName="site_info_configuration",
                mockTopPath=self.__mockTopPath
            )
            self.cRRRG = ResidueRsccReferenceGenerator(
                self.__cfgOb,
                cachePath=self.__cachePath,
                databaseName="dw",
                collectionNames=[
                    "core_entry",
                    "core_polymer_entity",
                    "core_polymer_entity_instance"
                ]
            )
        else:  # production testing
            self.__databaseName = "pdbx_core"
            self.__cfgOb = ConfigUtil(
                configPath=configPath,
                defaultSectionName="site_info_configuration",
                mockTopPath=self.__mockTopPath
            )
            # self.__workflowFixture()
            self.cRRRG = ResidueRsccReferenceGenerator(self.__cfgOb, cachePath=self.__cachePath)
        self.__startTime = time.time()
        logger.info("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    @unittest.skipUnless(debugFlag, "Skip individual debug tests")
    def testFetchEntry(self):
        """
        Test fetchEntry on a resolution bin.
        """
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            db = client[self.__databaseName]
            resolution_bin = [0, 0.5]
            self.cRRRG.fetchEntry(db, resolution_bin=resolution_bin)
            self.assertTrue(self.cRRRG.resolution_bin["entry_ids"])
            # Write to output file
            output_file = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_fetchEntry.json")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(self.cRRRG.resolution_bin["entry_ids"], f, indent=2)
            logger.info("Wrote data to output file %s", output_file)

    @unittest.skipUnless(debugFlag, "Skip individual debug tests")
    def testFetchEntity(self):
        """
        Test fetchEntity on several PDB IDs.
        """
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            db = client[self.__databaseName]
            self.cRRRG.resolution_bin["entry_ids"] = ["2ANR", "2OR2"]
            self.cRRRG.fetchEntity(db)
            self.assertTrue(self.cRRRG.resolution_bin["entities"])
            # Write to output file
            output_file = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_fetchEntity.json")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(self.cRRRG.resolution_bin["entities"], f, indent=2)
            logger.info("Wrote data to output file %s", output_file)

    @unittest.skipUnless(debugFlag, "Skip individual debug tests")
    def testProcessEntity(self):
        """
        Test processEntity on several PDB entries.
        """
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            db = client[self.__databaseName]
            self.cRRRG.resolution_bin["entry_ids"] = ["2ANR", "2OR2"]
            self.cRRRG.fetchEntity(db)
            self.cRRRG.processEntity()
            self.assertTrue(self.cRRRG.resolution_bin["sequences"])
            self.assertTrue(self.cRRRG.resolution_bin["instance_ids"])
            # Write to output file
            output_file = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_processEntity.json")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(self.cRRRG.resolution_bin["sequences"], f, indent=2)
            logger.info("Wrote data to output file %s", output_file)

    @unittest.skipUnless(debugFlag, "Skip individual debug tests")
    def testFetchInstance(self):
        """
        Test fetchInstance on several PDB IDs.
        """
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            db = client[self.__databaseName]
            self.cRRRG.resolution_bin["instance_ids"] = ["2ANR.B", "2OR2.A"]
            # self.cRRRG.bin["instance_ids"] = ["3NIR.A"]
            self.cRRRG.fetchInstance(db)
            self.assertTrue(self.cRRRG.resolution_bin["instances"])
            # Write to output file
            output_file = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_fetchInstance.json")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(self.cRRRG.resolution_bin["instances"], f, indent=2)
            logger.info("Wrote data to output file %s", output_file)

    @unittest.skipUnless(debugFlag, "Skip individual debug tests")
    def testProcessInstance(self):
        """
        Test processInstance on several PDB IDs.
        """
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            db = client[self.__databaseName]
            self.cRRRG.resolution_bin["instance_ids"] = ["2ANR.B", "2OR2.A"]
            # self.cRRRG.bin["instance_ids"] = ["3NIR.A"]
            self.cRRRG.fetchInstance(db)
            self.cRRRG.processInstance()
            self.assertTrue(self.cRRRG.resolution_bin["metrics"])
            self.assertTrue(self.cRRRG.resolution_bin["fragments_start"])
            # Write to output file
            output_file1 = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_processInstance1.json")
            with open(output_file1, "w", encoding="utf-8") as f:
                json.dump(self.cRRRG.resolution_bin["metrics"], f, indent=2)
            output_file2 = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_processInstance2.json")
            with open(output_file2, "w", encoding="utf-8") as f:
                json.dump(self.cRRRG.resolution_bin["fragments_start"], f, indent=2)
            logger.info("Wrote data to output files %s, %s", output_file1, output_file2)

    @unittest.skipUnless(debugFlag, "Skip individual debug tests")
    def testProcessResidue(self):
        """
        Test processResidue on several PDB IDs.
        """
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            db = client[self.__databaseName]
            l_entry_id = ["2ANR", "2OR2"]
            # l_entry_id = ["3NIR"]
            self.cRRRG.resolution_bin["entry_ids"] = l_entry_id
            self.cRRRG.fetchEntity(db)
            self.cRRRG.processEntity()
            self.cRRRG.fetchInstance(db)
            self.cRRRG.processInstance()
            self.cRRRG.processResidue()
            self.assertTrue(self.cRRRG.resolution_bin["residues"])
            logger.info(self.cRRRG.resolution_bin["tracking"])
            # Write to output file
            output_file = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_processResidue.json")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(self.cRRRG.resolution_bin["residues"], f, indent=2)
            logger.info("Wrote data to output files %s", output_file)
            output_file_tracking = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_processResidueTracking.tsv")
            self.cRRRG.writeTracking(output_file_tracking)
            logger.info("Wrote tracking data to output files %s", output_file_tracking)

    @unittest.skipUnless(debugFlag, "Skip individual debug tests")
    def testCalculatePercentiles(self):
        """
        Test calculatePercentiles on several PDB IDs.
        """
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            db = client[self.__databaseName]
            self.cRRRG.fetchEntry(db, [0, 0.6])
            self.cRRRG.fetchEntity(db)
            self.cRRRG.processEntity()
            self.cRRRG.fetchInstance(db)
            self.cRRRG.processInstance()
            self.cRRRG.processResidue()
            logger.info(self.cRRRG.resolution_bin["tracking"])
            self.cRRRG.calculatePercentiles()
            self.assertTrue(self.cRRRG.data_rscc)
            # Write to output file
            output_file = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_calculatePercentiles.json")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(self.cRRRG.data_rscc, f, indent=2)
            logger.info("Wrote data to output files %s", output_file)

    @unittest.skipUnless(debugFlag, "Skip individual debug tests")
    def testGenerateBin(self):
        """
        Test generateBin on several PDB IDs.
        """
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            db = client[self.__databaseName]
            self.cRRRG.generateBin(db, [0.1, 0.5])
            # self.cRRRG.generateBin([1, 1.1])
            self.assertTrue(self.cRRRG.data_rscc)
            # Write to output file
            output_file = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_generateBin.json")
            self.cRRRG.writeReference(output_file)
            logger.info("Wrote data to output files %s", output_file)
            output_file_tsv = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_generateBin.tsv")
            self.cRRRG.writeReviewReference(output_file_tsv)
            logger.info("Wrote data to output tsv files %s", output_file_tsv)
            output_file_tracking = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_generateBinTracking.tsv")
            self.cRRRG.writeTracking(output_file_tracking)
            logger.info("Wrote tracking data to output files %s", output_file_tracking)

    def testGenerate(self):
        """
        Test generate on all PDB entries.
        """
        self.cRRRG.generate(resolution_range=[1.9, 2.1])
        output_file = self.cRRRG.getRcssRefDataPath()
        self.assertTrue(os.path.exists(output_file), "Output JSON file does not exist.")
        output_file_tsv = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_generate.tsv")
        self.cRRRG.writeReviewReference(output_file_tsv)
        logger.info("Wrote data to output tsv files %s", output_file_tsv)


def genRsccRef():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ResidueRsccReferenceGeneratorTests("testFetchEntry"))
    suiteSelect.addTest(ResidueRsccReferenceGeneratorTests("testFetchEntity"))
    suiteSelect.addTest(ResidueRsccReferenceGeneratorTests("testProcessEntity"))
    suiteSelect.addTest(ResidueRsccReferenceGeneratorTests("testFetchInstance"))
    suiteSelect.addTest(ResidueRsccReferenceGeneratorTests("testProcessInstance"))
    suiteSelect.addTest(ResidueRsccReferenceGeneratorTests("testProcessResidue"))
    suiteSelect.addTest(ResidueRsccReferenceGeneratorTests("testCalculatePercentiles"))
    suiteSelect.addTest(ResidueRsccReferenceGeneratorTests("testGenerateBin"))
    suiteSelect.addTest(ResidueRsccReferenceGeneratorTests("testGenerate"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = genRsccRef()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
