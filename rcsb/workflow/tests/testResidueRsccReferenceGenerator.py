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
from rcsb.workflow.stats.ResidueRsccReferenceGenerator import ResidueRsccReferenceGenerator
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.db.mongo.Connection import Connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ResidueRsccReferenceGeneratorTests(unittest.TestCase):
    skipFull = True

    def setUp(self):
        self.__isMac = platform.system() == "Darwin"
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")
        os.makedirs(self.__cachePath, exist_ok=True)
        self.__resourceName = "MONGO_DB"
        if self.__isMac:
            self.__databaseName = "dw"
            self.__cfgOb = ConfigUtil(configPath=configPath,
                                      defaultSectionName="site_info_configuration",
                                      mockTopPath=self.__mockTopPath)
            self.cRRRG = ResidueRsccReferenceGenerator(self.__cfgOb,
                                                       cachePath=self.__cachePath,
                                                       databaseName="dw",
                                                       collectionNames=["core_entry",
                                                                        "core_polymer_entity",
                                                                        "core_polymer_entity_instance"])
        else:
            self.__databaseName = "pdbx_core"
            self.__cfgOb = ConfigUtil(configPath=configPath,
                                      defaultSectionName="site_info_configuration",
                                      mockTopPath=self.__mockTopPath)
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

    def testFetchEntry(self):
        """
        Test fetchEntry on a resolution bin.
        """
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            db = client[self.__databaseName]
            resolution_bin = [0, 0.5]
            self.cRRRG.fetchEntry(db, resolution_bin=resolution_bin)
            self.assertTrue(self.cRRRG.bin["entry_ids"])
            # Write to output file
            output_file = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_fetchEntry.json")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(self.cRRRG.bin["entry_ids"], f, indent=2)
            logger.info("Wrote data to output file %s", output_file)

    def testFetchEntity(self):
        """
        Test fetchEntity on several PDB IDs.
        """
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            db = client[self.__databaseName]
            self.cRRRG.bin["entry_ids"] = ["2ANR", "2OR2"]
            rt = self.cRRRG.fetchEntity(db)
            self.assertTrue(rt)
            self.assertTrue(self.cRRRG.bin["entities"])
            # Write to output file
            output_file = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_fetchEntity.json")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(self.cRRRG.bin["entities"], f, indent=2)
            logger.info("Wrote data to output file %s", output_file)

    def testProcessEntity(self):
        """
        Test processEntity on several PDB entries.
        """
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            db = client[self.__databaseName]
            self.cRRRG.bin["entry_ids"] = ["2ANR", "2OR2"]
            self.cRRRG.fetchEntity(db)
            rt = self.cRRRG.processEntity()
            self.assertTrue(rt)
            self.assertTrue(self.cRRRG.bin["sequences"])
            self.assertTrue(self.cRRRG.bin["instance_ids"])
            # Write to output file
            output_file = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_processEntity.json")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(self.cRRRG.bin["sequences"], f, indent=2)
            logger.info("Wrote data to output file %s", output_file)

    def testFetchInstance(self):
        """
        Test fetchInstance on several PDB IDs.
        """
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            db = client[self.__databaseName]
            self.cRRRG.bin["instance_ids"] = ["2ANR.B", "2OR2.A"]
            # self.cRRRG.bin["instance_ids"] = ["3NIR.A"]
            rt = self.cRRRG.fetchInstance(db)
            self.assertTrue(rt)
            self.assertTrue(self.cRRRG.bin["instances"])
            # Write to output file
            output_file = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_fetchInstance.json")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(self.cRRRG.bin["instances"], f, indent=2)
            logger.info("Wrote data to output file %s", output_file)

    def testProcessInstance(self):
        """
        Test processInstance on several PDB IDs.
        """
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            db = client[self.__databaseName]
            self.cRRRG.bin["instance_ids"] = ["2ANR.B", "2OR2.A"]
            # self.cRRRG.bin["instance_ids"] = ["3NIR.A"]
            self.cRRRG.fetchInstance(db)
            rt = self.cRRRG.processInstance()
            self.assertTrue(rt)
            self.assertTrue(self.cRRRG.bin["metrics"])
            self.assertTrue(self.cRRRG.bin["fragments_start"])
            # Write to output file
            output_file1 = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_processInstance1.json")
            with open(output_file1, "w", encoding="utf-8") as f:
                json.dump(self.cRRRG.bin["metrics"], f, indent=2)
            output_file2 = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_processInstance2.json")
            with open(output_file2, "w", encoding="utf-8") as f:
                json.dump(self.cRRRG.bin["fragments_start"], f, indent=2)
            logger.info("Wrote data to output files %s, %s", output_file1, output_file2)

    def testProcessResidue(self):
        """
        Test processResidue on several PDB IDs.
        """
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            db = client[self.__databaseName]
            l_entry_id = ["2ANR", "2OR2"]
            # l_entry_id = ["3NIR"]
            self.cRRRG.bin["entry_ids"] = l_entry_id
            self.cRRRG.fetchEntity(db)
            self.cRRRG.processEntity()
            self.cRRRG.fetchInstance(db)
            self.cRRRG.processInstance()
            self.cRRRG.processResidue()
            self.assertTrue(self.cRRRG.bin["residues"])
            logger.info(self.cRRRG.bin["tracking"])
            # Write to output file
            output_file = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_processResidue.json")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(self.cRRRG.bin["residues"], f, indent=2)
            logger.info("Wrote data to output files %s", output_file)
            output_file_tracking = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_processResidueTracking.tsv")
            self.cRRRG.writeTracking(output_file_tracking)
            logger.info("Wrote tracking data to output files %s", output_file_tracking)

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
            logger.info(self.cRRRG.bin["tracking"])
            self.cRRRG.calculatePercentiles()
            self.assertTrue(self.cRRRG.data)
            # Write to output file
            output_file = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_calculatePercentiles.json")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(self.cRRRG.data, f, indent=2)
            logger.info("Wrote data to output files %s", output_file)

    def testGenerateBin(self):
        """
        Test generateBin on several PDB IDs.
        """
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            db = client[self.__databaseName]
            self.cRRRG.generateBin(db, [0.1, 0.5])
            # self.cRRRG.generateBin([1, 1.1])
            self.assertTrue(self.cRRRG.data)
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
        self.cRRRG.generate([1.9, 2.1])
        output_file = os.path.join(self.__cachePath, "rscc-thresholds.json")
        self.assertTrue(os.path.exists(output_file), "Output JSON file does not exist.")
        output_file_tsv = os.path.join(self.__cachePath, "testResidueRsccReferenceGenerator_generate.tsv")
        self.cRRRG.writeReviewReference(output_file_tsv)
        logger.info("Wrote data to output tsv files %s", output_file_tsv)


def genRsccRef():
    suiteSelect = unittest.TestSuite()
    # suiteSelect.addTest(ResidueRsccReferenceGeneratorTests("testFetchEntry"))
    # suiteSelect.addTest(ResidueRsccReferenceGeneratorTests("testFetchEntity"))
    # suiteSelect.addTest(ResidueRsccReferenceGeneratorTests("testProcessEntity"))
    # suiteSelect.addTest(ResidueRsccReferenceGeneratorTests("testFetchInstance"))
    # suiteSelect.addTest(ResidueRsccReferenceGeneratorTests("testProcessInstance"))
    # suiteSelect.addTest(ResidueRsccReferenceGeneratorTests("testProcessResidue"))
    # suiteSelect.addTest(ResidueRsccReferenceGeneratorTests("testCalculatePercentiles"))
    # suiteSelect.addTest(ResidueRsccReferenceGeneratorTests("testGenerateBin"))
    suiteSelect.addTest(ResidueRsccReferenceGeneratorTests("testGenerate"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = genRsccRef()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
