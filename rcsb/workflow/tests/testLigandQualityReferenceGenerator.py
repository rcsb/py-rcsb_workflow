##
#  File:           testLigandQualityReferenceGenerator.py
#  Date:           2025-12-25 Chenghua Shao
#
#  Update:
##
"""
Unit tests for LigandQualityReferenceGenerator.py
"""
import json
import logging
import os
import platform
import resource
import time
import unittest
from rcsb.workflow.stats.LigandQualityReferenceGenerator import LigandQualityReferenceGenerator
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class LigandQualityReferenceGeneratorTests(unittest.TestCase):
    skipFull = True

    def setUp(self):
        self.__isMac = platform.system() == "Darwin"
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")
        os.makedirs(self.__cachePath, exist_ok=True)
        if self.__isMac:
            self.__cfgOb = ConfigUtil(configPath=configPath,
                                      defaultSectionName="site_info_remote_configuration",
                                      mockTopPath=self.__mockTopPath)
            self.cRLRG = LigandQualityReferenceGenerator(self.__cfgOb,
                                                         cachePath=self.__cachePath,
                                                         databaseName="dw",
                                                         collectionName="core_nonpolymer_entity_instance")
        else:
            self.__cfgOb = ConfigUtil(configPath=configPath,
                                      defaultSectionName="site_info_configuration",
                                      mockTopPath=self.__mockTopPath)
            # self.__workflowFixture()
            self.cRLRG = LigandQualityReferenceGenerator(self.__cfgOb, cachePath=self.__cachePath)
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testFetchLigand(self):
        """
        Test fetchLigand on several PDB IDs.
        """
        pdb_ids = ["6WJC", "1C0T", "1DT4", "2HYV", "XXXX"]  # 1C0T with ligand, 1DT4 without ligand, XXXX invalid ID
        self.cRLRG.fetchLigand(pdb_ids)
        self.assertTrue(self.cRLRG.qdata)
        response = self.cRLRG.qdata
        logger.info("query on %s with response %s", pdb_ids, response)
        l_pdb_id = []
        for d_lig in response:
            pdb_id = d_lig["pdb_ligand"].split("-")[0]
            l_pdb_id.append(pdb_id)
        l_pdb_id = list(set(l_pdb_id))
        logger.info("PDB IDs with response: %s", l_pdb_id)
        self.assertIn("1C0T", l_pdb_id)
        self.assertIn("1C0T", l_pdb_id)
        self.assertNotIn("1DT4", l_pdb_id)  # no ligand present
        self.assertNotIn("2HYV", l_pdb_id)  # skip ion ligands without complete metrics
        self.assertNotIn("XXXX", l_pdb_id)  # invalid PDB ID
        # Write to output file
        output_file = os.path.join(self.__cachePath, "testLigandQualityReferenceGenerator_fetchLigand.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(self.cRLRG.qdata, f, indent=2)
        logger.info("Wrote data to output file %s", output_file)

    def testFetchLigandAll(self):
        """
        Test fetchLigand on All PDB structures.
        """
        self.cRLRG.fetchLigand()
        self.assertTrue(self.cRLRG.qdata)
        # Write to output file
        output_file = os.path.join(self.__cachePath, "testLigandQualityReferenceGenerator_fetchLigandAll.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(self.cRLRG.qdata, f, indent=2)
        logger.info("Wrote data to output file %s", output_file)

    def testAnalyzeLigand(self):
        """
        Test analyze function on several PDB IDs.
        """
        pdb_ids = ["1C0T", "1DT4", "6WJC", "4HHB"]
        self.cRLRG.fetchLigand(pdb_ids)
        self.cRLRG.analyzeLigand()
        data_analyzed = self.cRLRG.data
        self.assertTrue(data_analyzed)
        # Write to output file
        output_file = os.path.join(self.__cachePath, "testLigandQualityReferenceGenerator_analyzeLigand.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data_analyzed, f, indent=2)
        logger.info("Wrote data to output file %s", output_file)

    def testAnalyzeLigandAll(self):
        """
        Test analyze function on all PDB structures.
        """
        self.cRLRG.fetchLigand()
        self.cRLRG.analyzeLigand()
        data_analyzed = self.cRLRG.data
        self.assertTrue(data_analyzed)
        # Write to output file
        output_file = os.path.join(self.__cachePath, "testLigandQualityReferenceGenerator_analyzeLigandAll.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data_analyzed, f, indent=2)
        logger.info("Wrote data to output file %s", output_file)

    def testGenerate(self):
        """
        Test generate function that runs the full pipeline on several PDB IDs.
        """
        pdb_ids = ["1C0T", "1DT4", "6WJC", "4HHB"]
        self.cRLRG.generate(pdb_ids)
        self.assertTrue(self.cRLRG.data)
        # Write to output file
        output_file = os.path.join(self.__cachePath, "testLigandQualityReferenceGenerator_generate.json")
        with open(output_file, "w", encoding="utf-8") as file:
            json.dump(self.cRLRG.data, file, indent=2)
        logger.info("Wrote data to output file %s", output_file)
        # Write reference data to csv
        csv_output_file = os.path.join(self.__cachePath, "ligand_score_reference.csv")
        self.assertTrue(os.path.exists(csv_output_file), "Output CSV file does not exist.")

    def testGenerateAll(self):
        """
        Test generate function that runs the full pipeline on all PDB structures.
        """
        self.cRLRG.generate()
        self.assertTrue(self.cRLRG.data)
        # Write to output file
        output_file = os.path.join(self.__cachePath, "testLigandQualityReferenceGenerator_generateAll.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(self.cRLRG.data, f, indent=2)
        logger.info("Wrote data to output file %s", output_file)
        # Write reference data to csv
        csv_output_file = os.path.join(self.__cachePath, "ligand_score_reference.csv")
        self.assertTrue(os.path.exists(csv_output_file), "Output CSV file does not exist.")


def genLigandRef():
    suiteSelect = unittest.TestSuite()
    # suiteSelect.addTest(LigandQualityReferenceGeneratorTests("testFetchLigand"))
    # suiteSelect.addTest(LigandQualityReferenceGeneratorTests("testFetchLigandAll"))
    # suiteSelect.addTest(LigandQualityReferenceGeneratorTests("testAnalyzeLigand"))
    # suiteSelect.addTest(LigandQualityReferenceGeneratorTests("testAnalyzeLigandAll"))
    # suiteSelect.addTest(LigandQualityReferenceGeneratorTests("testGenerate"))
    suiteSelect.addTest(LigandQualityReferenceGeneratorTests("testGenerateAll"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = genLigandRef()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
