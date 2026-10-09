##
# File:    testOpenFold3ModelCif.py
# Author:  Jose Duarte
# Date:    9-Oct-2026
##

"""
Test conversion of OpenFold3 predictions to ModelCIF BinaryCIF files.

The test data is an OpenFold3 0.5 output for 1a6y_A cut down to its first 4 residues:
two samples' aggregated confidences, and the model and full confidences of the
top-ranked one. Its PAE matrix is synthetic.
"""

__docformat__ = "google en"
__author__ = "Jose Duarte"
__email__ = "jose.duarte@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
import shutil
import tempfile
import unittest

import modelcif.reader
from mmcif.io.BinaryCifReader import BinaryCifReader
from rcsb.workflow.modelcif.OpenFold3ModelCif import convertOpenFold3Outputs, getTopSample

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TEST_DATA = os.path.join(HERE, "test-data", "openfold3")
NUM_RESIDUES = 4


class OpenFold3ModelCifTests(unittest.TestCase):
    def setUp(self):
        self.outDir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.outDir)

    def testTopSample(self):
        """The sample with the highest ranking score is chosen, not the first one."""
        queryDir = os.path.join(TEST_DATA, "1a6y_A")
        modelCifPath, _, scores = getTopSample(queryDir)
        self.assertTrue(modelCifPath.endswith("_sample_4_model.cif"))
        self.assertAlmostEqual(scores["sample_ranking_score"], 0.198238)

    def testConvert(self):
        failed = convertOpenFold3Outputs(TEST_DATA, self.outDir, "0.5", "of3-ob-2025-06-30-174k.pt")
        self.assertEqual(failed, [])
        # only the two BinaryCIF files are written
        self.assertEqual(sorted(os.listdir(self.outDir)), ["1a6y_A_model.bcif", "1a6y_A_pae.bcif"])

        [model] = BinaryCifReader().deserialize(os.path.join(self.outDir, "1a6y_A_model.bcif"))
        [pae] = BinaryCifReader().deserialize(os.path.join(self.outDir, "1a6y_A_pae.bcif"))

        # PAE is only in the associated file, which the model file links to
        self.assertIsNone(model.getObj("ma_qa_metric_local_pairwise"))
        self.assertEqual(pae.getObj("ma_qa_metric_local_pairwise").getRowCount(), NUM_RESIDUES * NUM_RESIDUES)
        self.assertEqual(pae.getObj("entry_link").getValue("entry_id", 0), "1A6Y_A")
        files = model.getObj("ma_entry_associated_files")
        self.assertEqual(files.getValue("file_url", 0), "1a6y_A_pae.bcif")
        self.assertEqual(files.getValue("file_format", 0), "bcif")

        # per-residue pLDDT is the mean of the residue's per-atom values (B-factors)
        local = model.getObj("ma_qa_metric_local")
        self.assertEqual(local.getRowCount(), NUM_RESIDUES)
        atomSite = model.getObj("atom_site")
        firstResidue = [float(atomSite.getValue("B_iso_or_equiv", i)) for i in range(atomSite.getRowCount()) if str(atomSite.getValue("label_seq_id", i)) == "1"]
        self.assertAlmostEqual(float(local.getValue("metric_value", 0)), sum(firstResidue) / len(firstResidue), places=2)

        # the model file is valid ModelCIF for python-modelcif
        with open(os.path.join(self.outDir, "1a6y_A_model.bcif"), "rb") as fh:
            systems = modelcif.reader.read(fh, format="BCIF")
        self.assertEqual(len(systems), 1)
        system = systems[0]
        self.assertEqual(system.title, "OpenFold3 prediction of 1a6y_A")
        self.assertEqual(len(system.repositories), 1)

    def testNestedQueryDirs(self):
        """Query directories are found at any depth, e.g. under one directory per job."""
        runDir = os.path.join(self.outDir, "run")
        shutil.copytree(TEST_DATA, os.path.join(runDir, "job_0"))
        failed = convertOpenFold3Outputs(runDir, os.path.join(self.outDir, "out"), "0.5", "ckpt")
        self.assertEqual(failed, [])
        self.assertEqual(sorted(os.listdir(os.path.join(self.outDir, "out"))), ["1a6y_A_model.bcif", "1a6y_A_pae.bcif"])

    def testQueryWithoutSamples(self):
        """A query directory without any OpenFold3 samples is reported as failed."""
        runDir = os.path.join(self.outDir, "run")
        os.makedirs(os.path.join(runDir, "9zzz_A", "seed_1"))
        failed = convertOpenFold3Outputs(runDir, os.path.join(self.outDir, "out"), "0.5", "ckpt")
        self.assertEqual(failed, ["9zzz_A"])


def suiteOpenFold3ModelCif():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(OpenFold3ModelCifTests("testTopSample"))
    suiteSelect.addTest(OpenFold3ModelCifTests("testConvert"))
    suiteSelect.addTest(OpenFold3ModelCifTests("testNestedQueryDirs"))
    suiteSelect.addTest(OpenFold3ModelCifTests("testQueryWithoutSamples"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = suiteOpenFold3ModelCif()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
