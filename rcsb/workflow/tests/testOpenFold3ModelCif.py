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

import gzip
import logging
import os
import shutil
import tempfile
import unittest

import modelcif.reader
from mmcif.io.BinaryCifReader import BinaryCifReader
from rcsb.workflow.modelcif.OpenFold3ModelCif import ModelCifConversionError, convertOpenFold3Outputs, getRankedSamples, roundToStep

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TEST_DATA = os.path.join(HERE, "test-data", "openfold3")
NUM_RESIDUES = 4
SAMPLE_PREFIX = "1a6y_A_seed_2746317213_sample_"


class OpenFold3ModelCifTests(unittest.TestCase):
    def setUp(self):
        self.outDir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.outDir)

    def testRankedSamples(self):
        """Samples are ranked best first by the chosen score, in either order."""
        queryDir = os.path.join(TEST_DATA, "1a6y_A")
        ranked = getRankedSamples(queryDir)
        self.assertEqual([os.path.basename(m) for m, _, _ in ranked], [SAMPLE_PREFIX + "4_model.cif", SAMPLE_PREFIX + "3_model.cif"])
        self.assertAlmostEqual(ranked[0][2]["sample_ranking_score"], 0.198238)
        # sample 3 has less disorder, so it comes first when lower is better
        ranked = getRankedSamples(queryDir, rankBy="disorder", descending=False)
        self.assertTrue(ranked[0][0].endswith(SAMPLE_PREFIX + "3_model.cif"))
        with self.assertRaises(KeyError):
            getRankedSamples(queryDir, rankBy="no_such_score")

    def testConvert(self):
        converted = convertOpenFold3Outputs(TEST_DATA, self.outDir, "0.5", "of3-ob-2025-06-30-174k.pt")
        self.assertEqual(converted, ["1a6y_A"])
        # only the two BinaryCIF files are written
        self.assertEqual(sorted(os.listdir(self.outDir)), ["1a6y_A_model.bcif.gz", "1a6y_A_pae.bcif.gz"])

        [model] = BinaryCifReader().deserialize(os.path.join(self.outDir, "1a6y_A_model.bcif.gz"))
        [pae] = BinaryCifReader().deserialize(os.path.join(self.outDir, "1a6y_A_pae.bcif.gz"))

        # PAE is only in the associated file, which the model file links to
        self.assertIsNone(model.getObj("ma_qa_metric_local_pairwise"))
        self.assertEqual(pae.getObj("ma_qa_metric_local_pairwise").getRowCount(), NUM_RESIDUES * NUM_RESIDUES)
        self.assertEqual(pae.getObj("entry_link").getValue("entry_id", 0), "1A6Y_A")
        files = model.getObj("ma_entry_associated_files")
        self.assertEqual(files.getValue("file_url", 0), "1a6y_A_pae.bcif.gz")
        self.assertEqual(files.getValue("file_format", 0), "bcif")

        # per-residue pLDDT is the mean of the residue's per-atom values (B-factors)
        local = model.getObj("ma_qa_metric_local")
        self.assertEqual(local.getRowCount(), NUM_RESIDUES)
        atomSite = model.getObj("atom_site")
        firstResidue = [float(atomSite.getValue("B_iso_or_equiv", i)) for i in range(atomSite.getRowCount()) if str(atomSite.getValue("label_seq_id", i)) == "1"]
        self.assertAlmostEqual(float(local.getValue("metric_value", 0)), sum(firstResidue) / len(firstResidue), places=2)

        # the model file is valid ModelCIF for python-modelcif
        with gzip.open(os.path.join(self.outDir, "1a6y_A_model.bcif.gz"), "rb") as fh:
            systems = modelcif.reader.read(fh, format="BCIF")
        self.assertEqual(len(systems), 1)
        system = systems[0]
        self.assertEqual(system.title, "OpenFold3 prediction of 1a6y_A")
        self.assertEqual(len(system.repositories), 1)

    def testNestedQueryDirs(self):
        """Query directories are found at any depth, e.g. under one directory per job."""
        runDir = os.path.join(self.outDir, "run")
        shutil.copytree(TEST_DATA, os.path.join(runDir, "job_0"))
        converted = convertOpenFold3Outputs(runDir, os.path.join(self.outDir, "out"), "0.5", "ckpt")
        self.assertEqual(converted, ["1a6y_A"])
        self.assertEqual(sorted(os.listdir(os.path.join(self.outDir, "out"))), ["1a6y_A_model.bcif.gz", "1a6y_A_pae.bcif.gz"])

    def testQueryWithoutSamples(self):
        """A query directory without any OpenFold3 samples fails, after the other queries are converted."""
        runDir = os.path.join(self.outDir, "run")
        shutil.copytree(TEST_DATA, runDir)
        os.makedirs(os.path.join(runDir, "9zzz_A", "seed_1"))
        outDir = os.path.join(self.outDir, "out")
        with self.assertRaises(ModelCifConversionError) as ctx:
            convertOpenFold3Outputs(runDir, outDir, "0.5", "ckpt")
        self.assertEqual(list(ctx.exception.errors), ["9zzz_A"])
        self.assertIn("no OpenFold3 sample outputs", str(ctx.exception))
        self.assertEqual(sorted(os.listdir(outDir)), ["1a6y_A_model.bcif.gz", "1a6y_A_pae.bcif.gz"])

    def __twoSampleRun(self):
        """Copy of the test data where sample 3 also has a model and full confidences (those of sample 4)."""
        runDir = os.path.join(self.outDir, "run")
        shutil.copytree(TEST_DATA, runDir)
        seedDir = os.path.join(runDir, "1a6y_A", "seed_2746317213")
        for suffix in ("_model.cif", "_confidences.json"):
            shutil.copy(os.path.join(seedDir, SAMPLE_PREFIX + "4" + suffix), os.path.join(seedDir, SAMPLE_PREFIX + "3" + suffix))
        return runDir

    def testAllModels(self):
        """With numModels 0, every sample becomes a ranked model, and the PAE file holds the PAE of each."""
        outDir = os.path.join(self.outDir, "out")
        converted = convertOpenFold3Outputs(self.__twoSampleRun(), outDir, "0.5", "ckpt", numModels=0)
        self.assertEqual(converted, ["1a6y_A"])
        [model] = BinaryCifReader().deserialize(os.path.join(outDir, "1a6y_A_model.bcif.gz"))
        modelList = model.getObj("ma_model_list")
        self.assertEqual(modelList.getRowCount(), 2)
        self.assertIn("seed 2746317213 sample 4, rank 1", modelList.getValue("model_name", 0))
        self.assertIn("seed 2746317213 sample 3, rank 2", modelList.getValue("model_name", 1))
        self.assertEqual(model.getObj("ma_qa_metric_local").getRowCount(), 2 * NUM_RESIDUES)
        [pae] = BinaryCifReader().deserialize(os.path.join(outDir, "1a6y_A_pae.bcif.gz"))
        pairwise = pae.getObj("ma_qa_metric_local_pairwise")
        self.assertEqual(pairwise.getRowCount(), 2 * NUM_RESIDUES * NUM_RESIDUES)
        self.assertEqual(sorted({str(pairwise.getValue("model_id", i)) for i in range(pairwise.getRowCount())}), ["1", "2"])

    def testSkipPae(self):
        """Without PAE, only the model file is written, with no associated file and no full confidences needed."""
        runDir = os.path.join(self.outDir, "run")
        shutil.copytree(TEST_DATA, runDir)
        os.remove(os.path.join(runDir, "1a6y_A", "seed_2746317213", SAMPLE_PREFIX + "4_confidences.json"))
        outDir = os.path.join(self.outDir, "out")
        converted = convertOpenFold3Outputs(runDir, outDir, "0.5", "ckpt", withPae=False)
        self.assertEqual(converted, ["1a6y_A"])
        self.assertEqual(os.listdir(outDir), ["1a6y_A_model.bcif.gz"])
        [model] = BinaryCifReader().deserialize(os.path.join(outDir, "1a6y_A_model.bcif.gz"))
        self.assertIsNone(model.getObj("ma_entry_associated_files"))
        self.assertEqual(model.getObj("ma_qa_metric_local").getRowCount(), NUM_RESIDUES)

    def testRoundToStep(self):
        """Values round half up to multiples of the step, without float noise."""
        self.assertEqual(roundToStep(0.25, 0.5), 0.5)
        self.assertEqual(roundToStep(0.74, 0.5), 0.5)
        self.assertEqual(roundToStep(31.53, 0.5), 31.5)
        self.assertEqual(roundToStep(0.31, 0.1), 0.3)
        self.assertEqual(roundToStep(3.17, 0.01), 3.17)
        self.assertEqual(roundToStep(13.5, 1), 14)

    def __paeValues(self, outDir):
        [pae] = BinaryCifReader().deserialize(os.path.join(outDir, "1a6y_A_pae.bcif.gz"))
        pairwise = pae.getObj("ma_qa_metric_local_pairwise")
        return [float(pairwise.getValue("metric_value", i)) for i in range(NUM_RESIDUES)]

    def testPaeStep(self):
        """PAE is rounded to multiples of paeStep (0.5 by default), which the PAE metric description records."""
        # first PAE row of the test data: 0.5, 0.8, 1.1, 1.4
        converted = convertOpenFold3Outputs(TEST_DATA, self.outDir, "0.5", "ckpt")
        self.assertEqual(converted, ["1a6y_A"])
        self.assertEqual(self.__paeValues(self.outDir), [0.5, 1.0, 1.0, 1.5])
        [model] = BinaryCifReader().deserialize(os.path.join(self.outDir, "1a6y_A_model.bcif.gz"))
        metrics = model.getObj("ma_qa_metric")
        descriptions = [metrics.getValue("description", i) for i in range(metrics.getRowCount())]
        self.assertIn("Predicted aligned error, in Angstrom, rounded to multiples of 0.5", descriptions)

        outDir = os.path.join(self.outDir, "out")
        converted = convertOpenFold3Outputs(TEST_DATA, outDir, "0.5", "ckpt", paeStep=0.01)
        self.assertEqual(converted, ["1a6y_A"])
        self.assertEqual(self.__paeValues(outDir), [0.5, 0.8, 1.1, 1.4])

    def testNoGzip(self):
        """Without gzip, plain .bcif files are written, and the model file links the plain PAE file."""
        converted = convertOpenFold3Outputs(TEST_DATA, self.outDir, "0.5", "ckpt", gzipped=False)
        self.assertEqual(converted, ["1a6y_A"])
        self.assertEqual(sorted(os.listdir(self.outDir)), ["1a6y_A_model.bcif", "1a6y_A_pae.bcif"])
        with open(os.path.join(self.outDir, "1a6y_A_pae.bcif"), "rb") as fh:
            self.assertNotEqual(fh.read(2), b"\x1f\x8b")
        [model] = BinaryCifReader().deserialize(os.path.join(self.outDir, "1a6y_A_model.bcif"))
        self.assertEqual(model.getObj("ma_entry_associated_files").getValue("file_url", 0), "1a6y_A_pae.bcif")

    def testUnknownRankScore(self):
        """Ranking by a score OpenFold3 does not report fails the query, with the cause chained."""
        with self.assertRaises(ModelCifConversionError) as ctx:
            convertOpenFold3Outputs(TEST_DATA, os.path.join(self.outDir, "out"), "0.5", "ckpt", rankBy="no_such_score")
        self.assertEqual(list(ctx.exception.errors), ["1a6y_A"])
        self.assertIsInstance(ctx.exception.__cause__, KeyError)


def suiteOpenFold3ModelCif():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(OpenFold3ModelCifTests("testRankedSamples"))
    suiteSelect.addTest(OpenFold3ModelCifTests("testConvert"))
    suiteSelect.addTest(OpenFold3ModelCifTests("testNestedQueryDirs"))
    suiteSelect.addTest(OpenFold3ModelCifTests("testQueryWithoutSamples"))
    suiteSelect.addTest(OpenFold3ModelCifTests("testAllModels"))
    suiteSelect.addTest(OpenFold3ModelCifTests("testSkipPae"))
    suiteSelect.addTest(OpenFold3ModelCifTests("testUnknownRankScore"))
    suiteSelect.addTest(OpenFold3ModelCifTests("testRoundToStep"))
    suiteSelect.addTest(OpenFold3ModelCifTests("testPaeStep"))
    suiteSelect.addTest(OpenFold3ModelCifTests("testNoGzip"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = suiteOpenFold3ModelCif()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
