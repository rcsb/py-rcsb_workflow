##
#  File:           testResidueRsccReferenceGeneratorPrefetch.py
#  Date:           24-Nov-2026 dwp
#
#  Update:
##
"""
Unit tests for the prefetch path of ResidueRsccReferenceGenerator (prefetchAll and the selectBin* methods):
the results must be identical to the per-bin fetch path (fetchEntry/fetchEntity/fetchInstance).

The tests load a small synthetic pdbx_core-like database (entry, polymer entity and polymer entity instance
collections) into the test MongoDB, with cases that exercise the bin selection and filtering, and drop it again.
"""
import logging
import os
import random
import time
import unittest

from rcsb.db.mongo.Connection import Connection
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.workflow.refstats.ResidueRsccReferenceGenerator import ResidueRsccReferenceGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))

# non-hydrogen atom counts used by ResidueRsccReferenceGenerator.processResidueOneEntry()
NUM_ATOMS = {
    "ALA": 6, "ARG": 12, "ASN": 9, "ASP": 9, "CYS": 7, "GLN": 10, "GLU": 10, "GLY": 5, "HIS": 11, "ILE": 9,
    "LEU": 9, "LYS": 10, "MET": 9, "PHE": 12, "PRO": 8, "SER": 7, "THR": 8, "TRP": 15, "TYR": 13, "VAL": 8, "MSE": 9,
}
ONE_TO_THREE = {
    "A": "ALA", "R": "ARG", "N": "ASN", "D": "ASP", "C": "CYS", "Q": "GLN", "E": "GLU", "G": "GLY", "H": "HIS", "I": "ILE",
    "L": "LEU", "K": "LYS", "M": "MET", "F": "PHE", "P": "PRO", "S": "SER", "T": "THR", "W": "TRP", "Y": "TYR", "V": "VAL",
}


class ResidueRsccReferenceGeneratorPrefetchTests(unittest.TestCase):
    def setUp(self):
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName="site_info_configuration", mockTopPath=self.__mockTopPath)
        self.__resourceName = "MONGO_DB"
        self.__databaseName = "test_rscc_prefetch"
        self.__collectionNames = ["pdbx_core_entry", "pdbx_core_polymer_entity", "pdbx_core_polymer_entity_instance"]
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE-rscc-prefetch")
        os.makedirs(self.__cachePath, exist_ok=True)
        self.__startTime = time.time()
        logger.info("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        logger.info("Completed %s (%.4f seconds)", self.id(), time.time() - self.__startTime)

    def testInResolutionBin(self):
        """inResolutionBin() follows MongoDB's {"$gte": high, "$lt": low} matching on scalars and arrays."""
        inBin = ResidueRsccReferenceGenerator.inResolutionBin
        self.assertTrue(inBin(1.0, 1.0, 1.1))  # lower bound inclusive
        self.assertFalse(inBin(1.1, 1.0, 1.1))  # upper bound exclusive
        self.assertTrue(inBin(2, 1.9, 2.1))  # int
        self.assertTrue(inBin([1.05], 1.0, 1.1))
        self.assertFalse(inBin([], 1.0, 1.1))
        # array: some element >= high and some (possibly other) element < low
        self.assertTrue(inBin([0.5, 3.0], 1.0, 1.1))
        self.assertTrue(inBin([1.05, 1.25], 1.1, 1.2))
        self.assertFalse(inBin([0.5, 0.9], 1.0, 1.1))
        self.assertFalse(inBin([1.2, 1.3], 1.0, 1.1))
        # only numbers compare
        for value in (None, True, "1.05", {"value": 1.05}, [None, "1.05", False]):
            self.assertFalse(inBin(value, 1.0, 1.1), value)

    def testPrefetchMatchesPerBinFetch(self):
        """generate() via prefetchAll gives the same percentiles and counts as the per-bin fetch path."""
        resolutionRange = [1.0, 1.3]
        binL = [[1.0, 1.1], [1.1, 1.2], [1.2, 1.3]]
        try:
            self.__loadSyntheticDatabase()
            # per-bin fetch path (prefetched data absent)
            genOld = self.__makeGenerator("old")
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                db = client[self.__databaseName]
                for resolutionBin in binL:
                    genOld.generateBin(db, resolutionBin)
                    genOld.resolution_bin = {}
            # prefetch path
            genNew = self.__makeGenerator("new")
            ok = genNew.generate(resolution_range=resolutionRange)
            self.assertTrue(ok)
            self.assertIsNone(genNew.prefetched)  # released after generate()
            #
            self.assertEqual(genNew.data_rscc, genOld.data_rscc)
            self.assertEqual(genNew.data_ref, genOld.data_ref)
            # every bin got data, and the multi-bin entry was counted in each bin it matches
            for resolutionBin in binL:
                self.assertIn("[%s,%s)" % tuple(resolutionBin), genNew.data_ref["ALA"])
            # the selection matched MongoDB's: same entry IDs per bin from both paths
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                db = client[self.__databaseName]
                genNew.prefetchAll(db)
                for resolutionBin in binL:
                    genOld.resolution_bin = {}
                    genOld.fetchEntry(db, resolutionBin)
                    genNew.resolution_bin = {}
                    genNew.selectBinEntries(resolutionBin)
                    self.assertEqual(sorted(genNew.resolution_bin["entry_ids"]), sorted(genOld.resolution_bin["entry_ids"]))
                    self.assertIn("9MUL", genNew.resolution_bin["entry_ids"])
                    self.assertNotIn("9NMR", genNew.resolution_bin["entry_ids"])
                    self.assertNotIn("9NOR", genNew.resolution_bin["entry_ids"])
                instances = genNew.prefetched["instances"]
                self.assertIn("9NOF.A", instances)  # instances without RSCC features are kept
                self.assertNotIn("9DNA.B", instances)  # instances of non-protein entities are not
                self.assertNotIn("9NMR.A", instances)  # nor those of non-X-ray entries
        finally:
            self.__dropSyntheticDatabase()

    def __makeGenerator(self, label):
        return ResidueRsccReferenceGenerator(
            self.__cfgOb,
            cachePath=os.path.join(self.__cachePath, label),
            databaseName=self.__databaseName,
            collectionNames=self.__collectionNames,
        )

    def __dropSyntheticDatabase(self):
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            client.drop_database(self.__databaseName)

    def __loadSyntheticDatabase(self):
        """Small pdbx_core-like data set: X-ray protein entries spread over the 1.0-1.3 bins plus edge cases."""
        rnd = random.Random(20261124)
        entryL, entityL, instanceL = [], [], []
        sequence = "MACDEFGHIKLMNPQRSTVWY" * 3 + "(MSE)GA(SEP)K"

        def addEntry(entryId, method, resolution, nEntities=1, nChains=2, polyType="polypeptide(L)", features=True):
            entryInfo = {"experimental_method": method}
            if resolution is not None:
                entryInfo["resolution_combined"] = resolution
            entryL.append({"rcsb_id": entryId, "rcsb_entry_info": entryInfo})
            chainIdx = 0
            for entityNum in range(1, nEntities + 1):
                asymIds = [chr(ord("A") + chainIdx + ii) for ii in range(nChains)]
                chainIdx += nChains
                entityL.append(
                    {
                        "rcsb_id": "%s_%d" % (entryId, entityNum),
                        "rcsb_polymer_entity_container_identifiers": {"entry_id": entryId, "entity_id": str(entityNum), "asym_ids": asymIds},
                        "entity_poly": {"type": polyType, "pdbx_seq_one_letter_code": sequence},
                    }
                )
                for asymId in asymIds:
                    instanceL.append({"rcsb_id": "%s.%s" % (entryId, asymId), "rcsb_polymer_instance_feature": makeFeatures(features)})

        def makeFeatures(withRscc):
            residues = []
            ii = 0
            while ii < len(sequence):
                if sequence[ii] == "(":
                    jj = sequence.index(")", ii)
                    residues.append(sequence[ii + 1: jj])
                    ii = jj + 1
                else:
                    residues.append(ONE_TO_THREE[sequence[ii]])
                    ii += 1
            beg = rnd.randint(1, 3)  # first modelled residue
            resL = residues[beg - 1:]
            rscc = [round(rnd.uniform(0.5, 1.0), 3) for _ in resL]
            occupancy = [1.0 if rnd.random() > 0.1 else 0.5 for _ in resL]
            natoms = [NUM_ATOMS.get(r, 10) - (2 if rnd.random() < 0.1 else 0) for r in resL]
            other = {"type": "UNOBSERVED_RESIDUE_XYZ", "feature_positions": [{"beg_seq_id": 1, "beg_comp_id": residues[0], "end_seq_id": 2}]}
            if not withRscc:
                return [other]

            def feature(fType, values):
                return {"type": fType, "feature_positions": [{"beg_seq_id": beg, "beg_comp_id": resL[0], "values": values}]}
            return [other, feature("RSCC", rscc), feature("NATOMS_EDS", natoms), feature("AVERAGE_OCCUPANCY", occupancy)]

        # regular entries in each bin
        for ii, resolution in enumerate([1.0, 1.02, 1.08, 1.1, 1.15, 1.19, 1.2, 1.25, 1.29]):
            addEntry("9X%02d" % ii, "X-ray", [resolution], nEntities=1 + ii % 2, nChains=1 + ii % 3)
        addEntry("9SCA", "X-ray", 1.12)  # scalar resolution
        addEntry("9MUL", "X-ray", [1.05, 1.25])  # matches all three bins (MongoDB array semantics)
        addEntry("9NOF", "X-ray", [1.15], features=False)  # instance without RSCC/NATOMS/OCCUPANCY features
        addEntry("9DNA", "X-ray", [1.15], polyType="polydeoxyribonucleotide")  # non-protein entity
        addEntry("9NMR", "SOLUTION NMR", [1.15])  # not X-ray
        addEntry("9NOR", "X-ray", None)  # no resolution
        addEntry("9OUT", "X-ray", [2.5])  # outside the tested range
        rnd.shuffle(entryL)  # load order differs from ID order
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            client.drop_database(self.__databaseName)
            db = client[self.__databaseName]
            db[self.__collectionNames[0]].insert_many(entryL)
            db[self.__collectionNames[1]].insert_many(entityL)
            db[self.__collectionNames[2]].insert_many(instanceL)
            db[self.__collectionNames[1]].create_index("rcsb_polymer_entity_container_identifiers.entry_id")
            db[self.__collectionNames[2]].create_index("rcsb_id")
        logger.info("Loaded %d entries, %d entities, %d instances", len(entryL), len(entityL), len(instanceL))


def suitePrefetch():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ResidueRsccReferenceGeneratorPrefetchTests("testInResolutionBin"))
    suiteSelect.addTest(ResidueRsccReferenceGeneratorPrefetchTests("testPrefetchMatchesPerBinFetch"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = suitePrefetch()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
