##
#  File:           ResidueRsccReferenceGenerator.py
#  Date:           2025-12-25 Chenghua Shao
#
#  Update:
##
"""
Generate RSCC reference for standard polymer residues for Mol* RSCC-based colored display.
Utilities to query RCSB MongoDB polymer-instance RSCC and related metrics,
aggregate them by residue type for X-ray entries in resolution bins, and
produce RSCC reference statistics (percentiles) for standard polymer residues.

"""
import os
import logging
import json
import numbers
import numpy as np
from pymongo.database import Database   # for type hint only

from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.io.StashableBase import StashableBase
from rcsb.db.mongo.Connection import Connection

logger = logging.getLogger(__name__)


class InvalidParametersError(Exception):
    """Custom exception class for errors in ResidueRsccReferenceGenerator parameter validation."""


class InvalidSequenceError(Exception):
    """Custom exception class for errors in sequence parsing and validation."""


class DatabaseError(Exception):
    """Custom exception class for errors in MongoDB connection and query execution."""


class OutputError(Exception):
    """Custom exception class for errors during output file writing."""


class ResidueRsccReferenceGenerator(StashableBase):
    """
    This module provides the class ResidueRsccReferenceGenerator which:
    - Connects to a configurable MongoDB instance (default database "pdbx_core").
    - Selects X-ray entries in a resolution bin.
    - Collects polymer entities, sequences and instance IDs.
    - Fetches per-instance features (RSCC, NATOMS_EDS, AVERAGE_OCCUPANCY).
    - Maps feature values to sequence ordinals and aggregates by residue type.
    - Computes percentiles and generates reference outputs.

    Each resolution bin is processed separately and non-parallely because of the large data volume
        Attributes:
            data: final output of RSCC percentile by residue type and resolution bin
            bin: dict to record data for the current resolution bin by its sub-keys:
                resolution: resolution bin
                entry_ids: PDB entry ids such as 2OR2
                entities: entity data with instance ids and sequences
                sequences: processed entity data to record residue identity by ordinal
                instance_ids: all instance ids such as 2OR2.A
                instances: instance data with RSCC, occupancy, etc
                metrics: processed instance data to record RSCC, occupancy etc by ordinal
                fragments_start: starting residue's identity for each fragment
                residues: filtered RSCC value array for each residue type
                tracking: record the number of residues present and selected for each entry
        Methods: the following methods work in tandem, except for the last two that consolidate all
            fetchEntry: query MongoDB to add self.resolution_bin["entry_id"] by resolution bin;
            fetchEntity: query MongoDB to add self.resolution_bin["entities"] by entry_id;
            processEntity: process self.resolution_bin["entities"] to add self.resolution_bin["sequences"] and self.resolution_bin["instance_ids"];
            fetchInstance: query MongoDB to add self.resolution_bin["instances"] by instance_id;
            processInstance: process self.resolution_bin["instances"] to add self.resolution_bin["metrics"] and self.resolution_bin["fragments_start];
            processResidue: map residue type to RSCC by sequence ordinal, filter by natoms and occupancy, to add self.resolution_bin["residues"] and self.resolution_bin["tracking"];
            calculatePercentile: calculate percentile for RSCC array;
            generateBin: consolidate all processes above to generate data for one resolution bin;
            generate: generate final data for all resolution bins.
    """
    def __init__(self, cfgOb, cachePath, **kwargs):
        """Initiate the class variables and the MongoDB connection"""
        self.__cfgOb = cfgOb
        self.__configName = kwargs.get("configName", "site_info_configuration")
        self.__dirName = "rscc-reference"
        super().__init__(cachePath, [self.__dirName])
        self.__dirPath = os.path.join(cachePath, self.__dirName)
        #
        self.__resourceName = "MONGO_DB"
        self.__databaseName = kwargs.get("databaseName", "pdbx_core")
        self.__collectionNames = kwargs.get(
            "collectionNames",
            ["pdbx_core_entry", "pdbx_core_polymer_entity", "pdbx_core_polymer_entity_instance"]
        )
        self.__collections = {}
        for collectionName in self.__collectionNames:
            collectionLevel = collectionName.split("_")[-1]  # level of entry, entity, instance
            self.__collections[collectionLevel] = collectionName
        # below is the alternate way to setup connection without context, saved as reference
        # conn = Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName)
        # conn.openConnection()
        # self.__client = conn.getClientConnection()
        # self.__db = self.__client[self.__databaseName]
        # self.__collections = {}
        # for collectionName in self.__collectionNames:
        #     collectionLevel = collectionName.split("_")[-1]  # level of entry, entity, instance
        #     self.__collections[collectionLevel] = self.__db[collectionName]
        #
        self.l_standard_residue = [
            "ALA", "ARG", "ASN", "ASP", "CYS", "GLN", "GLU", "GLY", "HIS", "ILE",
            "LEU", "LYS", "MET", "PHE", "PRO", "SER", "THR", "TRP", "TYR", "VAL",
            "MSE"
        ]  # 20 standard aa + MSE
        self.data_rscc = {}  # RSCC percentiles by residue type and resolution bin, formated for Mol*
        self.data_ref = {}  # optional reference, superset of self.data_rscc, for review purpose only
        for residue in self.l_standard_residue:
            self.data_rscc[residue] = {}
            self.data_ref[residue] = {}
        self.resolution_bin = {}  # store all data for the current resolution bin being worked on
        # self.resolution_bin is updated through each step of MongoDB data fetch and process to add values for
        # (1) MongoDB query results by keys of: "entry_ids", "entities",  "instances";
        # (2) Processed results by keys of: "instance_ids", "sequences", "residues", "metrics";
        # (3) Metadata by keys of: "resolution", "tracking".
        #

    def __getRcssRefDataPath(self):
        return os.path.join(self.__dirPath, "rscc-thresholds.json")

    def testCache(self):
        fU = FileUtil()
        rscc_data_file = self.__getRcssRefDataPath()
        #
        if self.data_rscc and fU.exists(rscc_data_file):
            logger.info("RSCC percentile data (%d) from file %s", len(self.data_rscc), rscc_data_file)
            if len(self.data_rscc) > 0:
                return True
        return False

    def generate(self, resolution_range: list[int] = None, backup: bool = False) -> bool:
        """
        Generate residue RSCC references for the entire PDB archive, throughtout bins of
        [0.1, 1.0], [1.0, 1.1], ..., [3.4,3.5], [3.5, 50], or within the optional resolution_range
        :param resolution_range: Two-element sequence ``[high_resolution, low_resolution]``
            specifying the half-open interval (``high_resolution <= resolution < low_resolution``).
        :type resolution_range: list[float] or list[int]
        :returns: True if all bins were processed successfully.
        :rtype: bool
        """
        # Set up resolution range
        if resolution_range:
            try:
                self.verifyResolution(resolution_range)
            except InvalidParametersError as e:
                logger.error("invalid resolution range of %s: %s", resolution_range, e)
                return False
            [high, low] = resolution_range
        else:
            high = 0.1  # minimum resolution by default
            low = 50  # maximum resolution by default
        if high < 1 and low > 3.5:
            l_range = [i / 10 for i in range(10, 36)]  # 0.1 increment from 1.0 to 3.5 resolution
            l_range.insert(0, high)
            l_range.append(low)
        elif high < 1:
            l_range = [i / 10 for i in range(10, int(low * 10) + 1)]
            l_range.insert(0, high)
        elif low > 3.5:
            l_range = [i / 10 for i in range(int(high * 10), 36)]
            l_range.append(low)
        else:
            l_range = [i / 10 for i in range(int(high * 10), int(low * 10) + 1)]
        # Construct all bins within the range by 0.1 increment
        l_bin = []
        for i in range(len(l_range) - 1):
            resol_bin = [l_range[i], l_range[i + 1]]
            l_bin.append(resol_bin)
        logger.info("to generate RSCC reference for %s resolution bins from %s to %s", len(l_bin), high, low)
        # Enumerate through each bin
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            db = client[self.__databaseName]
            for resol_bin in l_bin:
                try:
                    self.generateBin(db, resol_bin)
                except InvalidParametersError as e:
                    logger.error("Stop process resolution bin %s due to parameter ERROR %s", resol_bin, e)
                    return False
                except DatabaseError as e:
                    logger.error("Stop process resolution bin %s due to database ERROR %s", resol_bin, e)
                    return False
                except Exception as e:
                    logger.error("Stop process resolution bin %s due to other ERROR: %s", resol_bin, e)
                    return False
                self.resolution_bin = {}  # reset, empty self.resolution_bin data for the run on the next bin
        try:
            self.writeReference()
        except OutputError as e:
            logger.error("Failed to write ligand quality reference data to file, STOP. ERROR: %s", e)
            return False
        #
        logger.info("Finished generation workflow for RSCC reference data.")
        ok = True
        #
        if backup and self.testCache():
            logger.info("Backing up data to stash...")
            okB = self.backup(self.__cfgOb, self.__configName, useStash=True, useGit=False)
            logger.info("%r RSCC backup status (%r)", self.__dirName, okB)
            ok = ok and okB
        #
        return ok

    def generateBin(self, db: Database, resolution_bin: list[int]):
        """
        Generate the RSCC percentile reference of each standard residue for a given resolution bin.

        :param db: Instance of pymongo.database.Database class for pdbx_core database.
        :param resolution_bin: Two-element sequence ``[high_resolution, low_resolution]``
            specifying the half-open interval (``high_resolution <= resolution < low_resolution``).
        :type resolution_bin: list[float] or list[int]
        """
        self.fetchEntry(db, resolution_bin)
        self.fetchEntity(db)
        self.processEntity()
        self.fetchInstance(db)
        self.processInstance()
        self.processResidue()
        self.calculatePercentiles()
        logger.info("finished all RSCC data process for the resolution bin %s", resolution_bin)

    def fetchEntry(self, db: Database, resolution_bin: list[int]):
        """
        Fetch PDB entry IDs within a given resolution bin and store them in self.resolution_bin["entry_ids"].
        Perform MongoDB search by rcsb_entry_info.resolution_combined within the resolution bin
        :param db: Instance of pymongo.database.Database class for pdbx_core database.
        :param resolution_bin: Two-element sequence specifying the resolution bin as
            [high_resolution, low_resolution].  - high_resolution must be a number >= 0.
        :type resolution_bin: list[int] or list[float]
        Notes
        -----
        - The resolution interval is half-open: high_resolution <= resolution < low_resolution.
        """
        # Validate input data
        self.verifyResolution(resolution_bin)
        # Construct MongDB query
        logger.info("to fetch entry ID for resolution bin %s", resolution_bin)
        self.resolution_bin["resolution"] = resolution_bin
        [high, low] = resolution_bin
        collectionName = self.__collections["entry"]  # use core_entry collection
        collection = db[collectionName]
        d_condition = {
            "rcsb_entry_info.experimental_method": "X-ray",
            "rcsb_entry_info.resolution_combined": {
                "$gte": high, "$lt": low
            }
        }  # high <= bin < low
        # Run find
        try:
            cursor = collection.find(d_condition, {"_id": 0, "rcsb_id": 1})
            self.resolution_bin["entry_ids"] = [doc["rcsb_id"] for doc in cursor]  # collect IDs in a list only
            logger.info("%s PDB X-ray entries found within the resolution bin %s", len(self.resolution_bin["entry_ids"]), resolution_bin)
        except Exception as e:
            raise DatabaseError(f"fetchEntry failed to fetch data from MongoDB for resolution bin {resolution_bin}: {e}") from e

    def verifyResolution(self, resolution_bin: list[int]):
        """
        Verify the provided resolution bin, raise exception for any problem.
        :param resolution_bin: Two-element sequence specifying the resolution bin as
            [high_resolution, low_resolution].  - high_resolution must be a number >= 0.
        :type resolution_bin: list[int] or list[float]
        Notes
        -----
        - Input validation checks:
          * resolution_bin has length 2
          * both elements are numeric
          * high_resolution >= 0
          * low_resolution > high_resolution
        """
        if not resolution_bin:
            raise InvalidParametersError("the resolution bin is empty or None")
        if len(resolution_bin) != 2:
            raise InvalidParametersError("the resolution bin must be provided as a list of two numbers")
        for value in resolution_bin:
            if not isinstance(value, numbers.Number):
                raise InvalidParametersError("both elements of the resolution bin must be numbers")
        [high, low] = resolution_bin
        if high < 0:
            raise InvalidParametersError("high resolution, as the 1st value of the resolution bin, must be greater than zero")
        if low <= high:
            raise InvalidParametersError("high resolution, as the 1st value of the resolution bin, must be smaller than the 2nd")

    def fetchEntity(self, db: Database):
        """
        Fetch protein entities for entries in the current resolution bin and store them in self.resolution_bin["entities"].
        Perform MongoDB aggregation by entry_id, and returns the following fields for each matching entity document:
        - rcsb_id, e.g. "2OR2_1"
        - entry_id, e.g. "2OR2"
        - entity_id, e.g. "1"
        - asym_ids, e.g. ["A", "B"]
        - pdbx_seq_one_letter_code, e.g. "ASSVNELENWSKWMQPIPDNIPLARISIPGTHDSGT(MSE)A..."
        :param db: Instance of pymongo.database.Database class for pdbx_core database.
        """
        # Validate input data
        if ("entry_ids" not in self.resolution_bin) or (not self.resolution_bin["entry_ids"]):
            raise InvalidParametersError("fetchEntity failed, must run fetchEntry first to retrive valid PDB entry IDs")
        # Construct MongoBD aggregation
        logger.info("to fetch entity data for %s entries", len(self.resolution_bin["entry_ids"]))
        collectionName = self.__collections["entity"]  # use core_polymer_entity collection
        collection = db[collectionName]
        pipeline = [
            {
                "$match": {
                    "rcsb_polymer_entity_container_identifiers.entry_id": {"$in": self.resolution_bin["entry_ids"]},
                    "entity_poly.type": "polypeptide(L)"
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "rcsb_id": 1,
                    "entry_id": "$rcsb_polymer_entity_container_identifiers.entry_id",
                    "entity_id": "$rcsb_polymer_entity_container_identifiers.entity_id",
                    "asym_ids": "$rcsb_polymer_entity_container_identifiers.asym_ids",
                    "pdbx_seq_one_letter_code": "$entity_poly.pdbx_seq_one_letter_code"
                }
            }
        ]
        # Run aggregate
        try:
            self.resolution_bin["entities"] = list(collection.aggregate(pipeline))
            logger.info("%s entities retrived", len(self.resolution_bin["entities"]))
        except Exception as e:
            raise DatabaseError(f"fetchEntity failed to fetch data from MongoDB for resolution bin {self.resolution_bin['resolution']}: {e}") from e

    def processEntity(self):
        """
        Process entities stored in self.resolution_bin["entities"] and populate instance and sequence mappings.
        constructing:
        - self.resolution_bin["instance_ids"]: a flat list of instance identifiers in the form "entry_id.asym_id"
            for all asym IDs of X-ray protein entities in the bin, used for next MongoDB query.
        - self.resolution_bin["sequences"]: a nested mapping keyed first by entry_id then by entity_id, where
            each entity mapping contains:
                - "residue_ordinal": the output of residue identity for each ordinal index
                - "instance_ids": the list of instance identifiers (entry_id.asym_id) for the entity
        Side effects
        - Mutates self.resolution_bin by setting/updating:
            - self.resolution_bin["instance_ids"] (list of strings)
            - self.resolution_bin["sequences"] (dict: entry_id -> entity_id -> {"residue_ordinal", "instance_ids"})
        """
        # Validate input data
        if ("entities" not in self.resolution_bin) or (not self.resolution_bin["entities"]):
            raise InvalidParametersError("processEntity failed, must run fetchEntity first to retrive valid PDB entity data")
        n_all_entities = len(self.resolution_bin["entities"])
        logger.info("to process %s entities", n_all_entities)
        self.resolution_bin["instance_ids"] = []  # to record all instance ids for X-ray proteins of the resolution bin
        self.resolution_bin["sequences"] = {}  # key by entry id -> entity id, record each residue's identity
        n_success_entities = 0
        for d_entity in self.resolution_bin["entities"]:
            for key in ["entry_id", "entity_id", "asym_ids", "pdbx_seq_one_letter_code"]:
                if key not in d_entity:
                    raise InvalidParametersError(f"processEntity failed, key {key} not found in entities object")
                if not d_entity[key]:
                    raise InvalidParametersError(f"processEntity failed, empty value for key {key} in entities object")
            entry_id = d_entity["entry_id"]
            entity_id = d_entity["entity_id"]
            logger.debug("to process entity data of entity %s of entry %s", entity_id, entry_id)
            asym_ids = d_entity["asym_ids"]  # asym IDs as list
            sequence = d_entity["pdbx_seq_one_letter_code"]  # one-letter code sequence
            if entry_id not in self.resolution_bin["sequences"]:
                self.resolution_bin["sequences"][entry_id] = {}
            self.resolution_bin["sequences"][entry_id][entity_id] = {}
            self.resolution_bin["sequences"][entry_id][entity_id]["instance_ids"] = []
            for asym_id in asym_ids:
                instance_id = ".".join([entry_id, asym_id])
                self.resolution_bin["sequences"][entry_id][entity_id]["instance_ids"].append(instance_id)
                self.resolution_bin["instance_ids"].append(instance_id)
            try:
                d_residue_ordinal = self.getResidueByOrdinal(sequence)
                self.resolution_bin["sequences"][entry_id][entity_id]["residue_ordinal"] = d_residue_ordinal
                n_success_entities += 1
            except InvalidSequenceError as e:  # catch sequence error but do not stop process because it's not critical
                logger.error("invalid sequence: %s; %s", sequence, e)
                self.resolution_bin["sequences"][entry_id][entity_id]["residue_ordinal"] = {}
        logger.info("finished entity process success/total entities: %s/%s", n_success_entities, n_all_entities)

    def getResidueByOrdinal(self, sequence: str) -> dict:
        """
        Get a mapping from 1-based residue ordinals to residue identifiers parsed from a sequence string.
        This function interprets an input sequence string containing standard one-letter amino-acid codes
        and optional modified-residue annotations enclosed in parentheses. Standard one-letter codes are converted to their three-letter uppercase codes using an
        internal conversion table. Parenthesized residue annotations are captured verbatim (preserving
        case) and assigned as the residue identifier for a single ordinal.
        Supported behavior summary
        - Standard single-letter residues (case-insensitive) are mapped to three-letter codes (e.g. "A" -> "ALA").
        - Text between "(" and ")" is treated as a single modified residue and assigned to the next ordinal.
        - If a parenthesized modified residue exceeds 5 characters, an error is logged and an empty dict is returned.
        - Unknown single-letter codes (not present in the internal mapping) will raise a KeyError.
        Parameters
        :param sequence: The residue sequence to parse. May include whitespace and parenthesized modifications.
        :type sequence: str
        Returns
        :returns: A dictionary mapping 1-based residue ordinals (int) to residue identifiers (str). For
                  standard residues the values are three-letter uppercase codes (e.g. "ALA"); for modified
                  residues the values are the literal contents found inside the parentheses.
        :rtype: dict[int, str]
        Notes
        - The function logs an error and returns an empty dict if a parenthesized residue string grows
          larger than 5 characters, which usually means the sequence is wrong
        - Parentheses must be well-formed for modified residues to be captured correctly; unmatched or
          malformed parentheses may produce incomplete or unexpected results.
        Example of return
        :example:
            Input:  "ACD(MSE)F"
            Output: {1: "ALA", 2: "CYS", 3: "ASP", 4: "MSE", 5: "PHE"}
        """
        # Validate sequence input
        if not sequence.strip():
            raise InvalidSequenceError("getResidueByOrdinal failed, sequence is empty")
        # Set up convertion table from 1-char to 3-char, 3-char will be used throughout the data process
        d_aa_convert = {
            "A": "ALA",
            "R": "ARG",
            "N": "ASN",
            "D": "ASP",
            "C": "CYS",
            "Q": "GLN",
            "E": "GLU",
            "G": "GLY",
            "H": "HIS",
            "I": "ILE",
            "L": "LEU",
            "K": "LYS",
            "M": "MET",
            "F": "PHE",
            "P": "PRO",
            "S": "SER",
            "T": "THR",
            "W": "TRP",
            "Y": "TYR",
            "V": "VAL",
            "U": "SEC",
            "O": "PYL",
            "B": "ASX",
            "Z": "GLX",
        }
        sequence = "".join(sequence.strip().split())  # remove extra white spaces if any
        d_residue_ordinal = {}
        i = 0
        b_standard = True
        for char in sequence:
            if char.strip():
                if char == "(":
                    b_standard = False
                    mod_residue = ""
                elif char == ")":
                    b_standard = True
                    i += 1
                    if mod_residue:
                        d_residue_ordinal[i] = mod_residue
                    else:
                        raise InvalidSequenceError("getResidueByOrdinal failed, sequence misses a starting parenthesis")
                else:
                    if b_standard:
                        i += 1
                        try:
                            d_residue_ordinal[i] = d_aa_convert[char.upper()]
                        except KeyError:
                            d_residue_ordinal[i] = char.upper()  # no conversion for strange cases, does impact stats
                    else:
                        mod_residue += char
                        if len(mod_residue) > 5:
                            raise InvalidSequenceError("getResidueByOrdinal failed, sequence misses or wrongly places an ending parenthesis")
        return d_residue_ordinal

    def fetchInstance(self, db: Database):
        """
        Fetch instance documents from the MongoDB "instance" collection and store data in self.resolution_bin["instances"]
        Perform MongoDB aggregation by instance_id and filtered "rcsb_polymer_instance_feature"
        array whose "type" is one of: "RSCC", "NATOMS_EDS", or "AVERAGE_OCCUPANCY".
        :param db: Instance of pymongo.database.Database class for pdbx_core database.
        :postcondition: If True, self.resolution_bin["instances"] is a list of dicts with at least:
                        - "rcsb_id" (str)
                        - "rcsb_polymer_instance_feature" (list of filtered feature dicts)
        """
        # Validate data input
        if ("instance_ids" not in self.resolution_bin) or (not self.resolution_bin["instance_ids"]):
            raise InvalidParametersError("fetchInstance failed, must run processEntity first to retrive valid PDB instance IDs")
        # Construct MongoDB aggregation
        logger.info("to fetch instance data for %s instances", len(self.resolution_bin["instance_ids"]))
        collectionName = self.__collections["instance"]  # use core_polymer_entity_instance collection
        collection = db[collectionName]
        pipeline = [
            {
                "$match": {
                    "rcsb_id": {"$in": self.resolution_bin["instance_ids"]}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "rcsb_id": 1,
                    "rcsb_polymer_instance_feature": {
                        "$filter": {
                            "input": "$rcsb_polymer_instance_feature",
                            "as": "f",
                            "cond": {"$in": ["$$f.type", ["RSCC", "NATOMS_EDS", "AVERAGE_OCCUPANCY"]]}
                        }
                    }
                }
            }
        ]
        # Run aggregate
        try:
            self.resolution_bin["instances"] = list(collection.aggregate(pipeline))
            logger.info("%s instances retrieved", len(self.resolution_bin["instances"]))
        except Exception as e:
            raise DatabaseError(f"fetchInstance failed to fetch data from MongoDB for resolution bin {self.resolution_bin['resolution']}: {e}") from e

    def processInstance(self):
        """
        Process polymer instance feature data and populate aggregated metrics.
        This method validates that self.resolution_bin["instances"] and then iterates
        over each polymer instance to extract fragment-level feature values. For each
        instance it builds two mappings:
        - self.resolution_bin["metrics"]:
            A mapping keyed by polymer instance identifier (rcsb_id) to a dictionary with
            three feature-type keys: "RSCC", "NATOMS_EDS", and "AVERAGE_OCCUPANCY". Each
            feature key maps to a dict of sequence ordinal -> feature value. Feature lists
            provided for a fragment are converted to ordinal-keyed dicts beginning at the
            fragment's beg_seq_id.
            Example shape:
            {
                    "instance_id_1": {
                            "RSCC": {12: 0.92, 13: 0.88, ...},
                            "NATOMS_EDS": {12: 5, 13: 7, ...},
                            "AVERAGE_OCCUPANCY": {12: 1.0, 13: 0.5, ...}
                    },
                    ...
            }
        - self.resolution_bin["fragments_start"]:
            A mapping keyed by polymer instance identifier to a dict that records the
            starting component identifier for each fragment (beg_seq_id -> beg_comp_id).
            Example shape:
            {
                    "instance_id_1": {12: "ALA", 20: "GLY", ...},
                    ...
            }
        Expected input structure (self.resolution_bin["instances"]):
        A list of dicts where each dict represents an instance and contains at least:
        - "rcsb_id" (str): instance identifier.
        - "rcsb_polymer_instance_feature" (list): optional list of feature dicts.
            Each feature dict should contain:
            - "type" (str): feature type (e.g., "RSCC", "NATOMS_EDS", "AVERAGE_OCCUPANCY").
            - "feature_positions" (list): list of position dicts, each with:
                - "beg_seq_id" (int): starting sequence ordinal for the fragment.
                - "beg_comp_id" (str): starting component/residue identifier for the fragment.
                - "values" (list): list of feature values for consecutive sequence ordinals
                    starting at beg_seq_id.
        Behavior:
        - For each instance, absent or empty feature lists/positions are skipped.
        - For each feature position, the position's "values" list is enumerated starting
            at beg_seq_id; these ordinal-keyed entries are merged into the corresponding
            feature dict for that instance. If multiple fragments provide values for the
            same ordinal, later updates will overwrite earlier ones.
        """
        # Validate data input
        if ("instances" not in self.resolution_bin) or (not self.resolution_bin["instances"]):
            raise InvalidParametersError("processInstance failed, must run fetchInstance first to retrive valid PDB instance data")
        # Start process
        logger.info("to process feature data of RSCC, natom, occupancy on %s instances", len(self.resolution_bin["instances"]))
        d_feature_oridinal = {}  # dictionary by instance id, record rscc/natoms/occu value by sequence ordinal
        d_beg_comp = {}  # dictionary by instance id, record begining residue of all fragments, for mapping verification
        for d_instance in self.resolution_bin["instances"]:
            rcsb_id = d_instance["rcsb_id"]
            logger.debug("to process instance data of %s", rcsb_id)
            d_feature_oridinal[rcsb_id] = {"RSCC": {}, "NATOMS_EDS": {}, "AVERAGE_OCCUPANCY": {}}
            d_beg_comp[rcsb_id] = {}
            l_feature = d_instance.get("rcsb_polymer_instance_feature", [])
            if not l_feature:
                logger.info("no feature data of RSCC, natom, occupancy for %s", rcsb_id)
                continue
            for feature in l_feature:
                feature_type = feature.get("type")
                l_feature_position = feature.get("feature_positions", [])
                if not l_feature_position:
                    logger.debug("no feature data for %s of %s", feature_type, rcsb_id)
                    continue
                for d_position in l_feature_position:
                    beg_seq_id = d_position["beg_seq_id"]
                    beg_comp_id = d_position["beg_comp_id"]
                    d_beg_comp[rcsb_id][beg_seq_id] = beg_comp_id
                    l_value = d_position.get("values", [])
                    if not l_value:
                        logger.debug("no value found for %s of %s", feature_type, rcsb_id)
                        continue
                    d_fragment = dict(enumerate(l_value, start=beg_seq_id))  # convert list to dict with key of seq_id
                    d_feature_oridinal[rcsb_id][feature_type].update(d_fragment)
        self.resolution_bin["metrics"] = d_feature_oridinal
        self.resolution_bin["fragments_start"] = d_beg_comp

    def processResidue(self):
        """Process residue RSCC data for all entries
        Side effects
        ------------
        - Mutates self.resolution_bin by creating or overwriting the keys "residues" and
            "tracking".
        - Populates lists under self.resolution_bin["residues"] for each standard residue.
        - Calls self.processResidueOneEntry(entry_id) for each entry, which is
            expected to update per-entry tracking counters and append RSCC values.
        """
        # Validate input data
        if ("sequences" not in self.resolution_bin) or (not self.resolution_bin["sequences"]):
            raise InvalidParametersError("processResidue failed, must run fetchEntiy and processEntity first to retrive valid PDB sequence data")
        if ("metrics" not in self.resolution_bin) or (not self.resolution_bin["metrics"]):
            raise InvalidParametersError("processResidue failed, must run fetchInstance and processInstance first to retrive valid PDB instance data")
        # Process and filter RSCC data for every residue of each entry, and track residue counting
        self.resolution_bin["residues"] = {}  # record residue RSCC data
        self.resolution_bin["tracking"] = {}  # record residue counting per entry for problem finding and solving
        for standard_residue in self.l_standard_residue:
            self.resolution_bin["residues"][standard_residue] = []  # record RSCC in one single array for one residue type
        for entry_id in self.resolution_bin["sequences"]:
            logger.debug("to process residue data of %s", entry_id)
            self.resolution_bin["tracking"][entry_id] = {
                "residues_total": 0,
                "residues_with_rscc": 0,
                "residues_selected": 0
            }  # initialize counting
            self.processResidueOneEntry(entry_id)  # process a single entry and count all processed residues

    def processResidueOneEntry(self, entry_id, count_limit=1000, occupancy_limit=0.9):
        """
        Process residue-level RSCC and related metrics for a single entry and
        populate aggregated bins in ``self.resolution_bin``.

        The method iterates over all entities and instances for ``entry_id`` and:
        - validates that RSCC, NATOMS_EDS and AVERAGE_OCCUPANCY dictionaries have
          matching sequence-ordinal keys;
        - validates that RSCC ordinals are a subset of the entity residue ordinals;
        - filters residues to standard amino-acid types (``self.l_standard_residue``);
        - filters residues by minimum average occupancy (``occupancy_limit``);
        - filters residues with more than one missing heavy atom (based on expected
          non-hydrogen atom counts);
        - appends accepted RSCC values to ``self.resolution_bin["residues"][residue_type]``;
        - updates counters in ``self.resolution_bin["tracking"][entry_id]`` and stops when
          ``count_limit`` is reached.

        :param entry_id: Identifier of the entry to process.
        :type entry_id: str
        :param count_limit: Maximum number of residues to collect for the entry.
        :type count_limit: int
        :param occupancy_limit: Minimum average occupancy required for a residue.
        :type occupancy_limit: float

        Side effects
        ------------
        - Modifies ``self.resolution_bin["residues"]`` by appending RSCC values for accepted residues.
        - Increments counters in ``self.resolution_bin["tracking"][entry_id]``:
          ``residues_total``, ``residues_with_rscc``, ``residues_selected``.
        - Emits informational, debug and error logs for processing and skip reasons.

        Notes
        -----
        - A residue is skipped if observed atom count < (expected_count - 1).
        - Only residue types present in ``self.l_standard_residue`` are considered.
        - Instances whose RSCC, NATOMS_EDS and AVERAGE_OCCUPANCY do not share
          identical ordinal keys are skipped.
        - If RSCC ordinals are not a subset of the entity ordinals the instance
          is skipped (may indicate microheterogeneity).
        """
        # reference on number of non-hydrogen atoms of each standard residues
        d_num_atoms = {
            "ALA": 6,
            "ARG": 12,
            "ASN": 9,
            "ASP": 9,
            "CYS": 7,
            "GLN": 10,
            "GLU": 10,
            "GLY": 5,
            "HIS": 11,
            "ILE": 9,
            "LEU": 9,
            "LYS": 10,
            "MET": 9,
            "PHE": 12,
            "PRO": 8,
            "SER": 7,
            "THR": 8,
            "TRP": 15,
            "TYR": 13,
            "VAL": 8,
            "MSE": 9
        }  # number of non-hydrogen atoms for 20 standard aa and MSE
        # enumerate through each entity in the entry
        for entity_id in self.resolution_bin["sequences"][entry_id]:
            logger.debug("to process residue data of entity %s of entry %s", entity_id, entry_id)
            d_residue_ordinal = self.resolution_bin["sequences"][entry_id][entity_id]["residue_ordinal"]
            l_instance_id = self.resolution_bin["sequences"][entry_id][entity_id]["instance_ids"]
            # enumerate through each instance of the entity
            for instance_id in l_instance_id:
                logger.debug("to process residue data of instance %s of entity %s of entry %s", instance_id, entity_id, entry_id)
                self.resolution_bin["tracking"][entry_id]["residues_total"] += len(d_residue_ordinal)  # track all residues
                d_rscc_ordinal = self.resolution_bin["metrics"][instance_id]["RSCC"]
                self.resolution_bin["tracking"][entry_id]["residues_with_rscc"] += len(d_rscc_ordinal)  # track residues with RSCC
                d_natoms_ordinal = self.resolution_bin["metrics"][instance_id]["NATOMS_EDS"]
                d_occupancy_ordinal = self.resolution_bin["metrics"][instance_id]["AVERAGE_OCCUPANCY"]
                # validate the sequence ordinals match among RSCC, NATOMS_EDS, and AVERAGE_OCCUPANCY
                if not d_rscc_ordinal.keys() == d_natoms_ordinal.keys() == d_occupancy_ordinal.keys():
                    logger.error("%s has unmatching sequence ordinal for at least one residue on RSCC, natoms, and occupancy, skip", instance_id)
                    continue
                # validate the RSCC sequence ordinals are a subset of the overall sequence
                if not d_rscc_ordinal.keys() <= d_residue_ordinal.keys():
                    logger.error("%s has RSCC values with sequence ordinal unmatch the sequence, might be microheterogeneity, skip", instance_id)
                    continue
                # enumerate through each residue in RSCC dict, retrieve metrics and residue type by sequence ordinal
                for ordinal, rscc in d_rscc_ordinal.items():
                    # retrieve and filter by residue type
                    residue_type = d_residue_ordinal[ordinal]
                    if residue_type not in self.l_standard_residue:
                        logger.debug("%s skip ordinal %s, type %s, rscc %s",
                                     instance_id, ordinal, residue_type, rscc)
                        continue  # skip non-standard residues
                    # retrieve and filter by occupancy
                    occupancy = d_occupancy_ordinal[ordinal]
                    if occupancy < occupancy_limit:
                        logger.debug("%s skip ordinal %s, type %s, rscc %s, occupancy %s",
                                     instance_id, ordinal, residue_type, rscc, occupancy)
                        continue  # skip residues with inadequate occupancy
                    # retrieve and filter by number of observed atoms
                    num_observed_atoms = d_natoms_ordinal[ordinal]
                    if num_observed_atoms < d_num_atoms[residue_type] - 1:
                        logger.debug("%s skip ordinal %s, type %s, rscc %s, occupancy %s, observed atoms %s",
                                     instance_id, ordinal, residue_type, rscc, occupancy, num_observed_atoms)
                        continue  # skip residues with more than one missing heavy atoms
                    # retrieve RSCC and add it to the array by residue type
                    self.resolution_bin["residues"][residue_type].append(rscc)
                    self.resolution_bin["tracking"][entry_id]["residues_selected"] += 1
                    if self.resolution_bin["tracking"][entry_id]["residues_selected"] >= count_limit:  # set sampling limit to avoid over-representation of large structure
                        logger.debug("reached residue selection limit of %s for entry %s, stop collecting", count_limit, entry_id)
                        return  # stop the single-entry method when limit is reached

    def calculatePercentiles(self):
        """
        Calculate percentile statistics for RSCC values in the current resolution bin, and mutate self.data_rscc
        This method:
        - Reads the resolution range from self.resolution_bin["resolution"] (expected as [high, low])
            and formats it as the string "high-low" to use as a key in self.data_rscc.
        - Iterates over residue types in self.l_standard_residue.
        - For each residue type, obtains the list of RSCC values from self.resolution_bin["residues"][residue_type].
        - If values are present, computes the 1st, 5th, 25th and 50th (median) percentiles
            using numpy.percentile and records these along with the count under
            self.data_rscc[resolution][residue_type].
        - If no values are present for a residue type, logs a warning and skips it.
        :raises KeyError:
                If expected keys ("resolution" or "residues") are missing from self.resolution_bin.
        :raises ValueError:
                If numpy.percentile receives invalid input (e.g., non-numeric values).
        :raises Exception:
                Propagates other unexpected exceptions from called operations (logging, numpy).
        """
        # Validate input data
        if ("resolution" not in self.resolution_bin) or (not self.resolution_bin["resolution"]):
            raise InvalidParametersError("calculatePercentiles failed, must run fetchEntry and provide resolution bin")
        if ("residues" not in self.resolution_bin) or (not self.resolution_bin["residues"]):
            raise InvalidParametersError("calculatePercentiles failed, must run processResidue first to retrive valid PDB residue data")
        # start process
        [high, low] = self.resolution_bin["resolution"]
        resolution_str = f"[{high},{low})"  # convert resolution bin list to str output
        # convert resolution bin to index used by Mol*, i.e. 0.1-1 -> 9, 1-1.1 -> 10...
        if high < 1:
            resolution_index = str(9)
        else:
            resolution_index = str(int(high * 10))
        # calculate percentiles for each residue type
        for residue_type in self.l_standard_residue:
            logger.info("to find percentiles for %s", residue_type)
            l_value = self.resolution_bin["residues"][residue_type]
            if not l_value:
                logger.warning("no RSCC value found for %s", residue_type)
                continue
            count = len(l_value)
            p1, p5, p25, median = np.round(
                np.percentile(l_value, [1, 5, 25, 50]),
                3
            )
            self.data_rscc[residue_type][resolution_index] = [p25, p5, p1]
            self.data_ref[residue_type][resolution_str] = {
                "count": count,
                "median": median,
                "p25": p25,
                "p5": p5,
                "p1": p1
            }

    def writeTracking(self, output_file: str):
        """
        Write the tracking data for the current bin to a tsv file for review

        :param output_file: Path to the output tsv file.
        """
        if ("tracking" not in self.resolution_bin) or (not self.resolution_bin["tracking"]):
            raise OutputError("writeTracking failed, no tracking data to write, please run processResidue first to retrive valid PDB residue data")
        l_header = ["entry_id", "residues_total", "residues_with_rscc", "residues_selected"]
        try:
            with open(output_file, mode="w", newline="", encoding="utf-8") as file:
                file.write("\t".join(l_header))
                file.write("\n")
                for entry_id in self.resolution_bin["tracking"]:
                    l_line = [entry_id]
                    for key in ["residues_total", "residues_with_rscc", "residues_selected"]:
                        l_line.append(str(self.resolution_bin["tracking"][entry_id][key]))
                    file.write("\t".join(l_line))
                    file.write("\n")
        except Exception as e:
            raise OutputError(f"writeTracking failed to write tracking data to {output_file}: {e}") from e
        logger.info("Wrote tracking data to output files %s", output_file)

    def writeReference(self):
        """
        Write the generated self.data_rscc to a json file for Mol* to read
        """
        fU = FileUtil()
        fU.mkdir(self.__dirPath)
        #
        output_file = self.__getRcssRefDataPath()
        if not self.data_rscc:
            raise OutputError("No data to write. Please run generate() first.")
        if type(self.data_rscc) is not dict:
            raise OutputError("Data format incorrect. Expected a dictionary after generate()")
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(self.data_rscc, f, indent=2)
        except Exception as e:
            raise OutputError(f"writeReference failed to write data to {output_file}: {e}") from e
        logger.info("Finished generating RSCC reference data file at %s", output_file)

    def writeReviewReference(self, output_file: str):
        """
        Write the generated self.data_ref to a tsv file.

        :param output_file: Path to the output tsv file.
        :return: True upon successful write operation.
        """
        if not self.data_ref:
            raise OutputError("writeReviewReference failed, No data to write. Please run generate() first.")
        if type(self.data_ref) is not dict:
            raise OutputError("writeReviewReference failed, Data format incorrect. Expected a dictionary after generate()")
        l_header = ["resname", "res_cut", "count", "median", "p25", "p5", "p1"]
        try:
            with open(output_file, mode="w", newline="", encoding="utf-8") as file:
                file.write("\t".join(l_header))
                file.write("\n")
                for residue_type in self.data_ref:
                    for resolution in self.data_ref[residue_type]:
                        l_line = [residue_type, resolution]
                        for key in ["count", "median", "p25", "p5", "p1"]:
                            l_line.append(str(self.data_ref[residue_type][resolution][key]))
                        file.write("\t".join(l_line))
                        file.write("\n")
        except Exception as e:
            raise OutputError(f"writeReviewReference failed to write data to {output_file}: {e}") from e
        logger.info("Wrote review reference data to output files %s", output_file)
