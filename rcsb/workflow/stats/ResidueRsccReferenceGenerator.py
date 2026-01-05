##
#  File:           ResidueRsccReferenceGenerator.py
#  Date:           2025-12-25 Chenghua Shao
#
#  Update:
##
"""
Generate RSCC reference for standard polymer residues for Mol* RSCC-based colored display.
Utilities to query RCSB MongoDB polymer-instance RSCC and related metrics,
aggregate them by residue type for X‑ray entries in resolution bins, and
produce RSCC reference statistics (percentiles) for standard polymer residues.

"""
import csv
import logging
import json
import time
import numbers
import numpy as np

from rcsb.db.mongo.Connection import Connection
from rcsb.utils.config.ConfigUtil import ConfigUtil

logger = logging.getLogger(__name__)

class InvalidParametersError(ValueError):
    pass


class InvalidSequenceError(Exception):
    pass


class ResidueRsccReferenceGenerator:
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
            data: final output of RSCC percential by residue type and resolution bin
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
        Methods: the following methods works in tandem, except for the last two that consolidate all
            fetchEntry: query MongoDB to add self.bin["entry_id"] by resolution bin;
            fetchEntity: query MongoDB to add self.bin["entities"] by entry_id;
            processEntity: process self.bin["entities"] to add self.bin["sequences"] and self.bin["instance_ids"];
            fetchInstance: query MongoDB to add self.bin["instances"] by instance_id;
            processInstance: process self.bin["instances"] to add self.bin["metrics"] and self.bin["fragments_start];
            processResidue: map residue type to RSCC by sequence ordinal, filter by natoms and occupancy, to add self.bin["residues"] and self.bin["tracking"]
            calculatePercentile: calculate percentile for RSCC array;
            generateBin: consolidate all processes above to generate data for one resolution bin;
            generate: generate final data for all resolution bins. 
    """
    def __init__(self, cfgOb, **kwargs):
        """Initiate the class variables and the MongoDB connection"""
        #
        self.l_standard_residue = ["ALA", "ARG", "ASN", "ASP", "CYS",
                                   "GLN", "GLU", "GLY", "HIS", "ILE",
                                   "LEU", "LYS", "MET", "PHE", "PRO",
                                   "SER", "THR", "TRP", "TYR", "VAL",
                                   "MSE"] # 20 standard aa + MSE
        self.data = {}  # RSCC percentials by residue type and resolution bin, formated for Mol*    
        self.data_ref = {}  # optional reference, superset of self.data, for review purpose only
        for residue in self.l_standard_residue:
            self.data[residue] = {}
            self.data_ref[residue] = {}
        self.bin = {}  # store all data for the current resolution bin being worked on
        # self.bin is updated through each step of MongoDB data fetch and process to add values for
        # (1) MongoDB query results by keys of: "entry_ids", "entities",  "instances"; 
        # (2) Processed results by keys of: "instance_ids", "sequences", "residues", "metrics"; 
        # (3) Metadata by keys of: "resolution", "tracking".
        #
        _ = kwargs
        self.__cfgOb = cfgOb
        # self.__configName = cfgOb.getDefaultSectionName()
        self.__resourceName = "MONGO_DB"
        #  
        self.__databaseName = kwargs.get("databaseName", "pdbx_core")
        self.__collectionNames = kwargs.get("collectionNames", 
                                           ["pdbx_core_entry", 
                                            "pdbx_core_polymer_entity", 
                                            "pdbx_core_polymer_entity_instance"])
        #
        conn = Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName)
        conn.openConnection()
        self.__client = conn.getClientConnection()
        self.__db = self.__client[self.__databaseName]
        self.__collections = {}
        for collectionName in self.__collectionNames:
            collectionLevel = collectionName.split("_")[-1]  # level of entry, entity, instance
            self.__collections[collectionLevel] = self.__db[collectionName]

    def generate(self) -> bool:
        """
        Generate residue RSCC references for the entire PDB archive, throughtout bins of
        [0.1, 1.0], [1.0, 1.1], ..., [3.4,3.5], [3.5, 50].
        :returns: True if all bins were processed successfully.
        :rtype: bool
        """
        # Construct all resolution bins
        l_range = [i / 10 for i in range(10, 36)]
        l_range.insert(0, 0.1)
        l_range.append(50)
        l_bin = []
        for i in range(len(l_range)-1):
            bin = [l_range[i], l_range[i+1]]
            l_bin.append(bin)
        # Enumerate through each bin
        for bin in l_bin:
            if not self.generateBin(bin):
                logger.error("Stop process due to failure in the bin %s", bin)
                return False
            self.bin = {}
        return True

    def generateBin(self, resolution_bin: list[int]):
        """
        Generate the RSCC percentile reference of each standard residue for a given resolution bin.

        :param resolution_bin: Two‑element sequence ``[high_resolution, low_resolution]``
            specifying the half‑open interval (``high_resolution <= resolution < low_resolution``).
        :type resolution_bin: list[float] or list[int]
        :returns: True if all processing steps complete successfully; False if any step fails.
        :rtype: bool
        """
        if not self.fetchEntry(resolution_bin):
            logger.error("failed fetechEntry step for the resolution bin %s", resolution_bin)
            return False
        if not self.fetchEntity():
            logger.error("failed fetchEntity step for the resolution bin %s", resolution_bin)
            return False
        if not self.processEntity():
            logger.error("failed processEntity step for the resolution bin %s", resolution_bin)
            return False
        if not self.fetchInstance():
            logger.error("failed fetchInstance step for the resolution bin %s", resolution_bin)
            return False
        if not self.processInstance():
            logger.error("failed processInstance step for the resolution bin %s", resolution_bin)
            return False
        if not self.processResidue():
            logger.error("failed processResidue step for the resolution bin %s", resolution_bin)
            return False
        if not self.calculatePercentiles():
            logger.error("failed calculate Percentiles step for the resolution bin %s", resolution_bin)
            return False
        logger.info("finished all RSCC data process for the resolution bin %s", resolution_bin)
        return True

    def fetchEntry(self, resolution_bin: list[int]) -> bool:
        """
        Fetch PDB entry IDs within a given resolution bin and store them in self.bin["entry_ids"].
        Perform MongoDB search by rcsb_entry_info.resolution_combined within the resolution bin
        :param resolution_bin: Two-element sequence specifying the resolution bin as
            [high_resolution, low_resolution].  - high_resolution must be a number >= 0.
        :type resolution_bin: list[int] or list[float]
        :returns: True if the MongoDB query succeeded
        :rtype: bool
        Notes
        -----
        - The resolution interval is half-open: high_resolution <= resolution < low_resolution.
        """
        # Validate input data
        try:
            self.verifyResolution(resolution_bin)
        except InvalidParametersError as e:
            logger.error("invalid resolution bin of %s: %s", resolution_bin, e)
            return False
        # Construct MongDB query
        logger.info("to fetch entry ID for resolution bin %s", resolution_bin)
        self.bin["resolution"] = resolution_bin
        [high_resolution, low_resolution] = resolution_bin
        collection = self.__collections["entry"]  # use core_entry collection
        d_condition = {"rcsb_entry_info.experimental_method": "X-ray",
                        "rcsb_entry_info.resolution_combined": {
                            "$gte": high_resolution, "$lt": low_resolution
                        }
        }  # high_resolution <= bin < low_resolution
        # Run find
        try:
            cursor = collection.find(d_condition, {"_id": 0, "rcsb_id": 1})
            self.bin["entry_ids"] = [doc["rcsb_id"] for doc in cursor]  # collect IDs in a list only
            logger.info("%s PDB X-ray entries found within the resolution bin %s", len(self.bin["entry_ids"]), resolution_bin)
            return True
        except Exception as e:
            logger.error("failed to fetch entry data from MongoDB for resolution bin %s, %s", resolution_bin, e)
            return False
    
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
        [high_resolution, low_resolution] = resolution_bin
        if high_resolution <0:
            raise InvalidParametersError("high resolution, as the 1st value of the resolution bin, must be greater than zero")
        if low_resolution <= high_resolution:
            raise InvalidParametersError("high resolution, as the 1st value of the resolution bin, must be smaller than the 2nd")

    def fetchEntity(self) -> bool:
        """
        Fetch protein entities for entries in the current resolution bin and store them in self.bin["entities"].
        Perform MongoDB aggregation by entry_id, and returns the following fields for each matching entity document:
        - rcsb_id, e.g. '2OR2_1'
        - entry_id, e.g. '2OR2'
        - entity_id, e.g. '1'
        - asym_ids, e.g. ['A', 'B']
        - pdbx_seq_one_letter_code, e.g. 'ASSVNELENWSKWMQPIPDNIPLARISIPGTHDSGT(MSE)A...'
        :returns: True if the MongoDB aggregation succeeded
        :rtype: bool
        """
        # Validate input data
        if ("entry_ids" not in self.bin) or (not self.bin["entry_ids"]):
            logger.error("must run fetchEntry first to retrive valid PDB entry IDs")
            return False
        # Construct MongoBD aggregation
        logger.info("to fetch entity data for %s entries", len(self.bin["entry_ids"]))
        collection = self.__collections["entity"]  # use core_polymer_entity collection
        pipeline = [
            {
                "$match": {
                    "rcsb_polymer_entity_container_identifiers.entry_id": {"$in": self.bin["entry_ids"]},
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
            self.bin["entities"] = list(collection.aggregate(pipeline))
            logger.info("%s entities retrived", len(self.bin["entities"]))
            return True
        except Exception as e:
            logger.error("failed to fetch entitiy data from MongoDB with %s", e)
            return False

    def processEntity(self) -> bool:
        """
        Process entities stored in self.bin["entities"] and populate instance and sequence mappings.
        constructing:
        - self.bin["instance_ids"]: a flat list of instance identifiers in the form "entry_id.asym_id"
            for all asym IDs of X-ray protein entities in the bin, used for next MongoDB query.
        - self.bin["sequences"]: a nested mapping keyed first by entry_id then by entity_id, where
            each entity mapping contains:
                - "residue_ordinal": the output of residue identity for each ordinal index
                - "instance_ids": the list of instance identifiers (entry_id.asym_id) for the entity
        Returns
        :returns: True if all entities were validated and processed successfully.
        :rtype: bool
        Side effects
        - Mutates self.bin by setting/updating:
            - self.bin["instance_ids"] (list of strings)
            - self.bin["sequences"] (dict: entry_id -> entity_id -> {"residue_ordinal", "instance_ids"})
        """
        # Validate input data
        if ("entities" not in self.bin) or (not self.bin["entities"]):
            logger.error("must run fetchEntity first to retrive valid PDB entity data")
            return False
        n_all_entities = len(self.bin["entities"])
        logger.info("to process %s entities", n_all_entities)
        self.bin["instance_ids"] = []  # to record all instance ids for X-ray proteins of the resolution bin
        self.bin["sequences"] = {}  # key by entry id -> entity id, record each residue's identity
        n_success_entities = 0
        for d_entity in self.bin["entities"]:
            for key in ["entry_id", "entity_id", "asym_ids", "pdbx_seq_one_letter_code"]:
                if key not in d_entity:
                    logger.error("key %s not found in entities object", key)
                    return False
                if not d_entity[key]:
                    logger.error("empty value for %s in entities object", key)
                    return False
            entry_id = d_entity["entry_id"]
            entity_id = d_entity["entity_id"]
            logger.info("to process entity data of entity %s of entry %s", entity_id, entry_id)
            asym_ids = d_entity["asym_ids"]  # asym IDs as list
            sequence = d_entity["pdbx_seq_one_letter_code"]  # one-letter code sequence
            if entry_id not in self.bin["sequences"]:
                self.bin["sequences"][entry_id] = {}
            self.bin["sequences"][entry_id][entity_id] = {}
            self.bin["sequences"][entry_id][entity_id]["instance_ids"] = []
            for asym_id in asym_ids:
                instance_id = ".".join([entry_id, asym_id])
                self.bin["sequences"][entry_id][entity_id]["instance_ids"].append(instance_id)
                self.bin["instance_ids"].append(instance_id)
            try:
                d_residue_ordinal = self.getResidueByOrdinal(sequence)
                self.bin["sequences"][entry_id][entity_id]["residue_ordinal"] = d_residue_ordinal
                n_success_entities += 1
            except InvalidSequenceError as e:
                logger.error("invalid sequence: %s; %s", sequence, e)
                self.bin["sequences"][entry_id][entity_id]["residue_ordinal"] = {}
        logger.info("finished entity process success/total entities: %s/%s", n_success_entities, n_all_entities)
        return True

    def getResidueByOrdinal(self, sequence: str) -> dict:
        """
        Get a mapping from 1-based residue ordinals to residue identifiers parsed from a sequence string.
        This function interprets an input sequence string containing standard one-letter amino-acid codes
        and optional modified-residue annotations enclosed in parentheses. Standard one-letter codes are converted to their three-letter uppercase codes using an
        internal conversion table. Parenthesized residue annotations are captured verbatim (preserving
        case) and assigned as the residue identifier for a single ordinal.
        Supported behavior summary
        - Standard single-letter residues (case-insensitive) are mapped to three-letter codes (e.g. 'A' -> 'ALA').
        - Text between '(' and ')' is treated as a single modified residue and assigned to the next ordinal.
        - If a parenthesized modified residue exceeds 5 characters, an error is logged and an empty dict is returned.
        - Unknown single-letter codes (not present in the internal mapping) will raise a KeyError.
        Parameters
        :param sequence: The residue sequence to parse. May include whitespace and parenthesized modifications.
        :type sequence: str
        Returns
        :returns: A dictionary mapping 1-based residue ordinals (int) to residue identifiers (str). For
                  standard residues the values are three-letter uppercase codes (e.g. 'ALA'); for modified
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
            Output: {1: 'ALA', 2: 'CYS', 3: 'ASP', 4: 'MSE', 5: 'PHE'}
        """
        # Validate sequence input
        if not sequence.strip():
            raise InvalidSequenceError("sequence is empty")
        # Set up convertion table from 1-char to 3-char, 3-char will be used throughout the data process
        d_aa_convert = {'A': 'ALA',
                        'R': 'ARG',
                        'N': 'ASN',
                        'D': 'ASP',
                        'C': 'CYS',
                        'Q': 'GLN',
                        'E': 'GLU',
                        'G': 'GLY',
                        'H': 'HIS',
                        'I': 'ILE',
                        'L': 'LEU',
                        'K': 'LYS',
                        'M': 'MET',
                        'F': 'PHE',
                        'P': 'PRO',
                        'S': 'SER',
                        'T': 'THR',
                        'W': 'TRP',
                        'Y': 'TYR',
                        'V': 'VAL',
                        'U': 'SEC',
                        'O': 'PYL',
                        'B': 'ASX',
                        'Z': 'GLX',
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
                        raise InvalidSequenceError("sequence misses a starting parenthesis")
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
                            raise InvalidSequenceError("sequence misses or wrongly places an ending parenthesis")
        return d_residue_ordinal

    def fetchInstance(self) -> bool:
        """
        Fetch instance documents from the MongoDB "instance" collection and store data in self.bin["instances"]
        Perform MongoDB aggregation by instance_id and filtered "rcsb_polymer_instance_feature"
        array whose "type" is one of: "RSCC", "NATOMS_EDS", or "AVERAGE_OCCUPANCY".
        :returns: True if the aggregation succeeded and self.bin["instances"] was populated.
        :rtype: bool
        :postcondition: If True, self.bin["instances"] is a list of dicts with at least:
                        - "rcsb_id" (str)
                        - "rcsb_polymer_instance_feature" (list of filtered feature dicts)
        """
        # Validate data input
        if ("instance_ids" not in self.bin) or (not self.bin["instance_ids"]):
            logger.error("must run fetchEntity and processEntity first to retrive valid PDB instance IDs")
            return False
        # Construct MongoDB aggregation
        logger.info("to fetch instance data for %s instances", len(self.bin["instance_ids"]))
        collection = self.__collections["instance"]  # use core_polymer_entity_instance collection 
        pipeline = [
            {
                "$match": {
                    "rcsb_id": { "$in": self.bin["instance_ids"] }
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
            self.bin["instances"] = list(collection.aggregate(pipeline))
            logger.info("%s instances retrived", len(self.bin["instances"]))
            return True
        except Exception as e:
            logger.error("failed to fetch instance data from MongoDB with %s", e)
            return False

    def processInstance(self) -> bool:
        """
        Process polymer instance feature data and populate aggregated metrics.
        This method validates that self.bin["instances"] and then iterates
        over each polymer instance to extract fragment-level feature values. For each
        instance it builds two mappings:
        - self.bin["metrics"]:
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
        - self.bin["fragments_start"]:
            A mapping keyed by polymer instance identifier to a dict that records the
            starting component identifier for each fragment (beg_seq_id -> beg_comp_id).
            Example shape:
            {
                    "instance_id_1": {12: "ALA", 20: "GLY", ...},
                    ...
            }
        Expected input structure (self.bin["instances"]):
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
        :return: True if processing succeeded and self.bin was updated; False if required
                         instance data was not present.
        :rtype: bool
        """
        # Validate data input
        if ("instances" not in self.bin) or (not self.bin["instances"]):
            logger.error("must run fetchInstance first to retrive valid PDB instance data")
            return False
        # Start process
        logger.info("to process feature data of RSCC, natom, occupancy on %s instances", len(self.bin["instances"]))
        d_feature_oridinal = {}  # dictionary by instance id, record rscc/natoms/occu value by sequence ordinal
        d_beg_comp = {}  # dictionary by instance id, record begining residue of all fragments, for mapping verification
        for d_instance in self.bin["instances"]:
            id = d_instance["rcsb_id"]
            logger.info("to process instance data of %s", id)
            d_feature_oridinal[id] = {"RSCC": {}, "NATOMS_EDS": {}, "AVERAGE_OCCUPANCY": {}}
            d_beg_comp[id] = {}
            l_feature = d_instance.get("rcsb_polymer_instance_feature", [])
            if not l_feature:
                logger.info("no feature data of RSCC, natom, occupancy for %s", id)
                continue
            for feature in l_feature:
                feature_type = feature.get("type")
                l_feature_position = feature.get("feature_positions", [])
                if not l_feature_position:
                    logger.info("no feature data for %s of %s", feature_type, id)
                    continue
                for d_position in l_feature_position:
                    beg_seq_id = d_position["beg_seq_id"]
                    beg_comp_id = d_position["beg_comp_id"]
                    d_beg_comp[id][beg_seq_id] = beg_comp_id
                    l_value = d_position.get("values", [])
                    if not l_value:
                        logger.info("no value found for %s of %s", feature_type, id)
                        continue
                    d_fragment = dict(enumerate(l_value, start=beg_seq_id))  # convert list to dict with key of seq_id
                    d_feature_oridinal[id][feature_type].update(d_fragment)
        self.bin["metrics"] = d_feature_oridinal
        self.bin["fragments_start"] = d_beg_comp
        return True

    def processResidue(self) -> bool:
        """Process residue RSCC data for all entries
        Returns
        -------
        :returns: True if processing was initialized and delegated successfully;
        :rtype: bool
        Side effects
        ------------
        - Mutates self.bin by creating or overwriting the keys 'residues' and
            'tracking'.
        - Populates lists under self.bin['residues'] for each standard residue.
        - Calls self.processResidueOneEntry(entry_id) for each entry, which is
            expected to update per-entry tracking counters and append RSCC values.
        """
        # Validate input data
        if ("sequences" not in self.bin) or (not self.bin["sequences"]):
            logger.error("must run fetchEntiy and processEntity first to retrive valid PDB sequence data")
            return False
        if ("metrics" not in self.bin) or (not self.bin["metrics"]):
            logger.error("must run fetchInstance and processInstance first to retrive valid PDB instance data")
            return False
        # Process and filter RSCC data for every residue of each entry, and track residue counting
        self.bin["residues"] = {}  # record residue RSCC data
        self.bin["tracking"] = {}  # record residue counting per entry for problem finding and solving
        for standard_residue in self.l_standard_residue:
            self.bin["residues"][standard_residue] = []  # record RSCC in one single array for one residue type
        for entry_id in self.bin["sequences"]:
            logger.info("to process residue data of %s", entry_id)
            self.bin["tracking"][entry_id] = {"residues_total": 0,
                                              "residues_with_rscc": 0,
                                              "residues_selected": 0}  # initialize counting
            self.processResidueOneEntry(entry_id)  # process a single entry and count all processed residues
        return True

    def processResidueOneEntry(self, entry_id, count_limit=1000, occupancy_limit=0.9):
        """
        Process residue-level RSCC and related metrics for a single entry and
        populate aggregated bins in ``self.bin``.

        The method iterates over all entities and instances for ``entry_id`` and:
        - validates that RSCC, NATOMS_EDS and AVERAGE_OCCUPANCY dictionaries have
          matching sequence-ordinal keys;
        - validates that RSCC ordinals are a subset of the entity residue ordinals;
        - filters residues to standard amino-acid types (``self.l_standard_residue``);
        - filters residues by minimum average occupancy (``occupancy_limit``);
        - filters residues with more than one missing heavy atom (based on expected
          non-hydrogen atom counts);
        - appends accepted RSCC values to ``self.bin['residues'][residue_type]``;
        - updates counters in ``self.bin['tracking'][entry_id]`` and stops when
          ``count_limit`` is reached.

        :param entry_id: Identifier of the entry to process.
        :type entry_id: str
        :param count_limit: Maximum number of residues to collect for the entry.
        :type count_limit: int
        :param occupancy_limit: Minimum average occupancy required for a residue.
        :type occupancy_limit: float
        :returns: None (updates are performed in-place on ``self.bin``).
        :rtype: None

        Side effects
        ------------
        - Modifies ``self.bin['residues']`` by appending RSCC values for accepted residues.
        - Increments counters in ``self.bin['tracking'][entry_id]``:
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
        d_num_atoms = { 'ALA': 6, 
                        'ARG': 12,
                        'ASN': 9,
                        'ASP': 9,
                        'CYS': 7,
                        'GLN': 10,
                        'GLU': 10,
                        'GLY': 5,
                        'HIS': 11,
                        'ILE': 9,
                        'LEU': 9,
                        'LYS': 10,
                        'MET': 9,
                        'PHE': 12,
                        'PRO': 8,
                        'SER': 7,
                        'THR': 8,
                        'TRP': 15,
                        'TYR': 13,
                        'VAL': 8,
                        'MSE': 9 }  # number of non-hydrogen atoms for 20 standard aa and MSE
        # enumerate through each entity in the entry
        for entity_id in self.bin["sequences"][entry_id]:
            logger.info("to process residue data of entity %s of entry %s", entity_id, entry_id)
            d_residue_ordinal = self.bin["sequences"][entry_id][entity_id]["residue_ordinal"]
            l_instance_id = self.bin["sequences"][entry_id][entity_id]["instance_ids"]
            # enumerate through each instance of the entity
            for instance_id in l_instance_id:
                logger.info("to process residue data of instance %s of entity %s of entry %s", instance_id, entity_id, entry_id)
                self.bin["tracking"][entry_id]["residues_total"] += len(d_residue_ordinal)  # track all residues
                d_rscc_ordinal = self.bin["metrics"][instance_id]["RSCC"]
                self.bin["tracking"][entry_id]["residues_with_rscc"] += len(d_rscc_ordinal)  # track residues with RSCC
                d_natoms_ordinal = self.bin["metrics"][instance_id]["NATOMS_EDS"]
                d_occupancy_ordinal = self.bin["metrics"][instance_id]["AVERAGE_OCCUPANCY"]
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
                    self.bin["residues"][residue_type].append(rscc)
                    self.bin["tracking"][entry_id]["residues_selected"] += 1
                    if self.bin["tracking"][entry_id]["residues_selected"] >= count_limit:  # set sampling limit to avoid over-representation of large structure
                        logger.info("reached residue selection limit of %s for entry %s, stop collecting", count_limit, entry_id)
                        return  # stop the single-entry method when limit is reached

    def calculatePercentiles(self):
        """
        Calculate percentile statistics for RSCC values in the current resolution bin, and mutate self.data
        This method:
        - Reads the resolution range from self.bin["resolution"] (expected as [high, low])
            and formats it as the string "high-low" to use as a key in self.data.
        - Iterates over residue types in self.l_standard_residue.
        - For each residue type, obtains the list of RSCC values from self.bin["residues"][residue_type].
        - If values are present, computes the 1st, 5th, 25th and 50th (median) percentiles
            using numpy.percentile and records these along with the count under
            self.data[resolution][residue_type].
        - If no values are present for a residue type, logs a warning and skips it.
        :returns: True if processing percentiles successfully;
        :rtype: bool
        :raises KeyError:
                If expected keys ("resolution" or "residues") are missing from self.bin.
        :raises ValueError:
                If numpy.percentile receives invalid input (e.g., non-numeric values).
        :raises Exception:
                Propagates other unexpected exceptions from called operations (logging, numpy).
        """
        # Validate input data
        if ("resolution" not in self.bin) or (not self.bin["resolution"]):
            logger.error("must run fetchEntry and provide resolution bin")
            return False
        if ("residues" not in self.bin) or (not self.bin["residues"]):
            logger.error("must run processResidue first to retrive valid PDB residue data")
            return False
        # start process
        [high, low] = self.bin["resolution"]
        resolution_str = f"[{high},{low})" # convert resolution bin list to str output
        # convert resolution bin to index used by Mol*, i.e. 0.1-1 -> 9, 1-1.1 -> 10...
        if high < 1:
            resolution_index = str(9)
        else:
            resolution_index = str(high * 10)
        # calculate percentiles for each residue type
        for residue_type in self.l_standard_residue:
            logger.info("to find percentiles for %s", residue_type)
            l_value = self.bin["residues"][residue_type]
            if not l_value:
                logger.warning("no RSCC value found for %s", residue_type)
                continue
            count = len(l_value)
            p1, p5, p25, median = np.round(
                np.percentile(l_value, [1, 5, 25, 50]),
                3
            )
            self.data[residue_type][resolution_index] = [p25, p5, p1]
            self.data_ref[residue_type][resolution_str] = {"count": count,
                                                           "median": median,
                                                           "p25": p25,
                                                           "p5": p5,
                                                           "p1": p1}
        return True

    def writeTracking(self, output_file: str) -> bool:
        """
        Write the tracking data for the current bin to a tsv file for review

        :param output_file: Path to the output tsv file.
        :return: True upon successful write operation.
        """
        if ("tracking" not in self.bin) or (not self.bin["tracking"]):
            logger.error("must run processResidue first")
            return False
        l_header = ["entry_id", "residues_total", "residues_with_rscc", "residues_selected"]
        with open(output_file, mode="w", newline="", encoding="utf-8") as file:
            file.write('\t'.join(l_header))
            file.write('\n')
            for entry_id in self.bin["tracking"]:
                l_line = [entry_id]
                for key in ["residues_total", "residues_with_rscc", "residues_selected"]:
                    l_line.append(str(self.bin["tracking"][entry_id][key]))
                file.write('\t'.join(l_line))
                file.write('\n')
        return True

    def writeReference(self, output_file: str) -> bool: 
        """
        Write the generated self.data to a json file for Mol* to read

        :param output_file: Path to the output json file.
        :return: True upon successful write operation.
        """
        if not self.data:
            logger.error("No data to write. Please run generate() first.")
            return False
        if type(self.data) is not dict:
            logger.error("Data format incorrect. Expected a dictionary after generate()")
            return False
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2)
        logger.info("Wrote data to output files %s", output_file)
        return True      

    def writeReviewReference(self, output_file: str) -> bool:
        """
        Write the generated self.data_ref to a tsv file.

        :param output_file: Path to the output tsv file.
        :return: True upon successful write operation.
        """
        if not self.data_ref:
            logger.error("No data to write. Please run generate() first.")
            return False
        if type(self.data_ref) is not dict:
            logger.error("Data format incorrect. Expected a dictionary after generate()")
            return False
        l_header = ["resname", "res_cut", "count", "median", "p25", "p5", "p1"]
        with open(output_file, mode="w", newline="", encoding="utf-8") as file:
            file.write('\t'.join(l_header))
            file.write('\n')
            for residue_type in self.data_ref:
                for resolution in self.data_ref[residue_type]:
                    l_line = [residue_type, resolution]
                    for key in ["count", "median", "p25", "p5", "p1"]:
                        l_line.append(str(self.data_ref[residue_type][resolution][key]))
                    file.write('\t'.join(l_line))
                    file.write('\n')
        return True
