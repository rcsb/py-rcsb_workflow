##
#  File:           LigandQualityReferenceGenerator.py
#  Date:           2025-12-25 Chenghua Shao
#
#  Update:
##
"""
Generate ligand quality reference data that is used for ligand quality score computation.

"""
import csv
import logging
import time
import numpy as np
import os
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

from rcsb.db.mongo.Connection import Connection

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Custom exception class for errors in MongoDB connection and query execution."""
    pass


class AnalysisError(Exception):
    """Custom exception class for errors during data analysis."""
    pass


class OutputError(Exception):
    """Custom exception class for errors during output file writing."""
    pass


class LigandQualityReferenceGenerator:
    """ This class generates ligand quality reference data by performing the following steps:
    Query ligand quality metrics from RCSB MongoDB resource;
    Process the ligand quality data by filtering, aggregating, and formatting;
    Generate ligand reference data through PCA;
        Attributes:
            data: final output of each pdb_ligand combination and their quality scores
            qdata: direct data from MongoDB query
        Methods:
            fetchLigand(pdb_id): Fetch, filter, reduce, and reformat ligand quality metrics
            analyze(): Run PCA on the filtered and reduced ligand quality scores to generate reference data.
            generate(pdb_ids): Full pipeline to generate ligand quality reference data by running the steps of
                fetch -> analyze.
            writeReference(output_file): Write the generated ligand quality reference data to a csv file.
    """
    def __init__(self, cfgOb, cachePath, **kwargs):
        """Initiate the class and the MongoDB connection"""
        #
        self.data = []  # final output of each pdb_ligand combination and their quality scores, which is a list of dictionaries with keys of pdb_ligand, mogul_bonds_RMSZ, mogul_angles_RMSZ, RSR, RSCC, fit_pc1, geo_pc1
        self.qdata = None  # direct query data from MongoDB, which is a list of dictionaries with keys of pdb_ligand, mogul_bonds_RMSZ, mogul_angles_RMSZ, RSR, RSCC, and count (number of instances for the same ligand in the same PDB entry)
        #
        _ = kwargs
        self.__cfgOb = cfgOb
        self.__cachePath = cachePath
        # self.__configName = cfgOb.getDefaultSectionName()
        self.__resourceName = "MONGO_DB"
        #
        self.__databaseName = kwargs.get("databaseName", "pdbx_core")
        self.__collectionName = kwargs.get("collectionName", "pdbx_core_nonpolymer_entity_instance")
        # self.__databaseName = kwargs.get("databaseName", "dw")
        # self.__collectionName = kwargs.get("collectionName", "core_nonpolymer_entity_instance")
        #

    def generate(self, pdb_ids: list[str] = None) -> bool:
        """
        Full pipeline to generate ligand quality reference data by running the steps of
        query -> filter -> reduce -> analyze.

        :param pdb_ids: List of specified PDB IDs, default to [] which leads to all PDB structures being queried.
        :return: The same object with updated self.data of ligand quality reference data.
        """
        # fetch ligand quality data from MongoDB, and process the data
        try:
            self.fetchLigand(pdb_ids)
        except DatabaseError as e:
            logger.error("Failed to fetch ligand quality data, STOP. ERROR: %s", e)
            return False
        # verify data is not empty
        if not self.qdata:
            logger.error("no data fetched from MongoDB, cannot analyze, STOP")
            return False
        # analyze the data to generate reference scores
        try:
            self.analyzeLigand()
        except AnalysisError as e:
            logger.error("Failed to analyze ligand quality data, STOP. ERROR: %s", e)
            return False
        # output the reference data to file
        output_file = os.path.join(self.__cachePath, "ligand_score_reference.csv")  # final output file
        try:
            self.writeReference(output_file)
        except OutputError as e:
            logger.error("Failed to write ligand quality reference data to file, STOP. ERROR: %s", e)
            return False
        # proper finishing log
        logger.info("Finished generating ligand quality reference data file at %s", output_file)
        return True

    def fetchLigand(self, pdb_ids: list[str] = None):
        """
        Fetch, filter, reduce, and reformat ligand quality metrics for given PDB IDs, defaulting to all structures.
        Fetch ligand quality metrics from MongoDB;
        Filter the queried ligand quality scores based on defined criteria;
        Reduce the data by combining multiple instances of the same ligand in the same PDB entry;
        Reformat through projection.

        :param pdb_ids: List of specified PDB IDs, default to [] which leads to all PDB structures being queried.
        """
        if pdb_ids:
            # Match the PDB entries by provided IDs
            pipeline = [{
                "$match": {
                    "rcsb_nonpolymer_entity_instance_container_identifiers.entry_id": {
                        "$in": pdb_ids
                    }
                }
            }]
        else:
            # With no matching PDB IDs provided, All PDB entries are chosen
            pipeline = []

        pipeline.extend([
            # Project combined ID and filter scores
            {
                "$project": {
                    "_id": 0,
                    "id": {
                        "$concat": [
                            "$rcsb_nonpolymer_entity_instance_container_identifiers.entry_id",
                            "-",
                            "$rcsb_nonpolymer_entity_instance_container_identifiers.comp_id"
                        ]
                    },
                    "scores": {
                        "$filter": {
                            "input": "$rcsb_nonpolymer_instance_validation_score",
                            "as": "s",
                            "cond": {
                                "$and": [
                                    {"$gte": ["$$s.average_occupancy", 0.9]},
                                    {"$lte": ["$$s.average_occupancy", 1]},
                                    {"$gte": ["$$s.completeness", 0.9]},
                                    {"$lte": ["$$s.completeness", 1]},
                                    {"$gt": ["$$s.mogul_bonds_RMSZ", 0]},
                                    {"$lte": ["$$s.mogul_bonds_RMSZ", 10]},
                                    {"$gt": ["$$s.mogul_angles_RMSZ", 0]},
                                    {"$lte": ["$$s.mogul_angles_RMSZ", 10]},
                                    {"$gt": ["$$s.RSR", 0]},
                                    {"$lte": ["$$s.RSR", 1]},
                                    {"$gt": ["$$s.RSCC", 0]},
                                    {"$lte": ["$$s.RSCC", 1]}
                                ]
                            }
                        }
                    }
                }
            },

            # Remove documents with no valid scores
            {"$match": {"scores.0": {"$exists": True}}},

            # Unwind and group by ID to calculate averages
            {"$unwind": "$scores"},
            {
                "$group": {
                    "_id": "$id",
                    "mogul_bonds_RMSZ": {"$avg": "$scores.mogul_bonds_RMSZ"},
                    "mogul_angles_RMSZ": {"$avg": "$scores.mogul_angles_RMSZ"},
                    "RSR": {"$avg": "$scores.RSR"},
                    "RSCC": {"$avg": "$scores.RSCC"},
                    "count": {"$sum": 1}
                }
            },

            # Final projection
            {
                "$project": {
                    "_id": 0,
                    "pdb_ligand": "$_id",
                    "mogul_bonds_RMSZ": 1,
                    "mogul_angles_RMSZ": 1,
                    "RSR": 1,
                    "RSCC": 1,
                    "count": 1
                }
            },

            # Optional limit
            # {"$limit": 10}
        ])

        # Run aggregation
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            try:
                db = client[self.__databaseName]
                collection = db[self.__collectionName]
                startTime = time.time()
                self.qdata = list(collection.aggregate(pipeline))
                logger.info(
                    "Fetched %s unique pdb_ligand combination records from MongoDB: %s, collectionName: %s at %s (%.4f seconds)",
                    len(self.qdata),
                    self.__databaseName,
                    self.__collectionName,
                    time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                    time.time() - startTime,
                )
            except Exception as e:
                self.qdata = None
                raise DatabaseError(f"Failed to run aggregation to fetch MongoDB ligand quality data. ERROR {e}") from e

    def analyzeLigand(self):
        """
        Run PCA on the filtered and reduced ligand quality scores to generate reference data.
        The first principal component (PC1) is used as the ligand quality reference score.

        """

        # Prepare data for PCA, convert each score type into a separate list for matrix construction
        l_pdb_ligand = []
        l_mogul_bonds_RMSZ = []
        l_mogul_angles_RMSZ = []
        l_RSR = []
        l_RSCC = []
        for d_lig in self.qdata:
            l_pdb_ligand.append(d_lig["pdb_ligand"])
            l_mogul_bonds_RMSZ.append(d_lig["mogul_bonds_RMSZ"])
            l_mogul_angles_RMSZ.append(d_lig["mogul_angles_RMSZ"])
            l_RSR.append(d_lig["RSR"])
            l_RSCC.append(d_lig["RSCC"])
        # Construct data matrices for geometry and fit scores
        matrix_geo = np.column_stack([l_mogul_bonds_RMSZ, l_mogul_angles_RMSZ])
        matrix_fit = np.column_stack([l_RSR, l_RSCC])
        # Run PCA separately on geometry and fit scores
        try:
            logging.info("To Run PCA on geometry scores")
            l_geo_pc1 = self.runPca(matrix_geo)
            logging.info("To Run PCA on fit scores")
            l_fit_pc1 = self.runPca(matrix_fit)
        except Exception as e:
            raise AnalysisError(f"Failed to run PCA on ligand quality metrics. ERROR {e}") from e
        # Combine the two PC1 scores into a final reference score
        for i, pdb_ligand in enumerate(l_pdb_ligand):
            ref_score = {
                "pdb_ligand": pdb_ligand,
                "rsr": round(l_RSR[i], 4),
                "rscc": round(l_RSCC[i], 4),
                "mogul_bonds_rmsz": round(l_mogul_bonds_RMSZ[i], 4),
                "mogul_angles_rmsz": round(l_mogul_angles_RMSZ[i], 4),
                "fit_pc1": round(l_fit_pc1[i], 5),
                "geo_pc1": round(l_geo_pc1[i], 5)
            }
            self.data.append(ref_score)
        logger.info("%s unique pdb-ligand pairs analzed by PCA", len(self.data))

    def runPca(self, X: np.ndarray) -> list:
        """
        Run PCA on numpy matrix and output the first principal components.
        Loadings and explained variance can be examined in log but not in outcome.
        Ensure lower scores correspond to better quality by adjusting the sign of PC1 if needed.

        :param X: numpy matrix of any dimension
        :return: first principal components as a list
        """
        # Scale the data
        scaler = StandardScaler()
        X_std = scaler.fit_transform(X)
        # Perform PCA
        pca = PCA(n_components=1)
        principal_components = pca.fit_transform(X_std)
        logger.info("Explained variance ratios: %s", pca.explained_variance_ratio_)  # should be > 0.7(70%) for ligand geo and fit PC1
        loadings = pca.components_[0]
        logger.info("Resulting PCA loadings: %s", loadings)  # should be [sqrt(0.5), sqrt(0.5)] for scaled PCA or with minus sign on one component
        # Code below ensures loading on the first variable (RSR for fitting, and mogul_bonds_RMSZ for geometry) to be positive,
        # This is because PCA direction is arbitrary, i.e. 0.707,-0.707 and -0.707,0.707 are equivalent, but we want to ensure
        # lower scores correspond to better quality, which gives a correct direction for subsequent pecentile calculation.
        if loadings[0] < 0:
            loadings = -loadings
            logger.info("Adjusted PCA loadings: %s", loadings)
            principal_components = -principal_components
            logger.info("Change sign of the first principal components to ensure correct directionality.")
        return principal_components[:, 0].tolist()

    def writeReference(self, output_file: str):
        """
        Write the generated ligand quality reference data to a csv file.

        :param output_file: Path to the output csv file.
        """
        if not self.data:
            raise OutputError("No data to write. Please run generate() first.")
        if type(self.data) is not list:
            raise OutputError("Data format incorrect. Expected a list of dictionaries after generate().")
        if type(self.data[0]) is not dict:
            raise OutputError("Data format incorrect. Expected a list of dictionaries after generate().")
        fieldnames = self.data[0].keys()
        try:
            with open(output_file, mode="w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()  # write column headers
                writer.writerows(self.data)  # write all rows
        except Exception as e:
            raise OutputError(f"Failed to write ligand quality reference data to file: {e}") from e
