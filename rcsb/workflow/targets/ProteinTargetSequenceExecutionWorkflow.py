##
# File:    ProteinTargetSequenceExecutionWorkflow.py
# Author:  J. Westbrook
# Date:    25-Jun-2021
#
# Updates:
#   3-Mar-2023 Standard args passed into workflow
#  21-Mar-2023 Allow backing up Pharos-targets to stash, more __init__ improvement
#   5-May-2023 Pass in fromDbPharos and reloadPharos parameters to exportFasta()
#  12-Jun-2023 dwp Set useTaxonomy filter to False for CARD annotations
#  10-Dec-2024 dwp Specify 'max-seqs' for mmseqs search to override default value
#  13-Feb-2025 dwp Remove IMGT from feature building after service became unavailable February 2025
#  19-Feb-2025 dwp Bring back IMGT
##
"""
Execution workflow for protein target data ETL operations.
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

from rcsb.utils.taxonomy.TaxonomyProvider import TaxonomyProvider
from rcsb.workflow.targets.ProteinTargetSequenceWorkflow import ProteinTargetSequenceWorkflow
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))


class ProteinTargetSequenceExecutionWorkflow(object):
    def __init__(self, **kwargs):
        """Workflow wrapper  --  Workflow to rebuild and stash "buildable" cache resources.

        Args:
        configPath (str, optional): path to configuration file (default: exdb-config-example.yml)
        configName (str, optional): configuration section name (default: site_info_remote_configuration)
        mockTopPath (str, optional):  mockTopPath is prepended to path configuration options if it specified (default=None)
        workPath (str, optional):  path to working directory (default: HERE)
        cachePath (str, optional):  path to cache directory (default: HERE/CACHE)
        stashRemotePrefix (str, optional): file name prefix (channel) applied to remote stash file artifacts (default: None)
        debugFlag (bool, optional):  sets logger to debug mode (default: False)
        """
        configPath = kwargs.get("configPath", "exdb-config-example.yml")
        self.__configName = kwargs.get("configName", "site_info_remote_configuration")
        mockTopPath = kwargs.get("mockTopPath", None)
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=self.__configName, mockTopPath=mockTopPath)
        self.__workPath = kwargs.get("workPath", HERE)
        self.__cachePath = kwargs.get("cachePath", os.path.join(self.__workPath, "CACHE"))
        #
        self.__stashRemotePrefix = kwargs.get("stashRemotePrefix", None)
        #
        self.__debugFlag = kwargs.get("debugFlag", False)
        self.__startTime = time.time()
        if self.__debugFlag:
            logger.setLevel(logging.DEBUG)
            logger.debug("Starting at %s", time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        #

    def resourceCheck(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed at %s (%.4f seconds)\n", time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def cacheTaxonomy(self):
        """Cache NCBI taxonomy database files"""
        logger.info("Running cacheTaxonomy...")
        ok = False
        try:
            tU = TaxonomyProvider(cachePath=self.__cachePath, useCache=False, cleanup=False)
            ok = tU.testCache()
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def fetchUniProtTaxonomy(self):
        """Reload UniProt taxonomy mapping"""
        logger.info("Running fetchUniProtTaxonomy...")
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.reloadUniProtTaxonomy()
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def updateUniProtTaxonomy(self):
        """Test case - initialize the UniProt taxonomy provider (from scratch ~3482 secs)"""
        logger.info("Running updateUniProtTaxonomy...")
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.updateUniProtTaxonomy()
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def fetchProteinEntityData(self):
        """Export RCSB protein entity sequence FASTA, taxonomy, and sequence details
        by fetching from 'pdbx_core_polymer_entity'.
        """
        logger.info("Running fetchProteinEntityData...")
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.exportRCSBProteinEntityFasta()
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def fetchChemicalReferenceMappingData(self):
        """Export RCSB chemical reference identifier mapping details.

        Fetch/prepare mapping of all chemical references from 'bird_chem_comp_core' (for DrugBank and ChEMBL)
        (e.g., all CCs where {'rcsb_chem_comp_related.resource_name': 'DrugBank'}).

            --> Creates file: CACHE/chemref-mapping/chemref-mapping-data.json
        """
        logger.info("Running fetchChemicalReferenceMappingData...")
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.exportRCSBChemRefMapping()
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def fetchLigandNeighborMappingData(self):
        """Export RCSB ligand neighbor mapping details.

        Fetch/prepare mapping of all polymer_entities and their associated ligands (e.g., "2E1B_1": ["ZN"]).
        This is done by extracting out all "rcsb_ligand_neighbors" from all polymer_entity_instances, and then
        grouping them together on a per-entity basis.

            --> Creates file: CACHE/ligand-neighbor-mapping/ligand-neighbor-mapping-data.json
        """
        logger.info("Running fetchLigandNeighborMappingData...")
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.exportRCSBLigandNeighborMapping()
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def exportFasta(self, reloadPharos=True, fromDbPharos=True):
        """Export FASTA files for each target resource (and load Pharos from source).

            --> Creates files:
                - CACHE/SAbDab-features/sabdab-data.json  (e.g., (1074) Thera-SAbDab data records and (31152) SAbDab assignments)
                - CACHE/FASTA/sabdab-targets.fa  (e.g., SAbDab 2120 fasta sequences)
                - CACHE/FASTA/card-targets.fa
                - CACHE/FASTA/card-targets-taxon.tdd  (TDD is taxonomy database)
                - CACHE/FASTA/chembl-targets.fa
                - CACHE/FASTA/chembl-targets-taxon.tdd
                - CACHE/FASTA/pharos-targets.fa
                - CACHE/FASTA/pharos-targets-taxon.tdd
                - ...and more...
        """
        logger.info("Running exportFasta...")
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.exportTargetsFasta(
                useCache=True,
                addTaxonomy=True,
                reloadPharos=reloadPharos,
                fromDbPharos=fromDbPharos,
                resourceNameList=["sabdab", "card", "drugbank", "chembl", "pharos"],
                backupPharos=True,
                remotePrefix=self.__stashRemotePrefix
            )
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def createSearchDatabases(self):
        """Create search databases for each target resource.

            --> Creates directories/files:
                - CACHE/sequence-databases/sabdab/    (SABDAB is only non-taxonomy database; no TDD)
                - CACHE/sequence-databases/card/      (incl. taxonomy database, and all below)
                - CACHE/sequence-databases/chembl/    (file 'chembl' contains all sequences)
                - CACHE/sequence-databases/pharos/
                - CACHE/sequence-databases/pdbprent/
                - ...and more...
        """
        logger.info("Running createSearchDatabases...")
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.createSearchDatabases(resourceNameList=["sabdab", "card", "drugbank", "chembl", "pharos", "pdbprent"], addTaxonomy=True, timeOutSeconds=3600, verbose=False)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def searchDatabases(self):
        """Perform a search for each target resource DB against the 'pdbprent' database.

        Args (provided below):
            maxSeqs (int): Maximum results per query sequence allowed to pass the prefilter (affects sensitivity).
                           This corresponds to the '--max-seqs' flag in MMseqs2 (default: 300).
                           The higher the value, the fewer sequences will be filtered out (in cases of high sequence redundancy).
                           Note: Use with caution--this can dramatically increase disk usage (https://github.com/soedinglab/MMseqs2/wiki#disk-space).

            --> Creates files:
                - CACHE/sequence-search-results/card-vs-pdbprent-filtered-results.json  (number of keys represents number of query sequences with matches)
                - CACHE/sequence-search-results/card-vs-pdbprent-raw-results.json
                - CACHE/sequence-search-results/card-vs-pdbprent-raw-results.txt
                - CACHE/sequence-search-results/chembl-vs-pdbprent-filtered-results.json
                - CACHE/sequence-search-results/chembl-vs-pdbprent-raw-results.json
                - CACHE/sequence-search-results/chembl-vs-pdbprent-raw-results.txt
                - CACHE/sequence-search-results/pharos-vs-pdbprent-filtered-results.json
                - CACHE/sequence-search-results/pharos-vs-pdbprent-raw-results.json
                - CACHE/sequence-search-results/pharos-vs-pdbprent-raw-results.txt
                - CACHE/sequence-search-results/sabdab-vs-pdbprent-filtered-results.json
                - CACHE/sequence-search-results/sabdab-vs-pdbprent-raw-results.json
                - CACHE/sequence-search-results/sabdab-vs-pdbprent-raw-results.txt
                - ...and more...
        """
        logger.info("Running searchDatabases...")
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok1 = ptsW.search(
                referenceResourceName="pdbprent",
                resourceNameList=["sabdab", "drugbank", "chembl", "pharos"],
                identityCutoff=0.95,
                sensitivity=4.5,
                timeOutSeconds=1000,
                maxSeqs=750,  # number of seqs permitted past the prefilter (default 300; use caution when increasing w.r.t. disk usage)
            )
            ok2 = ptsW.search(
                referenceResourceName="pdbprent",
                resourceNameList=["card"],
                identityCutoff=0.95,
                sensitivity=4.5,
                timeOutSeconds=1000,
                useBitScore=True,
                maxSeqs=750,
            )
            ok = ok1 and ok2
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def buildFeatures(self):
        """Build features from search results"""
        logger.info("Running buildFeatures...")
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.buildFeatureData(
                referenceResourceName="pdbprent",
                resourceNameList=["sabdab", "card", "imgt"],
                useTaxonomy=False,
                backup=True,
                remotePrefix=self.__stashRemotePrefix
            )
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def buildActivityData(self):
        """Build features from search results"""
        logger.info("Running buildActivityData...")
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.buildActivityData(referenceResourceName="pdbprent", resourceNameList=["chembl", "pharos"], backup=True, remotePrefix=self.__stashRemotePrefix)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def buildCofactorData(self):
        """Build features from search results"""
        logger.info("Running buildCofactorData...")
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.buildCofactorData(referenceResourceName="pdbprent", resourceNameList=["chembl", "pharos", "drugbank"], backup=True, remotePrefix=self.__stashRemotePrefix)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def loadTargetCofactorData(self):
        """Load target cofactor data from search results"""
        logger.info("Running loadTargetCofactorData...")
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.loadTargetCofactorData(resourceNameList=["chembl", "pharos", "drugbank"])
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    #
    # --- --- --- ---


def fullWorkflow():
    """Entry point for the full targets sequence and cofactor update workflow."""
    ptsWf = ProteinTargetSequenceExecutionWorkflow()
    #
    # Fetch taxonomy data
    ok = ptsWf.cacheTaxonomy()
    logger.info("cacheTaxonomy status %r", ok)
    ok = ptsWf.updateUniProtTaxonomy() and ok
    logger.info("updateUniProtTaxonomy status %r", ok)
    #
    # Fetch all PDB entity sequences from 'pdbx_core_polymer_entity'
    ok = ptsWf.fetchProteinEntityData() and ok
    logger.info("fetchProteinEntityData status %r", ok)
    #
    # Fetch/prepare mapping of all chemical references from 'bird_chem_comp_core' (for DrugBank and ChEMBL)
    ok = ptsWf.fetchChemicalReferenceMappingData() and ok
    logger.info("fetchChemicalReferenceMappingData status %r", ok)
    #
    # Fetch/prepare mapping of all polymer_entities and their associated ligands (e.g., "2E1B_1": ["ZN"])
    ok = ptsWf.fetchLigandNeighborMappingData() and ok
    logger.info("fetchLigandNeighborMappingData status %r", ok)
    #
    # Export FASTA files for each target resource
    ok = ptsWf.exportFasta() and ok
    logger.info("exportFasta status %r", ok)
    #
    # Create search databases for each target resource
    ok = ptsWf.createSearchDatabases() and ok
    logger.info("createSearchDatabases status %r", ok)
    #
    # Search each target resource DB against the 'pdbprent' database...
    ok = ptsWf.searchDatabases() and ok
    logger.info("searchDatabases status %r", ok)
    #
    ok = ptsWf.buildFeatures() and ok
    logger.info("buildFeatures status %r", ok)
    #
    # To rebuild ChEMBL-target-activity data from scratch (non-incremental), set skip=None in fetchTargetActivityDataMulti()
    ok = ptsWf.buildActivityData() and ok
    logger.info("buildActivityData status %r", ok)
    #
    ok = ptsWf.buildCofactorData() and ok
    logger.info("buildCofactorData status %r", ok)
    #
    ptsWf.resourceCheck()
    return ok


if __name__ == "__main__":
    status = fullWorkflow()
    logger.info("Full workflow completion status (%r)", status)
