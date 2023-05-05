##
# File:    ProteinTargetSequenceExecutionWorkflow.py
# Author:  J. Westbrook
# Date:    25-Jun-2021
#
# Updates:
#   3-Mar-2023 Standard args passed into workflow
#  21-Mar-2023 Allow backing up Pharos-targets to stash, more __init__ improvement
#   5-May-2023 Pass in fromDbPharos and reloadPharos parameters to exportFasta()
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
        """Export RCSB protein entity sequence FASTA, taxonomy, and sequence details"""
        logger.info("Running fetchProteinEntityData...")
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.exportRCSBProteinEntityFasta()
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def fetchChemicalReferenceMappingData(self):
        """Export RCSB chemical reference identifier mapping details"""
        logger.info("Running fetchChemicalReferenceMappingData...")
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.exportRCSBChemRefMapping()
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def fetchLigandNeighborMappingData(self):
        """Export RCSB ligand neighbor mapping details"""
        logger.info("Running fetchLigandNeighborMappingData...")
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.exportRCSBLigandNeighborMapping()
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def exportFasta(self, reloadPharos=True, fromDbPharos=True):
        """Export FASTA target files (and load Pharos from source)"""
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
        """Create search databases"""
        logger.info("Running createSearchDatabases...")
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.createSearchDatabases(resourceNameList=["sabdab", "card", "drugbank", "chembl", "pharos", "pdbprent"], addTaxonomy=True, timeOutSeconds=3600, verbose=False)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def searchDatabases(self):
        """Search sequence databases"""
        logger.info("Running searchDatabases...")
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok1 = ptsW.search(
                referenceResourceName="pdbprent", resourceNameList=["sabdab", "card", "drugbank", "chembl", "pharos"], identityCutoff=0.95, sensitivity=4.5, timeOutSeconds=1000
            )
            ok2 = ptsW.search(referenceResourceName="pdbprent", resourceNameList=["card"], identityCutoff=0.95, sensitivity=4.5, timeOutSeconds=1000, useBitScore=True)
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
                useTaxonomy=True,
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

    #
    # --- --- --- ---


def fullWorkflow():
    """Entry point for the full targets sequence and cofactor update workflow."""
    ptsWf = ProteinTargetSequenceExecutionWorkflow()
    ok = ptsWf.cacheTaxonomy()
    ok = ptsWf.updateUniProtTaxonomy() and ok
    ok = ptsWf.fetchProteinEntityData() and ok
    ok = ptsWf.fetchChemicalReferenceMappingData() and ok
    ok = ptsWf.fetchLigandNeighborMappingData() and ok
    ok = ptsWf.exportFasta() and ok
    ok = ptsWf.createSearchDatabases() and ok
    ok = ptsWf.searchDatabases() and ok
    ok = ptsWf.buildFeatures() and ok
    ok = ptsWf.buildActivityData() and ok
    ok = ptsWf.buildCofactorData() and ok
    ptsWf.resourceCheck()
    return ok


if __name__ == "__main__":
    status = fullWorkflow()
    print("Full workflow completion status (%r)", status)
