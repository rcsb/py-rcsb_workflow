##
# File:    ProteinTargetSequenceExecutionWorkflow.py
# Author:  J. Westbrook
# Date:    25-Jun-2021
#
# Updates:
#
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

from rcsb.workflow.targets.ProteinTargetSequenceWorkflow import ProteinTargetSequenceWorkflow
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))


class ProteinTargetSequenceExecutionWorkflow(object):
    def __init__(self):
        self.__mockTopPath = None
        configPath = os.path.join(HERE, "exdb-config-example.yml")
        configName = "site_info_remote_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        self.__cachePath = os.path.join(HERE, "CACHE")
        #
        self.__remotePrefix = None
        self.__startTime = time.time()
        logger.info("Starting at %s", time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def resourceCheck(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed at %s (%.4f seconds)\n", time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def fetchUniProtTaxonomy(self):
        """Reload UniProt taxonomy mapping"""
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.reloadUniProtTaxonomy()
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def updateUniProtTaxonomy(self):
        """Test case - initialize the UniProt taxonomy provider (from scratch ~3482 secs)"""
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.updateUniProtTaxonomy()
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def fetchProteinEntityData(self):
        """Export RCSB protein entity sequence FASTA, taxonomy, and sequence details"""
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.exportRCSBProteinEntityFasta()
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def fetchChemicalReferenceMappingData(self):
        """Export RCSB chemical reference identifier mapping details"""
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.exportRCSBChemRefMapping()
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def fetchLigandNeighborMappingData(self):
        """Export RCSB ligand neighbor mapping details"""
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.exportRCSBLigandNeighborMapping()
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def exportFasta(self):
        """Export FASTA target files (and load Pharos from source)"""
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.exportTargetsFasta(useCache=True, addTaxonomy=True, reloadPharos=True, fromDbPharos=True, resourceNameList=["sabdab", "card", "drugbank", "chembl", "pharos"])
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def createSearchDatabases(self):
        """Create search databases"""
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.createSearchDatabases(resourceNameList=["sabdab", "card", "drugbank", "chembl", "pharos", "pdbprent"], addTaxonomy=True, timeOutSeconds=3600, verbose=False)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def searchDatabases(self):
        """Search sequence databases"""
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
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.buildFeatureData(referenceResourceName="pdbprent", resourceNameList=["sabdab", "card"], useTaxonomy=True, backup=True, remotePrefix=self.__remotePrefix)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def buildActivityData(self):
        """Build features from search results"""
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.buildActivityData(referenceResourceName="pdbprent", resourceNameList=["chembl", "pharos"], backup=True, remotePrefix=self.__remotePrefix)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def buildCofactorData(self):
        """Build features from search results"""
        ok = False
        try:
            ptsW = ProteinTargetSequenceWorkflow(self.__cfgOb, self.__cachePath)
            ok = ptsW.buildCofactorData(referenceResourceName="pdbprent", resourceNameList=["chembl", "pharos", "drugbank"], backup=True, remotePrefix=self.__remotePrefix)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    #
    # --- --- --- ---


def fullWorkflow():
    """Entry point for the full targets sequence and cofactor update workflow."""
    ptsWf = ProteinTargetSequenceExecutionWorkflow()
    ok = True
    ok = ptsWf.fetchUniProtTaxonomy()
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
