##
# File: ProteinTargetSequenceWorkflow.py
# Date: 8-Dec-2020  jdw
#
#  Workflow wrapper  --  protein target ETL operations
#    1. generate PDB protein sequence fasta file and related metadata --
#
#  Updates:
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import time


from rcsb.exdb.seq.PolymerEntityExtractor import PolymerEntityExtractor

from rcsb.utils.seq.UniProtIdMappingProvider import UniProtIdMappingProvider

from rcsb.utils.targets.CARDTargetProvider import CARDTargetProvider
from rcsb.utils.targets.ChEMBLTargetProvider import ChEMBLTargetProvider
from rcsb.utils.targets.DrugBankTargetProvider import DrugBankTargetProvider
from rcsb.utils.targets.PharosTargetProvider import PharosTargetProvider
from rcsb.utils.targets.SAbDabTargetProvider import SAbDabTargetProvider

logger = logging.getLogger(__name__)


class ProteinTargetSequenceWorkflow(object):
    def __init__(self, cfgOb, cachePath, **kwargs):
        """Workflow wrapper  --  protein target ETL operations."""
        #
        _ = kwargs
        self.__cfgOb = cfgOb
        self.__cachePath = os.path.abspath(cachePath)
        self.__fastaCachePath = os.path.join(self.__cachePath, "FASTA")
        self.__umP = None

    def testCache(self):
        return True

    def exportProteinEntityFasta(self):
        """Export protein entity sequence data (fasta, taxon mapping, and essential details)"""
        ok = False
        try:
            #
            pEx = PolymerEntityExtractor(self.__cfgOb)
            fastaPath = os.path.join(self.__fastaCachePath, "protein-entity.fa")
            taxonPath = os.path.join(self.__fastaCachePath, "protein-entity-taxon.tdd")
            detailsPath = os.path.join(self.__fastaCachePath, "protein-entity-details.json")
            ok = pEx.exportProteinEntityFasta(fastaPath, taxonPath, detailsPath)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def fetchUniProtTaxonomy(self):
        if not self.__umP:
            startTime = time.time()
            self.__umP = UniProtIdMappingProvider(cachePath=self.__cachePath, mapNames=["NCBI-taxon"], useCache=True, useLegacy=True, fmt="tdd")
            logger.info("Completed UniProt Id mapping at %s (%.4f seconds)", time.strftime("%Y %m %d %H:%M:%S", time.localtime()), time.time() - startTime)
            #
            # taxId = umP.getMappedId("Q6GZX0", mapName="NCBI-taxon")
            # logger.info("TaxId %r", taxId)
        return self.__umP is not None

    def exportTargets(self, useCache=True, addTaxonomy=False, reloadPharos=False, resourceNameList=None):
        resourceNameList = resourceNameList if resourceNameList else ["sabdab", "card", "drugbank", "chembl", "pharos"]
        retOk = True
        for resourceName in resourceNameList:
            startTime = time.time()
            ok = self.__exportTargetFasta(resourceName, useCache=useCache, addTaxonomy=addTaxonomy, reloadPharos=reloadPharos)
            logger.info(
                "Completed loading %s targets (status %r)at %s (%.4f seconds)", resourceName, ok, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), time.time() - startTime
            )
            retOk = retOk and ok
        return retOk

    def __exportTargetFasta(self, resourceName, useCache=True, addTaxonomy=False, reloadPharos=False):
        ok = False
        try:
            configName = self.__cfgOb.getDefaultSectionName()
            fastaPath = os.path.join(self.__fastaCachePath, resourceName + "-targets.fa")
            taxonPath = os.path.join(self.__fastaCachePath, resourceName + "-targets-taxon.tdd")
            if resourceName == "card":
                cardtP = CARDTargetProvider(cachePath=self.__cachePath, useCache=useCache)
                if cardtP.testCache():
                    ok = cardtP.exportCardFasta(fastaPath, taxonPath)
            elif resourceName == "drugbank":
                user = self.__cfgOb.get("_DRUGBANK_AUTH_USERNAME", sectionName=configName)
                pw = self.__cfgOb.get("_DRUGBANK_AUTH_PASSWORD", sectionName=configName)
                dbtP = DrugBankTargetProvider(cachePath=self.__cachePath, useCache=useCache, username=user, password=pw)
                if dbtP.testCache():
                    ok = dbtP.exportFasta(fastaPath, taxonPath, addTaxonomy=False)
            elif resourceName == "chembl":
                chtP = ChEMBLTargetProvider(cachePath=self.__cachePath, useCache=useCache)
                if chtP.testCache():
                    ok = chtP.exportFasta(fastaPath, taxonPath, addTaxonomy=addTaxonomy)
            elif resourceName == "pharos":
                user = self.__cfgOb.get("_MYSQL_DB_USER_NAME", sectionName=configName)
                pw = self.__cfgOb.get("_MYSQL_DB_PASSWORD_ALT", sectionName=configName)
                ptP = PharosTargetProvider(cachePath=self.__cachePath, useCache=useCache, reloadDb=reloadPharos, mysqlUser=user, mysqlPassword=pw)
                if ptP.testCache():
                    ok = ptP.exportProteinFasta(fastaPath, taxonPath, addTaxonomy=addTaxonomy)
            elif resourceName == "sabdab":
                stP = SAbDabTargetProvider(cachePath=self.__cachePath, useCache=False)
                if stP.testCache():
                    ok = stP.exportFasta(fastaPath)
        except Exception as e:
            logger.exception("Failing for %r with %s", resourceName, str(e))
        #
        return ok
