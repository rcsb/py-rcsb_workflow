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
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.seqalign.MMseqsUtils import MMseqsUtils
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
        self.__seqDbCachePath = os.path.join(self.__cachePath, "seq-databases")
        self.__resultCachePath = os.path.join(self.__cachePath, "seq-search-results")
        self.__umP = None
        self.__defaultResourceNameList = ["sabdab", "card", "drugbank", "chembl", "pharos"]

    def testCache(self):
        return True

    def exportProteinEntityFasta(self, resourceName="pdbprent"):
        """Export protein entity sequence data (fasta, taxon mapping, and essential details)"""
        ok = False
        try:
            #
            pEx = PolymerEntityExtractor(self.__cfgOb)
            fastaPath = self.__getFastaPath(resourceName)
            taxonPath = self.__getTaxonPath(resourceName)
            detailsPath = self.__getDetailsPath(resourceName)
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

    def exportTargets(self, useCache=True, addTaxonomy=False, reloadPharos=False, fromDbPharos=False, resourceNameList=None):
        resourceNameList = resourceNameList if resourceNameList else self.__defaultResourceNameList
        retOk = True
        for resourceName in resourceNameList:
            startTime = time.time()
            ok = self.__exportTargetFasta(resourceName, useCache=useCache, addTaxonomy=addTaxonomy, reloadPharos=reloadPharos, fromDbPharos=fromDbPharos)
            logger.info("Completed loading %s targets (status %r)at %s (%.4f seconds)", resourceName, ok, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), time.time() - startTime)
            retOk = retOk and ok
        return retOk

    def __exportTargetFasta(self, resourceName, useCache=True, addTaxonomy=False, reloadPharos=False, fromDbPharos=False):
        ok = False
        try:
            configName = self.__cfgOb.getDefaultSectionName()
            fastaPath = self.__getFastaPath(resourceName)
            taxonPath = self.__getTaxonPath(resourceName)
            if resourceName == "card":
                cardtP = CARDTargetProvider(cachePath=self.__cachePath, useCache=useCache)
                if cardtP.testCache():
                    ok = cardtP.exportCardFasta(fastaPath, taxonPath)
            elif resourceName == "drugbank":
                user = self.__cfgOb.get("_DRUGBANK_AUTH_USERNAME", sectionName=configName)
                pw = self.__cfgOb.get("_DRUGBANK_AUTH_PASSWORD", sectionName=configName)
                dbtP = DrugBankTargetProvider(cachePath=self.__cachePath, useCache=useCache, username=user, password=pw)
                if dbtP.testCache():
                    ok = dbtP.exportFasta(fastaPath, taxonPath, addTaxonomy=addTaxonomy)
            elif resourceName == "chembl":
                chtP = ChEMBLTargetProvider(cachePath=self.__cachePath, useCache=useCache)
                if chtP.testCache():
                    ok = chtP.exportFasta(fastaPath, taxonPath, addTaxonomy=addTaxonomy)
            elif resourceName == "pharos":
                user = self.__cfgOb.get("_MYSQL_DB_USER_NAME", sectionName=configName)
                pw = self.__cfgOb.get("_MYSQL_DB_PASSWORD_ALT", sectionName=configName)
                ptP = PharosTargetProvider(cachePath=self.__cachePath, useCache=useCache, reloadDb=reloadPharos, fromDb=fromDbPharos, mysqlUser=user, mysqlPassword=pw)
                if not (reloadPharos or fromDbPharos):
                    ok = ptP.restore(self.__cfgOb, configName)
                if ptP.testCache():
                    ok = ptP.exportProteinFasta(fastaPath, taxonPath, addTaxonomy=addTaxonomy)
            elif resourceName == "sabdab":
                stP = SAbDabTargetProvider(cachePath=self.__cachePath, useCache=False)
                if stP.testCache():
                    ok = stP.exportFasta(fastaPath)
            elif resourceName == "pdbprent":
                pEx = PolymerEntityExtractor(self.__cfgOb)
                detailsPath = self.__getDetailsPath(resourceName)
                ok = pEx.exportProteinEntityFasta(fastaPath, taxonPath, detailsPath)
        except Exception as e:
            logger.exception("Failing for %r with %s", resourceName, str(e))
        #
        return ok

    def createSearchDatabases(self, resourceNameList=None, timeOutSeconds=3600, verbose=False):
        """Create sequence search databases for the input resources and optionally include taxonomy details"""
        try:
            resourceNameList = resourceNameList if resourceNameList else self.__defaultResourceNameList
            retOk = True
            for resourceName in resourceNameList:
                startTime = time.time()
                fastaPath = self.__getFastaPath(resourceName)
                taxonPath = self.__getTaxonPath(resourceName)
                mmS = MMseqsUtils(cachePath=self.__cachePath)
                ok = mmS.createSearchDatabase(fastaPath, self.__seqDbCachePath, resourceName, timeOut=timeOutSeconds, verbose=verbose)
                if ok and taxonPath:
                    ok = mmS.createTaxonomySearchDatabase(taxonPath, self.__seqDbCachePath, resourceName, timeOut=timeOutSeconds)
                logger.info(
                    "Completed creating sequence databases for %s targets (status %r) at %s (%.4f seconds)",
                    resourceName,
                    ok,
                    time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                    time.time() - startTime,
                )
                retOk = retOk and ok
            return retOk
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            retOk = False
        return retOk

    def searchDatabases(self, queryResourceNameList, referenceResourceName, identityCutoff=0.90, timeOutSeconds=3600, sensitivity=4.5):
        """Search for similar sequences for each query resource in the reference resource"""
        ok = False
        try:
            mU = MarshalUtil()
            mU.mkdir(self.__resultCachePath)
            mmS = MMseqsUtils(cachePath=self.__cachePath)
            retOk = True
            for queryResourceName in queryResourceNameList:
                resultPath = self.__getSearchResultPath(queryResourceName, referenceResourceName)
                mmS = MMseqsUtils(cachePath=self.__cachePath)
                ok = mmS.searchDatabase(
                    queryResourceName,
                    self.__seqDbCachePath,
                    referenceResourceName,
                    resultPath,
                    minSeqId=identityCutoff,
                    timeOut=timeOutSeconds,
                    sensitivity=sensitivity,
                )
                taxonPath = self.__getTaxonPath(queryResourceName)
                if taxonPath and mU.exists(taxonPath):
                    mD = mmS.getMatchResults(resultPath, taxonPath, useTaxonomy=True, misMatchCutoff=-1, sequenceIdentityCutoff=identityCutoff)
                else:
                    mD = mmS.getMatchResults(resultPath, None, useTaxonomy=False, misMatchCutoff=-1, sequenceIdentityCutoff=identityCutoff)
                logger.info("Search result %r (status %r) (queries with matches %d)", queryResourceName, ok, len(mD))
                filteredPath = self.__getFilteredSearchResultPath(queryResourceName, referenceResourceName)
                mU.doExport(filteredPath, mD, fmt="json", indent=3)
                retOk = retOk and ok
            return retOk
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            retOk = False
        return retOk

    def __getFilteredSearchResultPath(self, queryResourceName, referenceResourceName):
        return os.path.join(self.__resultCachePath, queryResourceName + "-vs-" + referenceResourceName + "-filtered-results.json")

    def __getSearchResultPath(self, queryResourceName, referenceResourceName):
        return os.path.join(self.__resultCachePath, queryResourceName + "-vs-" + referenceResourceName + "-raw-results.json")

    def __getFastaPath(self, resourceName):
        return os.path.join(self.__fastaCachePath, resourceName + "-targets.fa")

    def __getTaxonPath(self, resourceName):
        if resourceName == "sabdab":
            return None
        return os.path.join(self.__fastaCachePath, resourceName + "-targets-taxon.tdd")

    def __getDetailsPath(self, resourceName):
        return os.path.join(self.__cachePath, resourceName, resourceName + "-details.json")
