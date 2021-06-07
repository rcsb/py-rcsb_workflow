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
        self.__configName = cfgOb.getDefaultSectionName()
        self.__cachePath = os.path.abspath(cachePath)
        self.__fastaCachePath = os.path.join(self.__cachePath, "FASTA")
        self.__umP = None
        self.__nameMap = {
            "pdb": "pdb-protein",
            "sabdab": "sabdab-protein",
            "card": "card-protein",
            "drugbank": "drugbank-protein",
            "chembl": "chembl-protein",
            "pharos": "pharos-protein",
        }

    def testCache(self):
        return True

    def exportPDBProteinEntityFasta(self, minSeqLen=20):
        """Export protein entity sequence data (fasta, taxon mapping, and essential details)"""
        ok = False
        try:
            #
            pEx = PolymerEntityExtractor(self.__cfgOb)
            fastaPath = self.getFastaPath("pdb")
            taxonPath = self.getTaxonPath("pdb")
            detailsPath = os.path.join(self.__cachePath, "pdb-targets", "pdb-protein-entity-details.json")
            ok = pEx.exportProteinEntityFasta(fastaPath, taxonPath, detailsPath, minSeqLen=minSeqLen)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def initUniProtTaxonomy(self):
        if not self.__umP:
            startTime = time.time()
            umP = UniProtIdMappingProvider(cachePath=self.__cachePath)
            umP.restore(self.__cfgOb, self.__configName)
            umP.reload(useCache=True, useLegacy=False, fmt="tdd", mapNames=["NCBI-taxon"])
            logger.info("Initialized UniProt Id mapping at %s (%.4f seconds)", time.strftime("%Y %m %d %H:%M:%S", time.localtime()), time.time() - startTime)
            ok = umP.testCache()
            if ok:
                self.__umP = umP
        return ok

    def updateUniProtTaxonomy(self):
        startTime = time.time()
        umP = UniProtIdMappingProvider(cachePath=self.__cachePath)
        umP.clearCache()
        ok1 = umP.reload(useCache=True, useLegacy=False, fmt="tdd", mapNames=["NCBI-taxon"])
        logger.info("Completed building UniProt Id mapping (%r) at %s (%.4f seconds)", ok1, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), time.time() - startTime)
        if ok1:
            ok2 = umP.backup(self.__cfgOb, self.__configName)
        return ok1 & ok2

    def exportTargets(self, useCache=True, addTaxonomy=False, reloadPharos=False, resourceNameList=None):
        resourceNameList = resourceNameList if resourceNameList else ["sabdab", "card", "drugbank", "chembl", "pharos"]
        retOk = True
        for resourceName in resourceNameList:
            startTime = time.time()
            ok = self.__exportTargetFasta(resourceName, useCache=useCache, addTaxonomy=addTaxonomy, reloadPharos=reloadPharos)
            logger.info("Completed loading %s targets (status %r)at %s (%.4f seconds)", resourceName, ok, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), time.time() - startTime)
            retOk = retOk and ok
        return retOk

    def getFastaPath(self, resourceName):
        fp = None
        if resourceName in self.__nameMap:
            fp = os.path.join(self.__fastaCachePath, self.__nameMap[resourceName] + "-targets.fa")
        return fp

    def getTaxonPath(self, resourceName):
        fp = None
        if resourceName in self.__nameMap:
            fp = os.path.join(self.__fastaCachePath, self.__nameMap[resourceName] + "-targets-taxon.tdd")
        return fp

    def getDatabasePath(self):
        return os.path.join(self.__cachePath, "db")

    def getResultDirPath(self):
        return os.path.join(self.__cachePath, "sequence-comparison-results")

    def __exportTargetFasta(self, resourceName, useCache=True, addTaxonomy=False, reloadPharos=False):
        ok = False
        try:
            configName = self.__cfgOb.getDefaultSectionName()
            fastaPath = self.getFastaPath(resourceName)
            taxonPath = self.getTaxonPath(resourceName)
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
                ptP = PharosTargetProvider(cachePath=self.__cachePath, useCache=useCache, reloadDb=reloadPharos, fromDb=reloadPharos, mysqlUser=user, mysqlPassword=pw)
                if reloadPharos:
                    ok = ptP.testCache()
                else:
                    ok = ptP.restore(self.__cfgOb, self.__configName)
                if ok:
                    ok = ptP.exportProteinFasta(fastaPath, taxonPath, addTaxonomy=addTaxonomy)
            elif resourceName == "sabdab":
                stP = SAbDabTargetProvider(cachePath=self.__cachePath, useCache=False)
                if stP.testCache():
                    ok = stP.exportFasta(fastaPath)
        except Exception as e:
            logger.exception("Failing for %r with %s", resourceName, str(e))
        #
        return ok

    def makeSearchDatabase(self, resourceNameList=None, timeOut=60):
        resourceNameList = resourceNameList if resourceNameList else ["sabdab", "card", "drugbank", "chembl", "pharos"]
        retOk = True
        for resourceName in resourceNameList:
            startTime = time.time()
            ok = self.__makeSearchDatabase(resourceName, timeOut=timeOut)
            logger.info("Completed loading %s targets (status %r) at %s (%.4f seconds)", resourceName, ok, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), time.time() - startTime)
            retOk = retOk and ok
        #
        return retOk

    def __makeSearchDatabase(self, resourceName, timeOut=None):
        """Create search database for the input resource.

        Args:
            resourceName (str): resource name (e.g., drugbank)

        Returns:
            (bool): True for success or false otherwise
        """
        try:

            mU = MarshalUtil(workPath=self.__cachePath)
            fastaPath = self.getFastaPath(resourceName)
            taxonPath = self.getTaxonPath(resourceName)
            seqDbTopPath = self.getDatabasePath()
            mU.mkdir(seqDbTopPath)
            #

            mmS = MMseqsUtils(cachePath=self.__cachePath)
            ok = mmS.createSearchDatabase(fastaPath, seqDbTopPath, resourceName, timeOut=timeOut, verbose=True)
            if ok and mU.exists(taxonPath):
                ok = mmS.createTaxonomySearchDatabase(taxonPath, seqDbTopPath, resourceName, timeOut=timeOut)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def search(self, referenceName, resourceNameList=None, identityCutoff=0.90, timeOut=10, sensitivity=4.5):
        resourceNameList = resourceNameList if resourceNameList else ["sabdab", "card", "drugbank", "chembl", "pharos"]
        retOk = True
        for resourceName in resourceNameList:
            startTime = time.time()
            ok = self.__searchSimilar(referenceName, resourceName, identityCutoff=identityCutoff, timeOut=timeOut, sensitivity=sensitivity)
            logger.info(
                "Completed searching %s targets (status %r) (cutoff=%r) at %s (%.4f seconds)",
                resourceName,
                ok,
                identityCutoff,
                time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                time.time() - startTime,
            )
            retOk = retOk and ok
        #
        return retOk

    def __searchSimilar(self, referenceName, resourceName, identityCutoff=0.90, timeOut=10, sensitivity=4.5):
        """Map similar sequences between reference resource and input resource -"""
        try:
            resultDirPath = self.getResultDirPath()
            taxonPath = self.getTaxonPath(resourceName)
            seqDbTopPath = self.getDatabasePath()
            mU = MarshalUtil(workPath=self.__cachePath)
            mU.mkdir(resultDirPath)
            #
            mmS = MMseqsUtils(cachePath=self.__cachePath)
            ky = referenceName + "-" + resourceName
            rawPath = os.path.join(resultDirPath, ky + "-raw.json")
            resultPath = os.path.join(resultDirPath, ky + "-results.json")

            mmS = MMseqsUtils(cachePath=self.__cachePath)

            ok = mmS.searchDatabase(resourceName, seqDbTopPath, referenceName, rawPath, minSeqId=identityCutoff, timeOut=timeOut)
            #
            if mU.exists(taxonPath):
                mL = mmS.getMatchResults(rawPath, taxonPath, useTaxonomy=True, misMatchCutoff=-1, sequenceIdentityCutoff=identityCutoff)
            else:
                mL = mmS.getMatchResults(rawPath, None, useTaxonomy=False, misMatchCutoff=-1, sequenceIdentityCutoff=identityCutoff)

            logger.info("Search result %r (%d)", resourceName, len(mL))
            mU.doExport(resultPath, mL, fmt="json")
            return ok and mL is not None
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False
