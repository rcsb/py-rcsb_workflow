##
# File: ProteinTargetSequenceWorkflow.py
# Date: 8-Dec-2020  jdw
#
#  Workflow wrapper  --  protein target ETL operations
#    1. generate PDB protein sequence fasta file and related metadata --
#
#  Updates:
#  25-Jul-2021 add new StashableBase options for git backup
#  29-Jul-2021 DrugBankProvider needs useCache=False to always rebuild
#   3-Mar-2023 Fix error for missing taxonPath
#  14-Mar-2023 Generate CARD annotations instead of CARD features
#  21-Mar-2023 Allow backing up Pharos-targets to stash
#   5-May-2023 Restore from stash if fromDbPharos and reloadPharos parameters are False
#   1-Jun-2023 aae Don't back up resources to GitHub during cache update workflows
#  12-Jun-2023 dwp Set useTaxonomy filter to False for CARD annotations
#   6-Jul-2023 aae Don't overwrite Buildlocker files if there is no data
#  20-Aug-2024 dwp Add step for loading target cofactor data to MongoDB
#  27-Aug-2024 dwp Update usage of CARDTargetOntologyProvider
#  10-Dec-2024 dwp Add support for 'max-seqs' flag in mmseqs search
#  13-Feb-2025 dwp Remove IMGT from feature building after service became unavailable February 2025
#  19-Feb-2025 dwp Bring back IMGT
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import time

from rcsb.exdb.chemref.ChemRefMappingProvider import ChemRefMappingProvider
from rcsb.exdb.seq.LigandNeighborMappingProvider import LigandNeighborMappingProvider
from rcsb.exdb.seq.PolymerEntityExtractor import PolymerEntityExtractor
from rcsb.utils.chemref.PharosProvider import PharosProvider
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.seqalign.MMseqsUtils import MMseqsUtils
from rcsb.utils.seq.UniProtIdMappingProvider import UniProtIdMappingProvider
from rcsb.utils.targets.CARDTargetProvider import CARDTargetProvider
from rcsb.utils.targets.CARDTargetAnnotationProvider import CARDTargetAnnotationProvider
from rcsb.utils.targets.CARDTargetOntologyProvider import CARDTargetOntologyProvider
from rcsb.utils.targets.ChEMBLTargetProvider import ChEMBLTargetProvider
from rcsb.utils.targets.ChEMBLTargetActivityProvider import ChEMBLTargetActivityProvider
from rcsb.utils.targets.ChEMBLTargetCofactorProvider import ChEMBLTargetCofactorProvider
from rcsb.utils.targets.DrugBankTargetProvider import DrugBankTargetProvider
from rcsb.utils.targets.DrugBankTargetCofactorProvider import DrugBankTargetCofactorProvider
from rcsb.utils.targets.IMGTTargetFeatureProvider import IMGTTargetFeatureProvider
from rcsb.utils.targets.PharosTargetProvider import PharosTargetProvider
from rcsb.utils.targets.PharosTargetActivityProvider import PharosTargetActivityProvider
from rcsb.utils.targets.PharosTargetCofactorProvider import PharosTargetCofactorProvider
from rcsb.utils.targets.SAbDabTargetProvider import SAbDabTargetProvider
from rcsb.utils.targets.SAbDabTargetFeatureProvider import SAbDabTargetFeatureProvider

logger = logging.getLogger(__name__)


class ProteinTargetSequenceWorkflow(object):
    def __init__(self, cfgOb, cachePath, **kwargs):
        """Workflow wrapper  --  protein target ETL operations."""
        #
        _ = kwargs
        self.__cfgOb = cfgOb
        self.__configName = cfgOb.getDefaultSectionName()
        self.__cachePath = os.path.abspath(cachePath)
        self.__umP = None
        self.__defaultResourceNameList = ["sabdab", "card", "drugbank", "chembl", "pdbprent"]

    def testCache(self):
        return True

    def exportRCSBChemRefMapping(self):
        """Export RCSB chemical reference data identifier mapping data"""
        ok = False
        try:
            crmP = ChemRefMappingProvider(self.__cachePath, useCache=False)
            ok = crmP.fetchChemRefMapping(self.__cfgOb)
            logger.info("Completed fetch ChemRefMappingProvider (%r)", ok)
            crmP.reload()
            if ok and crmP.testCache(minCount=1):
                ok = crmP.backup(self.__cfgOb, self.__configName, useStash=True, useGit=False)
                logger.info("Completed backup ChemRefMappingProvider (%r)", ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def exportRCSBLigandNeighborMapping(self):
        """Export RCSB ligand neighbor mapping data"""
        ok = False
        try:
            crmP = LigandNeighborMappingProvider(self.__cachePath, useCache=False)
            ok = crmP.fetchLigandNeighborMapping(self.__cfgOb)
            logger.info("Completed fetch LigandNeighborMappingProvider (%r)", ok)
            crmP.reload()
            if ok and crmP.testCache(minCount=1):
                ok = crmP.backup(self.__cfgOb, self.__configName, useStash=True, useGit=False)
                logger.info("Completed backup LigandNeighborMappingProvider (%r)", ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def exportRCSBProteinEntityFasta(self, resourceName="pdbprent"):
        """Export RCSB protein entity sequence data (FASTA, taxon mapping, and essential details)"""
        ok = False
        try:
            pEx = PolymerEntityExtractor(self.__cfgOb)
            fastaPath = self.__getFastaPath(resourceName)
            taxonPath = self.__getTaxonPath(resourceName)
            detailsPath = self.__getDetailsPath(resourceName)
            ok = pEx.exportProteinEntityFasta(fastaPath, taxonPath, detailsPath)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def reloadUniProtTaxonomy(self):
        """Reload UniProt taxonomy mapping data from cached resource files"""
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
        """Update Uniprot taxonomy mapping data from source files"""
        startTime = time.time()
        umP = UniProtIdMappingProvider(cachePath=self.__cachePath)
        umP.clearCache()
        ok = umP.reload(useCache=True, useLegacy=False, fmt="tdd", mapNames=["NCBI-taxon"])
        logger.info("Completed building UniProt Id mapping (%r) at %s (%.4f seconds)", ok, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), time.time() - startTime)
        if ok and umP.testCache():
            ok = umP.backup(self.__cfgOb, self.__configName)
            logger.info("Completed backup UniProt Id mapping (%r)", ok)
        return ok

    def exportTargetsFasta(self, resourceNameList=None, useCache=True, addTaxonomy=False, reloadPharos=False, fromDbPharos=False, backupPharos=False, remotePrefix=None):
        """Export the target FASTA files for the input data resources.

        Args:
            resourceNameList (list, optional): list of data resources. Defaults to ["sabdab", "card", "drugbank", "chembl", "pharos", "pdbprent"].
            useCache (bool, optional): use cached data files. Defaults to True.
            addTaxonomy (bool, optional): add taxonomy details to each target record. Defaults to False.
            reloadPharos (bool, optional): reload Pharos target resources from SQL dump. Defaults to False.
            fromDbPharos (bool, optional): export Pharos target resources from local database server. Defaults to False.
            backupPharos (bool, optional): export Pharos target resources from local database server. Defaults to False.
            remotePrefix (str, optional): channel prefix for stash storage. Defaults to None.

        Returns:
            bool: True for success or False otherwise
        """
        resourceNameList = resourceNameList if resourceNameList else self.__defaultResourceNameList
        retOk = True
        for resourceName in resourceNameList:
            startTime = time.time()
            ok = self.__exportTargetsFasta(
                resourceName,
                useCache=useCache,
                addTaxonomy=addTaxonomy,
                reloadPharos=reloadPharos,
                fromDbPharos=fromDbPharos,
                backupPharos=backupPharos,
                remotePrefix=remotePrefix
            )
            logger.info("Completed loading %s targets (status %r) at %s (%.4f seconds)", resourceName, ok, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), time.time() - startTime)
            retOk = retOk and ok
        return retOk

    def __exportTargetsFasta(self, resourceName, useCache=True, addTaxonomy=False, reloadPharos=False, fromDbPharos=False, backupPharos=False, remotePrefix=None):
        ok = False
        try:
            configName = self.__cfgOb.getDefaultSectionName()
            fastaPath = self.__getFastaPath(resourceName)
            taxonPath = self.__getTaxonPath(resourceName)
            if resourceName == "card":
                cardtP = CARDTargetProvider(cachePath=self.__cachePath, useCache=useCache)
                cardtP.reload()
                if cardtP.testCache():
                    ok = cardtP.exportCardFasta(fastaPath, taxonPath)
            elif resourceName == "drugbank":
                user = self.__cfgOb.get("_DRUGBANK_AUTH_USERNAME", sectionName=configName)
                pw = self.__cfgOb.get("_DRUGBANK_AUTH_PASSWORD", sectionName=configName)
                dbtP = DrugBankTargetProvider(cachePath=self.__cachePath, useCache=False, username=user, password=pw)
                if dbtP.testCache():
                    ok = dbtP.exportFasta(fastaPath, taxonPath, addTaxonomy=addTaxonomy)
            elif resourceName == "chembl":
                chtP = ChEMBLTargetProvider(cachePath=self.__cachePath, useCache=useCache)
                chtP.reload()
                if chtP.testCache():
                    ok = chtP.exportFasta(fastaPath, taxonPath, addTaxonomy=addTaxonomy)
            elif resourceName == "pharos":
                user = self.__cfgOb.get("_MYSQL_DB_USER_NAME", sectionName=configName)
                pw = self.__cfgOb.get("_MYSQL_DB_PASSWORD", sectionName=configName)
                ptP = PharosTargetProvider(cachePath=self.__cachePath, useCache=useCache, reloadDb=reloadPharos, fromDb=fromDbPharos, mysqlUser=user, mysqlPassword=pw)
                if ptP.testCache():
                    ok = ptP.exportProteinFasta(fastaPath, taxonPath, addTaxonomy=addTaxonomy)
                    if ok and backupPharos and ptP.testCache():
                        okB = ptP.backup(self.__cfgOb, self.__configName, remotePrefix=remotePrefix, useStash=True, useGit=False)
                        logger.info("%r targets backup status (%r)", resourceName, okB)
                elif not (reloadPharos or fromDbPharos):
                    ptP.restore(self.__cfgOb, self.__configName, remotePrefix=remotePrefix, useStash=True, useGit=True)
                    if ptP.testCache():
                        ok = ptP.exportProteinFasta(fastaPath, taxonPath, addTaxonomy=addTaxonomy)
            elif resourceName == "sabdab":
                stP = SAbDabTargetProvider(cachePath=self.__cachePath, useCache=False)
                stP.reload()
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

    def createSearchDatabases(self, resourceNameList=None, addTaxonomy=False, timeOutSeconds=3600, verbose=False):
        """Create sequence search databases for the input target resources and optionally include taxonomy details

        Args:
            resourceNameList (list, optional): list of data resources. Defaults to ["sabdab", "card", "drugbank", "chembl", "pharos", "pdbprent"].
            timeOutSeconds (int, optional): timeout applied to database creation operations. Defaults to 3600s.
            verbose (bool, optional): verbose output. Defaults to False.

        Returns:
            bool: True for success or False otherwise
        """
        try:
            mU = MarshalUtil(workPath=self.__cachePath)
            resourceNameList = resourceNameList if resourceNameList else self.__defaultResourceNameList
            retOk = True
            for resourceName in resourceNameList:
                startTime = time.time()
                fastaPath = self.__getFastaPath(resourceName)
                taxonPath = self.__getTaxonPath(resourceName)
                mmS = MMseqsUtils(cachePath=self.__cachePath)
                ok = mmS.createSearchDatabase(fastaPath, self.__getDatabasePath(), resourceName, timeOut=timeOutSeconds, verbose=verbose)
                if addTaxonomy and ok and taxonPath and mU.exists(taxonPath):
                    ok = mmS.createTaxonomySearchDatabase(taxonPath, self.__getDatabasePath(), resourceName, timeOut=timeOutSeconds)
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

    def search(self, referenceResourceName, resourceNameList=None, identityCutoff=0.90, timeOutSeconds=10, sensitivity=4.5, useBitScore=False, formatOutput=None, maxSeqs=300):
        """Search for similar sequences in the reference resource and the input sequence resources.

        Args:
            referenceResourceName (str): reference resource name (e.g., pdbprent)
            resourceNameList (list, optional): list of data resources. Defaults to ["sabdab", "card", "drugbank", "chembl", "pharos", "pdbprent"].
            identityCutoff (float, optional): sequence identity cutoff value. Defaults to 0.90.
            timeOutSeconds (int, optional): sequence comparision operation timeout. Defaults to 10s.
            sensitivity (float, optional): mmseq2 search sensitivity. Defaults to 4.5.
            useBitScore (bool, optional): use bitscore value as an additional comparison threshold. Defaults to False.
            formatOutput(str, optional):  mmseq2 search fields exported. Defaults to "query,target,taxid,taxname,pident,alnlen,mismatch,
                                                                         gapopen,qstart,qend,tstart,tend,evalue,raw,bits,qlen,tlen,qaln,taln,cigar".
            maxSeqs (int): Maximum results per query sequence allowed to pass the prefilter (affects sensitivity). Defaults to 300.

        Returns:
             bool: True for success or False otherwise
        """
        resourceNameList = resourceNameList if resourceNameList else self.__defaultResourceNameList
        retOk = True
        for resourceName in resourceNameList:
            if resourceName == referenceResourceName:
                continue
            startTime = time.time()
            ok = self.__searchSimilar(
                referenceResourceName,
                resourceName,
                identityCutoff=identityCutoff,
                timeOut=timeOutSeconds,
                sensitivity=sensitivity,
                useBitScore=useBitScore,
                formatOutput=formatOutput,
                maxSeqs=maxSeqs,
            )
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

    def __searchSimilar(self, referenceResourceName, resourceName, identityCutoff=0.90, timeOut=10, sensitivity=4.5, useBitScore=False, formatOutput=None, maxSeqs=300):
        """Search for similar sequences in reference resource and input resources"""
        try:
            resultDirPath = self.__getResultDirPath()
            taxonPath = self.__getTaxonPath(resourceName)
            seqDbTopPath = self.__getDatabasePath()
            mU = MarshalUtil(workPath=self.__cachePath)
            mU.mkdir(resultDirPath)
            #
            mmS = MMseqsUtils(cachePath=self.__cachePath)
            rawPath = self.__getSearchResultPath(resourceName, referenceResourceName)
            resultPath = self.__getFilteredSearchResultPath(resourceName, referenceResourceName)
            ok = mmS.searchDatabase(
                resourceName, seqDbTopPath, referenceResourceName, rawPath, minSeqId=identityCutoff, timeOut=timeOut, sensitivity=sensitivity, formatOutput=formatOutput, maxSeqs=maxSeqs
            )
            #
            if taxonPath and mU.exists(taxonPath):
                mL = mmS.getMatchResults(rawPath, taxonPath, useTaxonomy=True, useTaxonomyCache=True, misMatchCutoff=-1, sequenceIdentityCutoff=identityCutoff, useBitScore=useBitScore)
            else:
                mL = mmS.getMatchResults(rawPath, None, useTaxonomy=False, misMatchCutoff=-1, sequenceIdentityCutoff=identityCutoff, useBitScore=useBitScore)
            logger.info("Query sequences with matches %r (%d) bitScore filter (%r)", resourceName, len(mL), useBitScore)
            mU.doExport(resultPath, mL, fmt="json")
            return ok and mL is not None
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def buildFeatureData(self, referenceResourceName, resourceNameList=None, useTaxonomy=True, backup=False, remotePrefix=None):
        """Create feature data for the input data resources based on sequence comparison with the
           input reference resource.

        Args:
            referenceResourceName (str): reference resource name (e.g., pdbprent)
            resourceNameList (list, optional): list of data resources. Defaults to ["sabdab", "card", "imgt"].
            useTaxonomy (bool, optional): use taxonomy in filtering selections where implemented  (e.g., card). Defaults to True.
            backup (bool, optional): backup results to stash storage. Defaults to False.
            remotePrefix (str, optional): channel prefix for stash storage. Defaults to None.

        Returns:
            bool: True for success or False otherwise
        """
        resourceNameList = resourceNameList if resourceNameList else ["sabdab", "card", "imgt"]
        retOk = True
        for resourceName in resourceNameList:
            startTime = time.time()
            ok = self.__buildFeatureData(referenceResourceName, resourceName, useTaxonomy=useTaxonomy, backup=backup, remotePrefix=remotePrefix)
            logger.info(
                "Completed building features for %s (status %r)  at %s (%.4f seconds)",
                resourceName,
                ok,
                time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                time.time() - startTime,
            )
            retOk = retOk and ok
        #
        return retOk

    def __buildFeatureData(self, referenceResourceName, resourceName, useTaxonomy=False, backup=False, remotePrefix=None):
        """Build features and annotations inferred from sequence comparison results between the input resources."""
        try:
            okB = True
            resultPath = self.__getFilteredSearchResultPath(resourceName, referenceResourceName)
            #
            if resourceName == "sabdab":
                fP = SAbDabTargetFeatureProvider(cachePath=self.__cachePath, useCache=True)
                ok = fP.buildFeatureList(resultPath)
                fP.reload()
                if ok and backup and fP.testCache():
                    okB = fP.backup(self.__cfgOb, self.__configName, remotePrefix=remotePrefix, useStash=True, useGit=False)
                    logger.info("%r features backup status (%r)", resourceName, okB)
            elif resourceName == "card":
                fP = CARDTargetOntologyProvider(cachePath=self.__cachePath, useCache=True)
                ok = fP.buildOntologyData()
                fP.reload()
                if ok and backup and fP.testCache():
                    okB = fP.backup(self.__cfgOb, self.__configName, remotePrefix=remotePrefix, useStash=True, useGit=False)
                    logger.info("%r ontology backup status (%r)", resourceName, okB)
                fP = CARDTargetAnnotationProvider(cachePath=self.__cachePath, useCache=True)
                ok = fP.buildAnnotationList(resultPath, useTaxonomy=useTaxonomy)
                fP.reload()
                if ok and backup and fP.testCache():
                    okB = fP.backup(self.__cfgOb, self.__configName, remotePrefix=remotePrefix, useStash=True, useGit=False)
                    logger.info("%r annotations backup status (%r)", resourceName, okB)
            elif resourceName == "imgt":
                fP = IMGTTargetFeatureProvider(cachePath=self.__cachePath, useCache=True)
                ok = fP.buildFeatureList(useCache=True)
                fP.reload()
                if ok and backup and fP.testCache():
                    okB = fP.backup(self.__cfgOb, self.__configName, remotePrefix=remotePrefix, useStash=True, useGit=False)
                    logger.info("%r features backup status (%r)", resourceName, okB)
            return ok and okB
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def buildActivityData(self, referenceResourceName, resourceNameList=None, backup=False, remotePrefix=None, maxTargets=None):
        """Create activity data for the input data resources based on sequence comparison with the
           input reference resource.

        Args:
            referenceResourceName (str): reference resource name (e.g., pdbprent)
            resourceNameList (list, optional): list of data resources. Defaults to ["pharos", "chembl"].
            backup (bool, optional): backup results to stash storage. Defaults to False.
            remotePrefix (str, optional): channel prefix for stash storage. Defaults to None.
            maxTargets (int, optional): fetching activities (ChEMBL) for no more than maxTargets (for testing).  Defaults to None.

        Returns:
            bool: True for success or False otherwise
        """
        resourceNameList = resourceNameList if resourceNameList else ["pharos", "chembl"]
        retOk = True
        for resourceName in resourceNameList:
            startTime = time.time()
            ok = self.__buildActivityData(referenceResourceName, resourceName, backup=backup, remotePrefix=remotePrefix, maxTargets=maxTargets)
            logger.info(
                "Completed building activity data for %s (status %r)  at %s (%.4f seconds)",
                resourceName,
                ok,
                time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                time.time() - startTime,
            )
            retOk = retOk and ok
        #
        return retOk

    def __buildActivityData(self, referenceResourceName, resourceName, backup=False, remotePrefix=None, maxTargets=None):
        """Build features inferred from sequence comparison results between the input resources."""
        try:
            okB = okC = okD = True
            resultPath = self.__getFilteredSearchResultPath(resourceName, referenceResourceName)
            #
            if resourceName == "chembl":
                aP = ChEMBLTargetActivityProvider(cachePath=self.__cachePath, useCache=True)
                try:
                    aP.restore(self.__cfgOb, self.__configName, remotePrefix=remotePrefix)
                    aP.reload()
                except Exception:
                    pass
                targetIdList = aP.getTargetIdList(resultPath)
                targetIdList = targetIdList[:maxTargets] if maxTargets else targetIdList
                # To rebuild ChEMBL-target-activity data from scratch (non-incremental), change skip=None
                ok = aP.fetchTargetActivityDataMulti(targetIdList, skip="tried", chunkSize=50, numProc=6)
                #
                aP.reload()
                if ok and backup and aP.testCache():
                    okB = aP.backup(self.__cfgOb, self.__configName, remotePrefix=remotePrefix, useStash=True, useGit=False)
                    logger.info("%r activity backup status (%r)", resourceName, okB)
            elif resourceName == "pharos":
                aP = PharosTargetActivityProvider(cachePath=self.__cachePath, useCache=True)
                ok = aP.fetchTargetActivityData()
                aP.reload()
                if ok and backup and aP.testCache(minCount=1):
                    okB = aP.backup(self.__cfgOb, self.__configName, remotePrefix=remotePrefix, useStash=True, useGit=False)
                    logger.info("%r activity data backup status (%r)", resourceName, okB)
                #
                chemblIdList = aP.fetchCompoundIdentifiers()
                phP = PharosProvider(cachePath=self.__cachePath, useCache=False)
                okC = phP.load(chemblIdList, "identifiers", fmt="json", indent=0)
                phP.reload()
                if okC and backup and phP.testCache(minCount=1):
                    okD = phP.backup(self.__cfgOb, self.__configName, remotePrefix=remotePrefix, useStash=True, useGit=False)
                    logger.info("%r identifier backup status (%r)", resourceName, okC)

            return ok and okB and okC and okD
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def buildCofactorData(self, referenceResourceName, resourceNameList=None, backup=False, remotePrefix=None, maxActivity=10):
        """Assemble cofactor data for the input data resources based on sequence comparison with the
           input reference resource.

        Args:
            referenceResourceName (str): reference resource name (e.g., pdbprent)
            resourceNameList (list, optional): list of data resources. Defaults to ["pharos", "chembl"].
            backup (bool, optional): backup results to stash storage. Defaults to False.
            remotePrefix (str, optional): channel prefix for stash storage. Defaults to None.
            maxActivity (int, optional): limit for the number cofactors/activities incorporated per target. Default to 10.

        Returns:
            bool: True for success or False otherwise
        """
        resourceNameList = resourceNameList if resourceNameList else ["chembl", "pharos", "drugbank"]
        retOk = True
        for resourceName in resourceNameList:
            startTime = time.time()
            ok = self.__buildCofactorData(referenceResourceName, resourceName, backup=backup, remotePrefix=remotePrefix, maxActivity=maxActivity)
            logger.info(
                "Completed building cofactor data for %s (status %r)  at %s (%.4f seconds)",
                resourceName,
                ok,
                time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                time.time() - startTime,
            )
            retOk = retOk and ok
        #
        return retOk

    def __buildCofactorData(self, referenceResourceName, resourceName, backup=False, remotePrefix=None, maxActivity=10):
        """Build cofactor data inferred from sequence comparison results between the input resources."""
        try:
            crmpObj = ChemRefMappingProvider(cachePath=self.__cachePath, useCache=True)
            lnmpObj = LigandNeighborMappingProvider(cachePath=self.__cachePath, useCache=True)

            ok = okB = True
            resultPath = self.__getFilteredSearchResultPath(resourceName, referenceResourceName)
            #
            if resourceName == "chembl":
                aP = ChEMBLTargetCofactorProvider(cachePath=self.__cachePath, useCache=True)
                ok = aP.buildCofactorList(resultPath, crmpObj=crmpObj, lnmpObj=lnmpObj, maxActivity=maxActivity)
                ok = aP.reload() and ok
                logger.info("%r cofactor data build status (%r)", resourceName, ok)
                #
                if ok and backup and aP.testCache():
                    okB = aP.backup(self.__cfgOb, self.__configName, remotePrefix=remotePrefix, useStash=True, useGit=False)
                    logger.info("%r cofactor data backup status (%r)", resourceName, okB)

            elif resourceName == "pharos":
                aP = PharosTargetCofactorProvider(cachePath=self.__cachePath, useCache=True, useStash=True, useGit=True)
                ok = aP.buildCofactorList(resultPath, crmpObj=crmpObj, lnmpObj=lnmpObj, maxActivity=maxActivity)
                ok = aP.reload() and ok
                logger.info("%r cofactor data build status (%r)", resourceName, ok)
                #
                if ok and backup and aP.testCache():
                    okB = aP.backup(self.__cfgOb, self.__configName, remotePrefix=remotePrefix, useStash=True, useGit=False)
                    logger.info("%r cofactor data backup status (%r)", resourceName, okB)

            elif resourceName == "drugbank":
                aP = DrugBankTargetCofactorProvider(cachePath=self.__cachePath, useCache=True)
                ok = aP.buildCofactorList(resultPath, crmpObj=crmpObj, lnmpObj=lnmpObj)
                ok = aP.reload() and ok
                logger.info("%r cofactor data build status (%r)", resourceName, ok)
                #
                if ok and backup and aP.testCache():
                    okB = aP.backup(self.__cfgOb, self.__configName, remotePrefix=remotePrefix, useStash=True, useGit=False)
                    logger.info("%r cofactor data backup status (%r)", resourceName, okB)
            #
            return ok and okB
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def loadTargetCofactorData(self, resourceNameList=None):
        """Load target cofactor data to MongoDB.

        Args:
            resourceNameList (list, optional): list of data resources. Defaults to ["pharos", "chembl"].

        Returns:
            bool: True for success or False otherwise
        """
        resourceNameList = resourceNameList if resourceNameList else ["chembl", "pharos", "drugbank"]
        retOk = True
        for resourceName in resourceNameList:
            startTime = time.time()
            ok = self.__loadTargetCofactorData(resourceName)
            logger.info(
                "Completed loading target cofactor data for %s (status %r)  at %s (%.4f seconds)",
                resourceName,
                ok,
                time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                time.time() - startTime,
            )
            retOk = retOk and ok
        #
        return retOk

    def __loadTargetCofactorData(self, resourceName):
        """Load cofactor data inferred from sequence comparison results to MongoDB."""
        try:
            ok = okLoad = False
            aP = None
            #
            if resourceName == "chembl":
                aP = ChEMBLTargetCofactorProvider(cachePath=self.__cachePath, useCache=True)
            elif resourceName == "pharos":
                aP = PharosTargetCofactorProvider(cachePath=self.__cachePath, useCache=True, useStash=True, useGit=True)
            elif resourceName == "drugbank":
                aP = DrugBankTargetCofactorProvider(cachePath=self.__cachePath, useCache=True)
            #
            ok = aP.reload()
            logger.info("%r cofactor data reload status (%r)", resourceName, ok)
            #
            if ok and aP.testCache():
                okLoad = aP.loadCofactorData(cfgOb=self.__cfgOb)

            return ok and okLoad
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def __getDatabasePath(self):
        return os.path.join(self.__cachePath, "sequence-databases")

    def __getResultDirPath(self):
        return os.path.join(self.__cachePath, "sequence-search-results")

    def __getFilteredSearchResultPath(self, queryResourceName, referenceResourceName):
        return os.path.join(self.__getResultDirPath(), queryResourceName + "-vs-" + referenceResourceName + "-filtered-results.json")

    def __getSearchResultPath(self, queryResourceName, referenceResourceName):
        return os.path.join(self.__getResultDirPath(), queryResourceName + "-vs-" + referenceResourceName + "-raw-results.json")

    def __getFastaPath(self, resourceName):
        return os.path.join(self.__cachePath, "FASTA", resourceName + "-targets.fa")

    def __getTaxonPath(self, resourceName):
        if resourceName == "sabdab":
            return None
        return os.path.join(self.__cachePath, "FASTA", resourceName + "-targets-taxon.tdd")

    def __getDetailsPath(self, resourceName):
        return os.path.join(self.__cachePath, resourceName, resourceName + "-details.json")
