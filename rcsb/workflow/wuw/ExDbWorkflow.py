##
# File: ExDbWorkflow.py
# Date: 17-Dec-2019  jdw
#
#  Workflow wrapper  --  exchange database loading utilities --
#
#  Updates:
#   2-Mar-2023 dwp Add "numProc" parameter to 'upd_ref_seq' operation methods
#   9-Mar-2023 dwp Lower refChunkSize to 10 (UniProt API having trouble streaming XML responses)
#  25-Apr-2024 dwp Add arguments and methods to support CLI usage from weekly-update workflow
#  20-Aug-2024 dwp Add LoadTargetCofactors step; change name of UpdateTargetsCofactors step to UpdateTargetsData
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os

from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.utils.TimeUtil import TimeUtil
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.dictionary.DictMethodResourceProvider import DictMethodResourceProvider
from rcsb.utils.dictionary.DictMethodResourceCacheWorkflow import DictMethodResourceCacheWorkflow
from rcsb.utils.dictionary.NeighborInteractionWorkflow import NeighborInteractionWorkflow
from rcsb.workflow.targets.ProteinTargetSequenceExecutionWorkflow import ProteinTargetSequenceExecutionWorkflow
from rcsb.workflow.chem.ChemCompImageWorkflow import ChemCompImageWorkflow
from rcsb.workflow.chem.ChemCompFileWorkflow import ChemCompFileWorkflow
from rcsb.exdb.chemref.ChemRefEtlWorker import ChemRefEtlWorker
from rcsb.exdb.seq.ReferenceSequenceAnnotationAdapter import ReferenceSequenceAnnotationAdapter
from rcsb.exdb.seq.ReferenceSequenceAnnotationProvider import ReferenceSequenceAnnotationProvider
from rcsb.exdb.seq.UniProtCoreEtlWorker import UniProtCoreEtlWorker
from rcsb.exdb.tree.TreeNodeListWorker import TreeNodeListWorker
from rcsb.exdb.utils.ObjectTransformer import ObjectTransformer
from rcsb.exdb.wf.EntryInfoEtlWorkflow import EntryInfoEtlWorkflow
from rcsb.exdb.wf.GlycanEtlWorkflow import GlycanEtlWorkflow
from rcsb.exdb.wf.PubChemEtlWorkflow import PubChemEtlWorkflow

logger = logging.getLogger(__name__)


class ExDbWorkflow(object):
    def __init__(self, **kwargs):
        #  Configuration Details
        self.__configPath = kwargs.get("configPath", "exdb-config-example.yml")
        self.__configName = kwargs.get("configName", "site_info_remote_configuration")
        self.__mockTopPath = kwargs.get("mockTopPath", None)
        self.__cfgOb = ConfigUtil(configPath=self.__configPath, defaultSectionName=self.__configName, mockTopPath=self.__mockTopPath)
        #
        self.__cachePath = kwargs.get("cachePath", ".")
        self.__cachePath = os.path.abspath(self.__cachePath)
        self.__debugFlag = kwargs.get("debugFlag", False)
        if self.__debugFlag:
            logger.setLevel(logging.DEBUG)
        #
        # Rebuild cache (default False)
        rebuildCache = kwargs.get("rebuildCache", False)
        self.__useCache = not rebuildCache
        providerTypeExcludeL = kwargs.get("providerTypeExcludeL", None)
        restoreUseGit = kwargs.get("restoreUseGit", True)
        restoreUseStash = kwargs.get("restoreUseStash", True)
        self.__cacheStatus = True
        if rebuildCache:
            logger.info("Rebuilding cache %r", rebuildCache)
            self.__cacheStatus = self.buildResourceCache(
                rebuildCache=rebuildCache,
                providerTypeExcludeL=providerTypeExcludeL,
                restoreUseStash=restoreUseStash,
                restoreUseGit=restoreUseGit,
            )
            logger.info("Cache status if %r", self.__cacheStatus)
            if not self.__cacheStatus:
                logger.error("Failed to rebuild CACHE in ExDBWorkflow")

    def load(self, op, **kwargs):
        logger.info("Starting operation %r\n", op)
        #
        # argument processing
        if op not in ["etl_tree_node_lists", "etl_chemref", "etl_uniprot_core", "upd_ref_seq", "upd_ref_seq_comp_models", "refresh_pubchem"]:
            logger.error("Unsupported operation %r - exiting", op)
            return False
        if not self.__cacheStatus:
            raise ValueError("Failed to rebuild CACHE in ExDBWorkflow - exiting")
        try:
            # test mode and UniProt accession primary match minimum count for doReferenceSequenceUpdate()
            testMode = kwargs.get("testMode", False)
            minMatchPrimaryPercent = kwargs.get("minMatchPrimaryPercent", None)
            minMissing = kwargs.get("minMissing", 0)
            #
            readBackCheck = kwargs.get("readBackCheck", True)
            numProc = int(kwargs.get("numProc", 1))
            chunkSize = int(kwargs.get("chunkSize", 10))
            maxStepLength = int(kwargs.get("maxStepLength", 500))
            refChunkSize = int(kwargs.get("refChunkSize", 10))
            documentLimit = kwargs.get("documentLimit", None)
            documentLimit = int(documentLimit) if documentLimit else None
            loadType = kwargs.get("loadType", "full")  # or replace
            dbType = kwargs.get("dbType", "mongo")
            tU = TimeUtil()
            dataSetId = kwargs.get("dataSetId") if "dataSetId" in kwargs else tU.getCurrentWeekSignature()
            #  Rebuild or reuse reference sequence cache
            rebuildSequenceCache = kwargs.get("rebuildSequenceCache", False)
            useSequenceCache = not rebuildSequenceCache
            #
            useFilteredLists = kwargs.get("useFilteredLists", False)

        except Exception as e:
            logger.exception("Argument or configuration processing failing with %s", str(e))
            return False
        #
        okS = ok = False
        if dbType == "mongo":
            if op == "etl_tree_node_lists":
                rhw = TreeNodeListWorker(
                    self.__cfgOb,
                    self.__cachePath,
                    numProc=numProc,
                    chunkSize=chunkSize,
                    maxStepLength=maxStepLength,
                    documentLimit=documentLimit,
                    verbose=self.__debugFlag,
                    readBackCheck=readBackCheck,
                    useCache=self.__useCache,
                    useFilteredLists=useFilteredLists,
                )
                ok = rhw.load(dataSetId, loadType=loadType)
                okS = self.loadStatus(rhw.getLoadStatus(), readBackCheck=readBackCheck)

            elif op == "etl_chemref":
                crw = ChemRefEtlWorker(
                    self.__cfgOb,
                    self.__cachePath,
                    numProc=numProc,
                    chunkSize=chunkSize,
                    maxStepLength=maxStepLength,
                    documentLimit=documentLimit,
                    verbose=self.__debugFlag,
                    readBackCheck=readBackCheck,
                    useCache=self.__useCache,
                )
                ok = crw.load(dataSetId, extResource="DrugBank", loadType=loadType)
                okS = self.loadStatus(crw.getLoadStatus(), readBackCheck=readBackCheck)

            elif op == "etl_uniprot_core":
                crw = UniProtCoreEtlWorker(
                    self.__cfgOb,
                    self.__cachePath,
                    numProc=numProc,
                    chunkSize=chunkSize,
                    maxStepLength=maxStepLength,
                    documentLimit=documentLimit,
                    verbose=self.__debugFlag,
                    readBackCheck=readBackCheck,
                    useCache=self.__useCache,
                )
                ok = crw.load(dataSetId, extResource="UniProt", loadType=loadType)
                okS = self.loadStatus(crw.getLoadStatus(), readBackCheck=readBackCheck)

            elif op == "upd_ref_seq":
                databaseName = "pdbx_core"
                collectionName = "pdbx_core_polymer_entity"
                polymerType = "Protein"
                ok = self.doReferenceSequenceUpdate(
                    databaseName,
                    collectionName,
                    polymerType,
                    fetchLimit=documentLimit,
                    useSequenceCache=useSequenceCache,
                    testMode=testMode,
                    minMatchPrimaryPercent=minMatchPrimaryPercent,
                    minMissing=minMissing,
                    refChunkSize=refChunkSize,
                    numProc=numProc
                )
                okS = ok
            # elif op == "upd_ref_seq_comp_models":
            #     databaseName = "pdbx_comp_model_core"
            #     collectionName = "pdbx_comp_model_core_polymer_entity"
            #     polymerType = "Protein"
            #     ok = self.doReferenceSequenceUpdate(
            #         databaseName,
            #         collectionName,
            #         polymerType,
            #         fetchLimit=documentLimit,
            #         useSequenceCache=useSequenceCache,
            #         testMode=testMode,
            #         minMatchPrimaryPercent=minMatchPrimaryPercent,
            #         minMissing=minMissing,
            #         refChunkSize=refChunkSize,
            #     )
            #     okS = ok
        #
        logger.info("Completed operation %r with status %r\n", op, ok and okS)
        return ok and okS

    def loadStatus(self, statusList, readBackCheck=True):
        ret = False
        try:
            dl = DocumentLoader(self.__cfgOb, self.__cachePath, "MONGO_DB", numProc=1, chunkSize=2, documentLimit=None, verbose=False, readBackCheck=readBackCheck)
            #
            sectionName = "data_exchange_configuration"
            databaseName = self.__cfgOb.get("DATABASE_NAME", sectionName=sectionName)
            collectionName = self.__cfgOb.get("COLLECTION_UPDATE_STATUS", sectionName=sectionName)
            ret = dl.load(databaseName, collectionName, loadType="append", documentList=statusList, indexAttributeList=["update_id", "database_name", "object_name"], keyNames=None)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ret

    def buildResourceCache(self, rebuildCache=False, providerTypeExcludeL=None, restoreUseStash=True, restoreUseGit=True):
        """Generate and cache resource dependencies."""
        ret = False
        try:
            # First make sure the CACHE directory exists
            if not os.path.isdir(self.__cachePath):
                logger.info("Cache directory %s doesn't exist. Creating it", self.__cachePath)
                os.makedirs(self.__cachePath)
            else:
                logger.info("Cache directory %s already exists.", self.__cachePath)

            # Now build the cache
            useCache = not rebuildCache
            rP = DictMethodResourceProvider(
                self.__cfgOb,
                configName=self.__configName,
                cachePath=self.__cachePath,
                restoreUseStash=restoreUseStash,
                restoreUseGit=restoreUseGit,
                providerTypeExcludeL=providerTypeExcludeL,
            )
            ret = rP.cacheResources(useCache=useCache, doBackup=False)
            logger.info("useCache %r cache reload status (%r)", useCache, ret)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ret

    def doReferenceSequenceUpdate(
        self,
        databaseName,
        collectionName,
        polymerType,
        fetchLimit=None,
        useSequenceCache=False,
        testMode=False,
        minMatchPrimaryPercent=None,
        minMissing=0,
        refChunkSize=10,
        numProc=2,
        **kwargs
    ):
        try:
            _ = kwargs
            _ = testMode
            ok = False
            # -------
            rsaP = ReferenceSequenceAnnotationProvider(
                self.__cfgOb, databaseName, collectionName, polymerType, useCache=useSequenceCache, cachePath=self.__cachePath, maxChunkSize=refChunkSize, numProc=numProc
            )
            ok = rsaP.testCache(minMatchPrimaryPercent=minMatchPrimaryPercent, minMissing=minMissing)
            if ok:
                logger.info("Cached reference data count is %d", rsaP.getRefDataCount())
                rsa = ReferenceSequenceAnnotationAdapter(rsaP)
                obTr = ObjectTransformer(self.__cfgOb, objectAdapter=rsa)
                ok = obTr.doTransform(
                    databaseName=databaseName, collectionName=collectionName, fetchLimit=fetchLimit, selectionQuery={"entity_poly.rcsb_entity_polymer_type": polymerType}
                )
            else:
                logger.error("Reference sequence data cache build failing")
                return False
            return ok
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def generateCcdFiles(self, op, **kwargs):
        logger.info("Starting operation %r\n", op)
        #
        # argument processing
        if op not in [
            "ccd_img_gen",
            "ccd_file_gen",
        ]:
            logger.error("Unsupported operation %r - exiting", op)
            return False
        try:
            ccOutputPath = kwargs.get("ccOutputPath", None)
            ccCachePath = kwargs.get("ccCachePath", None)
            licenseFilePath = kwargs.get("licenseFilePath", None)
            ccUrlTarget = kwargs.get("ccUrlTarget", None)
            birdUrlTarget = kwargs.get("birdUrlTarget", None)
            #
        except Exception as e:
            logger.exception("Argument or configuration processing failing with %s", str(e))
            return False
        #
        ok = False
        if op == "ccd_img_gen":
            logger.info("Generating CCD 2D images ...")
            cciWf = ChemCompImageWorkflow(
                imagePath=ccOutputPath,
                cachePath=ccCachePath,
                licenseFilePath=licenseFilePath,
                ccUrlTarget=ccUrlTarget,
                birdUrlTarget=birdUrlTarget,
            )
            ok = cciWf.testCache()
            logger.info("CCD image generation setup status %r", ok)
            #
            ok = cciWf.makeImages() and ok
            logger.info("CCD image generation status %r", ok)
        #
        elif op == "ccd_file_gen":
            logger.info("Generating SDF and Mol2 files from CCD...")
            # CCD ideal coordinates
            ccfWf = ChemCompFileWorkflow(
                fileDirPath=ccOutputPath,
                cachePath=ccCachePath,
                licenseFilePath=licenseFilePath,
                ccUrlTarget=ccUrlTarget,
                birdUrlTarget=birdUrlTarget,
                molBuildType="ideal-xyz"
            )
            ok1 = ccfWf.testCache()
            logger.info("CCD ideal coordinates generation setup status %r", ok1)
            #
            ok1 = ccfWf.makeFiles(fmt="sdf") and ok1
            logger.info("CCD ideal file sdf generation status %r", ok1)
            ok1 = ccfWf.makeFiles(fmt="mol2") and ok1
            logger.info("CCD ideal file mol2 generation status %r", ok1)

            # CCD model coordinates
            ccfWf = ChemCompFileWorkflow(
                fileDirPath=ccOutputPath,
                cachePath=ccCachePath,
                licenseFilePath=licenseFilePath,
                ccUrlTarget=ccUrlTarget,
                birdUrlTarget=birdUrlTarget,
                molBuildType="model-xyz"
            )
            ok2 = ccfWf.testCache()
            logger.info("CCD model coordinates generation setup status %r", ok2)
            #
            ok2 = ccfWf.makeFiles(fmt="sdf") and ok2
            logger.info("CCD model file sdf generation status %r", ok2)
            ok2 = ccfWf.makeFiles(fmt="mol2") and ok2
            logger.info("CCD model file mol2 generation status %r", ok2)
            #
            ok = ok1 and ok2
        #
        logger.info("Completed operation %r with status %r\n", op, ok)
        if not ok:
            logger.error("%r FAILED with status %s", op, ok)
            raise ValueError("%r FAILED. Check the loader log for details." % op)

        return ok

    def buildExdbResource(self, op, **kwargs):
        logger.info("Starting operation %r\n", op)
        #
        # argument processing
        if op not in [
            "upd_neighbor_interactions",
            "upd_uniprot_taxonomy",
            "upd_targets",
            "load_target_cofactors",
            "upd_pubchem",
            "upd_entry_info",
            "upd_glycan_idx",
            "upd_resource_stash",
        ]:
            logger.error("Unsupported operation %r - exiting", op)
            return False
        try:
            numProc = int(kwargs.get("numProc", 1))
            chunkSize = int(kwargs.get("chunkSize", 10))
            documentLimit = kwargs.get("documentLimit", None)
            documentLimit = int(documentLimit) if documentLimit else None
            dbType = kwargs.get("dbType", "mongo")
            rebuildAllNeighborInteractions = kwargs.get("rebuildAllNeighborInteractions", False)
            incrementalUpdate = not rebuildAllNeighborInteractions
            ccFileNamePrefix = kwargs.get("ccFileNamePrefix", "cc-full")
            ccUrlTarget = kwargs.get("ccUrlTarget", None)
            birdUrlTarget = kwargs.get("birdUrlTarget", None)
            rebuildChemIndices = kwargs.get("rebuildChemIndices", True)
            #
        except Exception as e:
            logger.exception("Argument or configuration processing failing with %s", str(e))
            return False
        #
        ok = False
        if dbType == "mongo":
            if op == "upd_neighbor_interactions":
                logger.info("Starting workflow NeighborInteractionWorkflow.buildResourceCache()")
                niWf = NeighborInteractionWorkflow(
                    configPath=self.__configPath,
                    mockTopPath=self.__mockTopPath,
                    configName=self.__configName,
                    cachePath=self.__cachePath,
                    numProc=numProc,
                    chunkSize=chunkSize,
                )
                ok = niWf.update(incremental=incrementalUpdate)
                logger.info("UpdateNeighborInteraction status %r", ok)
                ok = niWf.backup() and ok
                logger.info("UpdateNeighborInteraction backup status %r", ok)
            #
            elif op == "upd_uniprot_taxonomy":
                logger.info("Starting workflow ProteinTargetSequenceExecutionWorkflow (full)")
                ptsWf = ProteinTargetSequenceExecutionWorkflow(
                    configPath=self.__configPath,
                    mockTopPath=self.__mockTopPath,
                    configName=self.__configName,
                    cachePath=self.__cachePath,
                )
                ok = ptsWf.cacheTaxonomy()
                logger.info("cacheTaxonomy status %r", ok)
                ok = ptsWf.updateUniProtTaxonomy() and ok
                logger.info("updateUniProtTaxonomy status %r", ok)
            #
            elif op == "upd_targets":
                logger.info("Starting UpdateTargetsData")
                ptsWf = ProteinTargetSequenceExecutionWorkflow(
                    configPath=self.__configPath,
                    mockTopPath=self.__mockTopPath,
                    configName=self.__configName,
                    cachePath=self.__cachePath,
                )
                ok = ptsWf.fetchProteinEntityData()
                logger.info("fetchProteinEntityData status %r", ok)
                ok = ptsWf.fetchChemicalReferenceMappingData() and ok
                logger.info("fetchChemicalReferenceMappingData status %r", ok)
                ok = ptsWf.fetchLigandNeighborMappingData() and ok
                logger.info("fetchLigandNeighborMappingData status %r", ok)
                ok = ptsWf.exportFasta(reloadPharos=False, fromDbPharos=False) and ok
                logger.info("exportFasta status %r", ok)
                ok = ptsWf.createSearchDatabases() and ok
                logger.info("createSearchDatabases status %r", ok)
                ok = ptsWf.searchDatabases() and ok
                logger.info("searchDatabases status %r", ok)
                ok = ptsWf.buildFeatures() and ok
                logger.info("buildFeatures status %r", ok)
                ok = ptsWf.buildActivityData() and ok
                logger.info("buildActivityData status %r", ok)
                ok = ptsWf.buildCofactorData() and ok
                logger.info("buildCofactorData status %r", ok)
                ptsWf.resourceCheck()
            #
            elif op == "load_target_cofactors":
                logger.info("Starting LoadTargetCofactors")
                ptsWf = ProteinTargetSequenceExecutionWorkflow(
                    configPath=self.__configPath,
                    mockTopPath=self.__mockTopPath,
                    configName=self.__configName,
                    cachePath=self.__cachePath,
                )
                ok = ptsWf.loadTargetCofactorData()
                logger.info("loadTargetCofactorData status %r", ok)
                ptsWf.resourceCheck()
            #
            elif op == "upd_pubchem":
                #  -- Update local chemical indices and  create PubChem mapping index ---
                logger.info("Starting workflow PubChemEtlWorkflow")
                pcewP = PubChemEtlWorkflow(
                    configPath=self.__configPath,
                    mockTopPath=self.__mockTopPath,
                    configName=self.__configName,
                    cachePath=self.__cachePath,
                )
                logger.info("Starting workflow PubChemEtlWorkflow.updateMatchedIndex()")
                ok1 = pcewP.updateMatchedIndex(
                    rebuildChemIndices=rebuildChemIndices,
                    ccUrlTarget=ccUrlTarget,
                    birdUrlTarget=birdUrlTarget,
                    ccFileNamePrefix=ccFileNamePrefix,
                    # numProc=numProc,  # Leave at default of 2, due to throttling at PubChem
                    # chunkSize=chunkSize,  # need to propagate this through
                )
                logger.info("updateMatchedIndex status %r", ok1)
                #
                logger.info("Starting workflow PubChemEtlWorkflow.updateMatchedData()")
                ok2 = pcewP.updateMatchedData(numProc=numProc)
                logger.info("updateMatchedData status %r", ok2)
                ok = ok1 and ok2
            #
            elif op == "upd_entry_info":
                logger.info("Starting workflow EntryInfoEtlWorkflow.update()")
                ewf = EntryInfoEtlWorkflow(
                    configPath=self.__configPath,
                    mockTopPath=self.__mockTopPath,
                    configName=self.__configName,
                    cachePath=self.__cachePath,
                )
                ok = ewf.update()
            #
            elif op == "upd_glycan_idx":
                logger.info("Starting workflow GlycanEtlWorkflow.updateMatchedIndex()")
                gwf = GlycanEtlWorkflow(
                    configPath=self.__configPath,
                    mockTopPath=self.__mockTopPath,
                    configName=self.__configName,
                    cachePath=self.__cachePath,
                )
                ok = gwf.updateMatchedIndex()
            #
            elif op == "upd_resource_stash":
                logger.info("Starting workflow DictMethodResourceCacheWorkflow.buildResourceCache()")
                dmWf = DictMethodResourceCacheWorkflow(
                    configPath=self.__configPath,
                    mockTopPath=self.__mockTopPath,
                    configName=self.__configName,
                    cachePath=self.__cachePath,
                )
                ok = dmWf.buildResourceCache()
        #
        logger.info("Completed operation %r with status %r\n", op, ok)
        if not ok:
            logger.error("%r FAILED with status %s", op, ok)
            raise ValueError("%r FAILED. Check the loader log for details." % op)

        return ok
