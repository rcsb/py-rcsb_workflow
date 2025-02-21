##
# File:    bcif_workflow_utilities.py
# Author:  James Smith
# Date:    21-Feb-2025
##

"""
Functions borrowed from original bcif workflow at github.com/rcsb/weekly-update-workflow.
"""

__docformat__ = "google en"
__author__ = "James Smith"
__email__ = "james.smith@rcsb.org"
__license__ = "Apache 2.0"

import os
from enum import Enum
import datetime
import logging
from typing import Tuple, Dict
from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


class ContentTypeEnum(Enum):
    EXPERIMENTAL = "experimental"
    COMPUTATIONAL = "computational"


class BcifWorkflowUtilities:

    def __init__(
        self,
        updateBase: str = None,
        tempPath: str = None,
        prereleaseFtpFileBasePath: str = None,
        pdbIdsTimestampFilePath: str = None,
        csmFileRepoBasePath: str = None,
        csmHoldingsUrl: str = None,
        structureFilePath: str = None,
    ):
        self.contentTypeDir: Dict = {
            ContentTypeEnum.EXPERIMENTAL.value: "pdb",
            ContentTypeEnum.COMPUTATIONAL.value: "csm",
        }
        self.updateBase = updateBase
        self.tempPath = tempPath
        self.prereleaseFtpFileBasePath = prereleaseFtpFileBasePath
        self.pdbIdsTimestampFilePath = pdbIdsTimestampFilePath
        self.csmFileRepoBasePath = csmFileRepoBasePath
        self.csmHoldingsUrl = csmHoldingsUrl
        self.structureFilePath = structureFilePath

    # list file download

    def getPdbList(self, loadType: str) -> list:
        pdbIdsTimestamps, url = self.getAllCurrentPdbIdsWithTimestamps()
        logger.info("read timestamps from %s", url)
        pdbList = []
        contentType = ContentTypeEnum.EXPERIMENTAL.value
        # /mnt/models/update-store/pdb
        # changed to
        # /mnt/vdb1/out/pdb
        baseDir = os.path.join(self.updateBase, self.contentTypeDir[contentType])
        if loadType == "full":
            logger.info("running full workflow")
            for pdbId in pdbIdsTimestamps:
                pdbPath = pdbId[1:3] + "/" + pdbId + ".cif.gz"
                pdbList.append("%s %s %s" % (pdbId, pdbPath, contentType))
        else:
            # 'incremental' for weekly
            logger.info("running incremental workflow")
            for pdbId, cifTimestamp in pdbIdsTimestamps.items():
                zipCifPath = pdbId[1:3] + "/" + pdbId + ".cif.gz"
                bcifPath = pdbId[1:3] + "/" + pdbId + ".bcif"
                zipBcifPath = pdbId[1:3] + "/" + pdbId + ".bcif.gz"
                # /mnt/models/update-store/pdb/1o08/1o08.bcif.gz
                # changed to
                # /mnt/vdb1/out/pdb/1o08/1o08.bcif.gz
                bcifFile = os.path.join(baseDir, bcifPath)
                zipBcifFile = os.path.join(baseDir, zipBcifPath)
                # test pre-existence and modification time
                # allow option to output either .bcif or .bcif.gz files (determined by default at time of file write)
                # return .cif.gz file paths for download rather than bcif output file
                if os.path.exists(bcifFile):
                    t1 = os.path.getmtime(bcifFile)
                    t2 = cifTimestamp.timestamp()
                    if t1 < t2:
                        pdbList.append("%s %s %s" % (pdbId, zipCifPath, contentType))
                elif os.path.exists(zipBcifFile):
                    t1 = os.path.getmtime(zipBcifFile)
                    t2 = cifTimestamp.timestamp()
                    if t1 < t2:
                        pdbList.append("%s %s %s" % (pdbId, zipCifPath, contentType))
                else:
                    pdbList.append("%s %s %s" % (pdbId, zipCifPath, contentType))
        return pdbList

    def getAllCurrentPdbIdsWithTimestamps(self) -> Tuple[dict, str]:
        timesDic = {}
        # http://prereleaseftp-east.rcsb.org/pdb, holdings/released_structures_last_modified_dates.json.gz
        allPdbIdsUrl = os.path.join(
            self.prereleaseFtpFileBasePath, self.pdbIdsTimestampFilePath
        )
        mu = MarshalUtil(workPath=self.tempPath)
        data = mu.doImport(allPdbIdsUrl, fmt="json")
        for pdbId in data:
            try:
                datetimeObject = datetime.datetime.strptime(
                    data[pdbId], "%Y-%m-%dT%H:%M:%S%z"
                )
            except ValueError:
                # requires python >= 3.12
                # datetimeObject = datetime.datetime.strptime(data[pdbId], "%Y-%m-%dT%H:%M:%S%:z")
                dat = data[pdbId][0:-6]
                offset = data[pdbId][-6:]
                if offset.find(":") >= 0:
                    offset = offset.replace(":", "")
                dat = "%s%s" % (dat, offset)
                datetimeObject = datetime.datetime.strptime(dat, "%Y-%m-%dT%H:%M:%S%z")
            timesDic[pdbId.lower()] = datetimeObject
        return timesDic, allPdbIdsUrl

    def getCompList(self, loadType) -> list:
        modelIdsMetadata = self.getAllCurrentModelIdsWithMetadata()
        if not modelIdsMetadata:
            return None
        modelList = []
        contentType = ContentTypeEnum.COMPUTATIONAL.value
        # /mnt/models/update-store/csm
        # changed to
        # /mnt/vdb1/out/csm
        baseDir = os.path.join(self.updateBase, self.contentTypeDir[contentType])
        if loadType == "full":
            for modelId, metadata in modelIdsMetadata.items():
                modelPath = metadata["modelPath"]
                modelList.append("%s %s %s" % (modelId, modelPath, contentType))
        else:
            # 'incremental' for weekly
            for modelId, metadata in modelIdsMetadata.items():
                modelPath = metadata["modelPath"]
                bcifModelPath = (
                    metadata["modelPath"]
                    .replace(".cif.gz", ".bcif")
                    .replace(".cif", ".bcif")
                )
                bcifZipPath = (
                    metadata["modelPath"]
                    .replace(".cif.gz", ".bcif.gz")
                    .replace(".cif", ".bcif.gz")
                )
                # /mnt/models/update-store/csm/1o08/1o08.bcif.gz
                # changed to
                # /mnt/vdb1/out/csm/1o08/1o08.bcif.gz
                bcifFile = os.path.join(baseDir, bcifModelPath)
                bcifZipFile = os.path.join(baseDir, bcifZipPath)
                # check pre-existence and modification time
                # enable output of either .bcif or .bcif.gz files (determined by default at time of file write)
                # return cif model path for download rather than output bcif filepath
                if os.path.exists(bcifFile):
                    t1 = os.path.getmtime(bcifFile)
                    t2 = metadata["datetime"].timestamp()
                    if t1 < t2:
                        modelList.append("%s %s %s" % (modelId, modelPath, contentType))
                elif os.path.exists(bcifZipFile):
                    t1 = os.path.getmtime(bcifZipFile)
                    t2 = metadata["datetime"].timestamp()
                    if t1 < t2:
                        modelList.append("%s %s %s" % (modelId, modelPath, contentType))
                else:
                    modelList.append("%s %s %s" % (modelId, modelPath, contentType))
        return modelList

    def getAllCurrentModelIdsWithMetadata(self) -> dict:
        try:
            # http://computed-models-internal-east.rcsb.org/staging, holdings/computed-models-holdings.json.gz
            holdingsFileUrl = os.path.join(
                self.csmFileRepoBasePath, self.csmHoldingsUrl
            )
            dic = {}
            mu = MarshalUtil(workPath=self.tempPath)
            data = mu.doImport(holdingsFileUrl, fmt="json")
            for modelId in data:
                item = data[modelId]
                item["modelPath"] = item[
                    "modelPath"
                ].lower()  # prod route of BinaryCIF wf produces lowercase filenames
                item["datetime"] = datetime.datetime.strptime(
                    item["lastModifiedDate"], "%Y-%m-%dT%H:%M:%S%z"
                )
                dic[modelId.lower()] = item
            return dic
        except Exception as e:
            logger.exception(str(e))
            return None

    # cif file download

    def getDownloadUrl(self, inputFile, contentType) -> str:
        # http://prereleaseftp-%s.rcsb.org/pdb % east
        baseUrl = self.getFileRepoBaseUrlContentTypeAware(contentType)
        if contentType == ContentTypeEnum.EXPERIMENTAL.value:
            cifgzUrl = os.path.join(baseUrl, self.structureFilePath)
        else:
            cifgzUrl = baseUrl + "/"
        cifgzUrl += inputFile
        return cifgzUrl

    def getFileRepoBaseUrlContentTypeAware(
        self, contentType=ContentTypeEnum.EXPERIMENTAL.value
    ) -> str:
        if contentType == ContentTypeEnum.EXPERIMENTAL.value:
            return self.prereleaseFtpFileBasePath
        if contentType == ContentTypeEnum.COMPUTATIONAL.value:
            return self.csmFileRepoBasePath
        raise ValueError(
            "Unsupported value for 'contentType' parameter: '%s'" % contentType
        )
