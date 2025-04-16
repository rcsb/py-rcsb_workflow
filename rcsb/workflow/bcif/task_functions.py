##
# File:    task_functions.py
# Author:  James Smith
# Date:    21-Feb-2025
##

"""
Workflow task descriptors.
"""

__docformat__ = "google en"
__author__ = "James Smith"
__email__ = "james.smith@rcsb.org"
__license__ = "Apache 2.0"

import multiprocessing
import os
import shutil
import tempfile
import logging
from enum import Enum
from typing import Optional
from mmcif.api.DictionaryApi import DictionaryApi
from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter
from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


ContentTypeEnum = Enum(
    "ContentTypeEnum",
    [("EXPERIMENTAL", "pdb"), ("COMPUTATIONAL", "csm"), ("INTEGRATIVE", "ihm")],
)


def convertCifFilesToBcif(
    listFileName: str,
    listFileBase: str,
    remotePath: str,
    updateBase: str,
    outfileSuffix: str,
    contentType: str,
    outputContentType: bool,
    outputHash: bool,
    inputHash: bool,
    batchSize: int,
    maxFiles: int,
    pdbxDict: str,
    maDict: str,
    rcsbDict: str,
    ihmDict: str,
):
    """
    Converts CIF files to BCIF format based on a given list file.

    Raises:
        FileNotFoundError: If the specified list file does not exist.
        ValueError: If the list file is empty.
        RuntimeError: If processing fails for some files.
    """

    # read sublist
    listfilepath = os.path.join(listFileBase, listFileName)
    logger.info(
        "reading list file %s and remote path %s",
        listfilepath,
        remotePath,
    )
    files = []
    if not os.path.exists(listfilepath):
        raise FileNotFoundError("No list file found at %s" % listfilepath)
    f = open(listfilepath, "r", encoding="utf-8")
    for line in f:
        files.append(line.strip())
        if 0 < maxFiles <= len(files):
            break
    f.close()
    if len(files) < 1:
        raise ValueError("No files found in %s" % listfilepath)

    # determine batch size
    if (batchSize is None) or not str(batchSize).isdigit():
        batchSize = 1
    batchSize = int(batchSize)
    if batchSize <= 0:
        batchSize = 1
    logger.info("distributing %d files across %d sublists", len(files), batchSize)

    # form dictionary object
    dictionaryApi = getDictionaryApi(pdbxDict, maDict, rcsbDict, ihmDict)

    # traverse sublist and send each input file to converter
    temppath = tempfile.mkdtemp()
    if batchSize == 1:
        # process one file at a time
        for line in files:
            args = (
                line,
                remotePath,
                updateBase,
                outfileSuffix,
                outputContentType,
                outputHash,
                inputHash,
                contentType,
                dictionaryApi,
                temppath,
            )
            singleTask(*args)
    elif batchSize > 1:
        pool = multiprocessing.Pool(processes=batchSize)
        results = []
        for line in files:
            entry = line.strip()
            args = (
                entry,
                remotePath,
                updateBase,
                outfileSuffix,
                outputContentType,
                outputHash,
                inputHash,
                contentType,
                dictionaryApi,
                temppath,
            )
            results.append(pool.apply_async(singleTask, args))
        for r in results:
            r.get(timeout=60 * 5)
        pool.close()
        results.clear()

    try:
        if os.path.exists(temppath):
            shutil.rmtree(temppath)
    except Exception as e:
        logger.error(str(e))

    logger.info("Done with CIF to BCIF conversion")


def getDictionaryApi(
    pdbxDict: str, maDict: str, rcsbDict: str, ihmDict: str
) -> DictionaryApi:
    paths = [pdbxDict, maDict, rcsbDict, ihmDict]
    try:
        adapter = IoAdapter(raiseExceptions=True)
        containers = []
        for path in paths:
            containers += adapter.readFile(inputFilePath=path)
        dictionaryApi = DictionaryApi(containerList=containers, consolidate=True)
    except Exception as e:
        raise FileNotFoundError("failed to create dictionary api: %s" % str(e)) from e
    return dictionaryApi


def singleTask(
    pdbId: str,
    remotePath: str,
    updateBase: str,
    outfileSuffix: str,
    outputContentType: bool,
    outputHash: bool,
    inputHash: bool,
    contentType: str,
    dictionaryApi: DictionaryApi,
    temppath: str,
) -> None:
    if contentType in [
        ContentTypeEnum.EXPERIMENTAL.value,
        ContentTypeEnum.INTEGRATIVE.value,
    ]:
        pdbId = pdbId.lower()
    elif contentType == ContentTypeEnum.COMPUTATIONAL.value:
        pdbId = pdbId.upper()
    remoteFileName = "%s%s" % (
        pdbId,
        outfileSuffix.replace(".bcif.gz", ".cif.gz").replace(".bcif", ".cif"),
    )

    # form input cifFilePath
    if not remotePath.startswith("http"):
        # local file
        cifFilePath = os.path.join(remotePath, remoteFileName)
        if inputHash:
            cifFilePath = os.path.join(
                remotePath, getHash(pdbId, contentType), remoteFileName
            )
        if not os.path.exists(cifFilePath):
            logger.warning("%s not found", cifFilePath)
            return
    else:
        cifFilePath = getRemoteFilePath(pdbId, contentType, remotePath, remoteFileName)

    # form output bcifFilePath
    bcifFilePath = getBcifFilePath(
        pdbId, outfileSuffix, updateBase, contentType, outputContentType, outputHash
    )
    if not bcifFilePath:
        raise ValueError("could not form bcif file path for %s" % pdbId)

    if os.path.exists(bcifFilePath):
        # earlier timestamp ... overwrite
        os.unlink(bcifFilePath)
        if os.path.exists(bcifFilePath):
            raise PermissionError("file %s not removed" % bcifFilePath)

    # make nested directories
    dirs = os.path.dirname(bcifFilePath)
    if not os.path.exists(dirs):
        os.makedirs(dirs)

    # convert to bcif
    result = convert(cifFilePath, bcifFilePath, temppath, dictionaryApi)
    if not result:
        raise Exception("failed to convert %s" % cifFilePath)


def getHash(pdbId: str, contentType: str) -> str:
    if contentType == ContentTypeEnum.COMPUTATIONAL.value:
        pdbId = pdbId.upper()
    else:
        pdbId = pdbId.lower()
    result = pdbId[1:3]
    if contentType == ContentTypeEnum.COMPUTATIONAL.value:
        result = os.path.join(pdbId[0:2], pdbId[-6:-4], pdbId[-4:-2])
    return result


def getRemoteFilePath(
    pdbId: str, contentType: str, remotePath: str, remoteFileName: str
) -> str:
    result = os.path.join(remotePath, getHash(pdbId, contentType), remoteFileName)
    if contentType == ContentTypeEnum.INTEGRATIVE.value:
        result = os.path.join(
            remotePath, getHash(pdbId, contentType), pdbId, "structures", remoteFileName
        )
    return result


def getBcifFilePath(
    pdbId: str,
    outfileSuffix: str,
    updateBase: str,
    contentType: str,
    outputContentType: bool,
    outputHash: bool,
) -> Optional[str]:
    bcifFileName = "%s%s" % (pdbId, outfileSuffix)
    bcifFilePath = None
    if contentType in [
        ContentTypeEnum.EXPERIMENTAL.value,
        ContentTypeEnum.INTEGRATIVE.value,
    ]:
        if outputContentType and outputHash:
            bcifFilePath = os.path.join(
                updateBase, contentType, getHash(pdbId, contentType), bcifFileName
            )
        elif outputContentType:
            bcifFilePath = os.path.join(updateBase, contentType, bcifFileName)
        elif outputHash:
            bcifFilePath = os.path.join(
                updateBase, getHash(pdbId, contentType), bcifFileName
            )
        else:
            bcifFilePath = os.path.join(updateBase, bcifFileName)
    elif contentType == ContentTypeEnum.COMPUTATIONAL.value:
        if outputContentType and outputHash:
            bcifFilePath = os.path.join(
                updateBase,
                contentType,
                getHash(pdbId, contentType),
                bcifFileName,
            )
        elif outputContentType:
            bcifFilePath = os.path.join(
                updateBase,
                contentType,
                bcifFileName,
            )
        elif outputHash:
            bcifFilePath = os.path.join(
                updateBase,
                getHash(pdbId, contentType),
                bcifFileName,
            )
        else:
            bcifFilePath = os.path.join(
                updateBase,
                bcifFileName,
            )
    return bcifFilePath


def convert(
    infile: str, outfile: str, workpath: str, dictionaryApi: DictionaryApi
) -> bool:
    mu = MarshalUtil(workPath=workpath)
    data = mu.doImport(infile, fmt="mmcif")
    result = mu.doExport(outfile, data, fmt="bcif", dictionaryApi=dictionaryApi)
    if not result:
        return False
    return True


def deconvert(
    infile: str, outfile: str, workpath: str, dictionaryApi: DictionaryApi
) -> bool:
    mu = MarshalUtil(workPath=workpath)
    data = mu.doImport(infile, fmt="bcif")
    result = mu.doExport(outfile, data, fmt="mmcif", dictionaryApi=dictionaryApi)
    if not result:
        return False
    return True
