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
from itertools import chain
import logging
from typing import List
import requests
from mmcif.api.DictionaryApi import DictionaryApi
from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter
from rcsb.utils.io.MarshalUtil import MarshalUtil

# pylint:disable=W0102

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    fmt="%(asctime)s @%(process)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


def convertPrereleaseCifFiles(
    listFileName: str,
    listFileBase: str,
    remotePath: str,
    updateBase: str,
    outfileSuffix: str,
    outputContentType: bool,
    outputHash: bool,
    batch: int,
    maxFiles: int,
    maxTempFiles: int,
    pdbxDict: str,
    maDict: str,
    rcsbDict: str,
) -> bool:
    """runs once per list file"""

    # path for cif file downloads with known file names (removed on conversion)
    tempPath = tempfile.mkdtemp()
    # paths for randomly-named temp files (bulk removal periodically)
    dtemps = []

    # read sublist
    filepath = os.path.join(listFileBase, listFileName)
    logger.info(
        "convert prerelease cif files reading list file %s and remote path %s",
        filepath,
        remotePath,
    )
    files = []
    if not os.path.exists(filepath):
        raise FileNotFoundError("no input files")
    f = open(filepath, "r", encoding="utf-8")
    for line in f:
        files.append(line.strip())
        if 0 < maxFiles <= len(files):
            break
    f.close()
    if len(files) < 1:
        raise ValueError("no files")

    # set content type
    contentType = "pdb"
    if filepath.find("pdbx_comp_model_") >= 0:
        contentType = "csm"

    # determine batch size
    if (batch is None) or not str(batch).isdigit():
        batch = 1
    batch = int(batch)
    if batch <= 0:
        batch = multiprocessing.cpu_count()
    logger.info("distributing %d files across %d sublists", len(files), batch)

    # form dictionary object
    dictionaryApi = None
    paths = [pdbxDict, maDict, rcsbDict]
    try:
        adapter = IoAdapter(raiseExceptions=True)
        containers = []
        for path in paths:
            containers += adapter.readFile(inputFilePath=path)
        dictionaryApi = DictionaryApi(containerList=containers, consolidate=True)
    except Exception as e:
        raise FileNotFoundError("failed to create dictionary api: %s" % str(e)) from e

    # traverse sublist and send each input file to converter
    procs = []
    if batch == 1:
        dtemp = tempfile.mkdtemp(dir=tempPath)
        dtemps.append(dtemp)
        # process one file at a time
        for line in files:
            args = (
                line,
                remotePath,
                updateBase,
                outfileSuffix,
                outputContentType,
                outputHash,
                contentType,
                dictionaryApi,
                tempPath,
                dtemp,
                maxTempFiles,
            )
            singleTask(*args)
    else:
        # process with file batching
        nfiles = len(files)
        tasks = splitList(nfiles, batch, files)
        nresults = len(list(chain(*tasks)))
        if nresults != nfiles:
            logger.warning(
                "split list returned %d files instead of %d", nresults, nfiles
            )
        for task in tasks:
            dtemp = tempfile.mkdtemp(dir=tempPath)
            dtemps.append(dtemp)
            args = (
                task,
                remotePath,
                updateBase,
                outfileSuffix,
                outputContentType,
                outputHash,
                contentType,
                dictionaryApi,
                tempPath,
                dtemp,
                maxTempFiles,
            )
            procs.append(multiprocessing.Process(target=batchTask, args=args))
        for p in procs:
            p.start()
        for p in procs:
            p.join()
        procs.clear()

    try:
        for dtemp in dtemps:
            if os.path.exists(dtemp):
                shutil.rmtree(dtemp)
        shutil.rmtree(tempPath)
    except Exception as e:
        logger.error(str(e))

    return True


def splitList(nfiles: int, subtasks: int, tasklist: List[str]) -> List[List[str]]:
    step = nfiles // subtasks
    if step < 1:
        step = 1
    steps = nfiles // step
    logger.info("splitting %d files into %d steps with step %d", nfiles, steps, step)
    if not isinstance(tasklist[0], str):
        tasklist = [str(task) for task in tasklist]
    tasks = [
        (
            tasklist[index * step : step + index * step]
            if index < steps - 1
            else tasklist[index * step : nfiles]
        )
        for index in range(0, steps)
    ]
    return tasks


def batchTask(
    tasks,
    remotePath,
    updateBase,
    outfileSuffix,
    outputContentType,
    outputHash,
    contentType,
    dictionaryApi,
    tempPath,
    dtemp,
    maxTempFiles,
):
    logger.info("processing %d tasks", len(tasks))
    for task in tasks:
        singleTask(
            task,
            remotePath,
            updateBase,
            outfileSuffix,
            outputContentType,
            outputHash,
            contentType,
            dictionaryApi,
            tempPath,
            dtemp,
            maxTempFiles,
        )


def singleTask(
    pdbId,
    remotePath,
    updateBase,
    outfileSuffix,
    outputContentType,
    outputHash,
    contentType,
    dictionaryApi,
    tempPath,
    dtemp,
    maxTempFiles,
    counter=[0],
):
    if contentType == "pdb":
        pdbId = pdbId.lower()
    elif contentType == "csm":
        pdbId = pdbId.upper()
    remoteFileName = "%s%s" % (
        pdbId,
        outfileSuffix.replace(".bcif.gz", ".cif.gz").replace(".bcif", ".cif"),
    )
    # copy or download to cifFilePath
    if not remotePath.startswith("http"):
        # local file
        localFilePath = os.path.join(remotePath, remoteFileName)
        if not os.path.exists(localFilePath):
            logger.error("%s not found", localFilePath)
            return
        cifFilePath = os.path.join(tempPath, remoteFileName)
        shutil.copy(localFilePath, cifFilePath)
    else:
        url = os.path.join(remotePath, pdbId[-3:-1], remoteFileName)
        cifFilePath = os.path.join(tempPath, remoteFileName)
        if contentType == "csm":
            url = os.path.join(
                remotePath, pdbId[0:2], pdbId[-6:-4], pdbId[-4:-2], remoteFileName
            )
            cifFilePath = os.path.join(tempPath, remoteFileName)
        r = requests.get(url, timeout=(20, 30), stream=True)
        if r and r.status_code == 200:
            dirs = os.path.dirname(cifFilePath)
            if not os.path.exists(dirs):
                os.makedirs(dirs)
            with open(cifFilePath, "ab") as w:
                for chunk in r.raw.stream(1024, decode_content=False):
                    if chunk:
                        w.write(chunk)
        else:
            status_code = 0
            if r:
                status_code = r.status_code
            raise requests.exceptions.RequestException(
                "error - request failed for %s with status code %d" % (url, status_code)
            )

    # form output path bcifFilePath
    bcifFilePath = getBcifFilePath(
        pdbId, outfileSuffix, updateBase, contentType, outputContentType, outputHash
    )
    if not bcifFilePath:
        raise ValueError("failed to form bcif file path")
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
    result = convert(cifFilePath, bcifFilePath, dtemp, dictionaryApi)
    if not result:
        raise Exception("failed to convert %s" % cifFilePath)

    # remove cif file
    if remotePath.startswith("http") and os.path.exists(cifFilePath):
        os.unlink(cifFilePath)

    counter[0] += 1

    # remove temp files
    if counter[0] >= maxTempFiles:
        removeTempFiles(tempPath=dtemp)
        counter[0] = 0


def getBcifFilePath(
    pdbId, outfileSuffix, updateBase, contentType, outputContentType, outputHash
):
    bcifFileName = "%s%s" % (pdbId, outfileSuffix)
    bcifFilePath = None
    if contentType == "pdb":
        if outputContentType and outputHash:
            bcifFilePath = os.path.join(
                updateBase, contentType, pdbId[-3:-1], bcifFileName
            )
        elif outputContentType:
            bcifFilePath = os.path.join(updateBase, contentType, bcifFileName)
        elif outputHash:
            bcifFilePath = os.path.join(updateBase, pdbId[-3:-1], bcifFileName)
        else:
            bcifFilePath = os.path.join(updateBase, bcifFileName)
    elif contentType == "csm":
        if outputContentType and outputHash:
            bcifFilePath = os.path.join(
                updateBase,
                contentType,
                pdbId[0:2],
                pdbId[-6:-4],
                pdbId[-4:-2],
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
                pdbId[0:2],
                pdbId[-6:-4],
                pdbId[-4:-2],
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


def removeTempFiles(tempPath: str):
    try:
        if tempPath and os.path.exists(tempPath) and os.path.isdir(tempPath):
            for filename in os.listdir(tempPath):
                path = os.path.join(tempPath, filename)
                if os.path.isfile(path):
                    os.unlink(path)
    except Exception as e:
        logger.warning(str(e))
