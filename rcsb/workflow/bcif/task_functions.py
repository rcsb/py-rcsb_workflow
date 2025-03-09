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
from typing import List
import requests
from mmcif.api.DictionaryApi import DictionaryApi
from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter
from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter(fmt="%(asctime)s @%(process)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def localTaskMap(
    listFileBase: str,
    listFileName: str,
    prereleaseFtpFileBasePath: str,
    csmFileRepoBasePath: str,
    structureFilePath: str,
    tempPath: str,
    updateBase: str,
    outfileSuffix: str,
    batch: int,
    maxFiles: int,
    pdbxDict: str,
    maDict: str,
    rcsbDict: str,
    outputContentType: bool,
    outputHash: bool,
    maxTempFiles: int,
) -> bool:
    # read sublist
    filepath = os.path.join(listFileBase, listFileName)
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
        logger.exception("failed to create dictionary api: %s", str(e))

    # traverse sublist and send each input file to converter
    if (batch is None) or not str(batch).isdigit():
        batch = 1
    batch = int(batch)
    if batch == 0:
        batch = multiprocessing.cpu_count()
    logger.info("distributing %d files across %d sublists", len(files), batch)

    contentType = "pdb"
    remotePath = prereleaseFtpFileBasePath
    if filepath.find("pdbx_comp_model_") >= 0:
        contentType = "csm"
        remotePath = csmFileRepoBasePath

    dtemp = tempfile.mkdtemp(dir=tempPath)

    procs = []
    if batch == 1:
        # process one file at a time
        for line in files:
            args = (
                line,
                remotePath,
                structureFilePath,
                updateBase,
                outfileSuffix,
                tempPath,
                dictionaryApi,
                contentType,
                outputContentType,
                outputHash,
                dtemp,
                maxTempFiles,
            )
            singleTask(*args)
    else:
        # process with file batching
        nfiles = len(files)
        tasks = splitList(nfiles, batch, files)
        for task in tasks:
            args = (
                task,
                remotePath,
                structureFilePath,
                updateBase,
                outfileSuffix,
                tempPath,
                dictionaryApi,
                contentType,
                outputContentType,
                outputHash,
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
        shutil.rmtree(dtemp)
    except Exception as e:
        logger.error(str(e))

    return True


def splitList(nfiles: int, subtasks: int, tasklist: List[str]) -> List[List[str]]:
    step = nfiles // subtasks
    if step < 1:
        step = 1
    steps = nfiles // step
    logger.info(
        "splitting %d files into %d steps with step %d", nfiles, steps, step
    )
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
    structureFilePath,
    updateBase,
    outfileSuffix,
    tempPath,
    dictionaryApi,
    contentType,
    outputContentType,
    outputHash,
    dtemp,
    maxTempFiles,
):
    logger.info("processing %d tasks", len(tasks))
    for task in tasks:
        singleTask(
            task,
            remotePath,
            structureFilePath,
            updateBase,
            outfileSuffix,
            tempPath,
            dictionaryApi,
            contentType,
            outputContentType,
            outputHash,
            dtemp,
            maxTempFiles,
        )


def singleTask(
    pdbId,
    remotePath,
    structureFilePath,
    updateBase,
    outfileSuffix,
    tempPath,
    dictionaryApi,
    contentType,
    outputContentType,
    outputHash,
    dtemp,
    maxTempFiles,
    counter=[0],
):
    """
    download to cifFilePath
    form output path bcifFilePath
    """
    if not remotePath.startswith("http"):
        # local files not yet implemented
        return
    else:
        # experimental models are stored with lower case file name and hash
        if contentType == "pdb":
            pdbId = pdbId.lower()
            remoteFileName = "%s.cif.gz" % pdbId
            url = os.path.join(
                remotePath, structureFilePath, pdbId[-3:-1], remoteFileName
            )
            cifFilePath = os.path.join(tempPath, remoteFileName)
        # computed structure models are stored with upper case file name and hash
        elif contentType == "csm":
            remoteFileName = "%s.cif.gz" % pdbId
            url = os.path.join(
                remotePath, pdbId[0:2], pdbId[-6:-4], pdbId[-4:-2], remoteFileName
            )
            cifFilePath = os.path.join(tempPath, remoteFileName)
        try:
            r = requests.get(url, timeout=300, stream=True)
            if r and r.status_code < 400:
                dirs = os.path.dirname(cifFilePath)
                if not os.path.exists(dirs):
                    os.makedirs(dirs, mode=0o777)
                    shutil.chown(dirs, "root", "root")
                with open(cifFilePath, "ab") as w:
                    for chunk in r.raw.stream(1024, decode_content=False):
                        if chunk:
                            w.write(chunk)
                shutil.chown(cifFilePath, "root", "root")
                os.chmod(cifFilePath, 0o777)
            else:
                raise requests.exceptions.RequestException(
                    "error - request failed for %s" % url
                )
        except Exception as e:
            logger.exception(str(e))
            if os.path.exists(cifFilePath):
                os.unlink(cifFilePath)
            return
    bcifFileName = "%s%s" % (pdbId, outfileSuffix)
    if contentType == "pdb":
        if outputContentType and outputHash:
            bcifFilePath = os.path.join(
                updateBase, contentType, pdbId[-3:-1], bcifFileName
            )
        elif outputContentType:
            bcifFilePath = os.path.join(updateBase, pdbId[-3:-1], bcifFileName)
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
    if os.path.exists(bcifFilePath):
        if not remotePath.startswith("http"):
            # assume local experiment - make no assumptions about file removal
            return
        # earlier timestamp ... overwrite
        try:
            os.unlink(bcifFilePath)
            if os.path.exists(bcifFilePath):
                raise Exception("file %s not removed" % bcifFilePath)
        except Exception as e:
            logger.exception(str(e))
    # make nested directories
    dirs = os.path.dirname(bcifFilePath)
    if not os.path.exists(dirs):
        os.makedirs(dirs, mode=0o777)
        shutil.chown(dirs, "root", "root")
        os.chmod(dirs, 0o777)
    # convert to bcif
    try:
        result = convert(cifFilePath, bcifFilePath, dtemp, dictionaryApi)
        if not result:
            raise Exception("failed to convert %s" % cifFilePath)
        shutil.chown(bcifFilePath, "root", "root")
        os.chmod(bcifFilePath, 0o777)
        counter[0] += 1
    except Exception as e:
        logger.exception(str(e))
    finally:
        # remove input file
        if not remotePath.startswith("http"):
            os.unlink(cifFilePath)
        # remove temp files
        if counter[0] >= maxTempFiles:
            removeTempFiles(tempPath=dtemp)
            counter[0] = 0


def convert(
    infile: str, outfile: str, workpath: str, dictionaryApi: DictionaryApi
) -> bool:
    mu = MarshalUtil(workPath=workpath)
    data = mu.doImport(infile, fmt="mmcif")
    try:
        result = mu.doExport(outfile, data, fmt="bcif", dictionaryApi=dictionaryApi)
        if not result:
            raise Exception()
    except Exception as e:
        raise Exception("error during bcif conversion: %s", str(e))
    return True


def deconvert(
    infile: str, outfile: str, workpath: str, dictionaryApi: DictionaryApi
) -> bool:
    mu = MarshalUtil(workPath=workpath)
    data = mu.doImport(infile, fmt="bcif")
    try:
        result = mu.doExport(outfile, data, fmt="mmcif", dictionaryApi=dictionaryApi)
        if not result:
            raise Exception()
    except Exception as e:
        raise Exception("error during bcif conversion: %s", str(e))
    return True


def removeTempFiles(tempPath: str) -> bool:
    try:
        if tempPath and os.path.exists(tempPath) and os.path.isdir(tempPath):
            for filename in os.listdir(tempPath):
                path = os.path.join(tempPath, filename)
                if os.path.isfile(path):
                    os.unlink(path)
    except Exception as e:
        logger.warning(str(e))
    return True
