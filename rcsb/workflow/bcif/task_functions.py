##
# File:    task_functions.py
# Author:  James Smith
# Date:    21-Feb-2025
##

"""
Workflow task descriptors that the airflow scheduler will invoke from the command line interface.
"""

__docformat__ = "google en"
__author__ = "James Smith"
__email__ = "james.smith@rcsb.org"
__license__ = "Apache 2.0"

import multiprocessing
import os
import shutil
import glob
import pathlib
import datetime
import logging
from typing import List
import requests
from mmcif.api.DictionaryApi import DictionaryApi
from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.db.wf.RepoLoadWorkflow import RepoLoadWorkflow

logger = logging.getLogger(__name__)


def statusStart(listFileBase: str, statusStartFile: str) -> bool:
    startFile = os.path.join(listFileBase, statusStartFile)
    dirs = os.path.dirname(startFile)
    if not os.path.exists(dirs):
        os.makedirs(dirs, mode=0o777)
    with open(startFile, "w", encoding="utf-8") as w:
        w.write("Binary cif run started at %s." % str(datetime.datetime.now()))
    return True


def makeDirs(updateBase: str) -> bool:
    """mounted paths must be already made"""
    if not os.path.exists(updateBase):
        os.makedirs(updateBase, mode=0o777)
    for contentType in ["pdb", "csm"]:
        path = os.path.join(updateBase, contentType)
        if not os.path.exists(path):
            os.mkdir(path, mode=0o777)
    return True


def splitRemoteTaskLists(
    pdbHoldingsFilePath: str,
    csmHoldingsFilePath: str,
    loadFileListDir: str,
    tempFilePath: str,
    targetFileDir: str,
    incrementalUpdate: bool,
    compress: bool,
    numSublistFiles: int,
) -> bool:
    holdingsFilePath = pdbHoldingsFilePath
    databaseName = "pdbx_core"
    result1 = splitRemoteTaskList(
        loadFileListDir,
        tempFilePath,
        holdingsFilePath,
        targetFileDir,
        databaseName,
        incrementalUpdate,
        compress,
        numSublistFiles,
    )
    holdingsFilePath = csmHoldingsFilePath
    databaseName = "pdbx_comp_model_core"
    # result2 = splitRemoteTaskList(loadFileListDir, tempFilePath, holdingsFilePath, targetFileDir, databaseName, incrementalUpdate, compress, numSublistFiles)
    result2 = splitCompList(
        holdingsFilePath,
        targetFileDir,
        tempFilePath,
        loadFileListDir,
        "incremental",
        numSublistFiles,
    )
    if result1 and result2:
        return True
    return False


def splitRemoteTaskList(
    loadFileListDir: str,
    tempFilePath: str,
    holdingsFilePath: str,
    targetFileDir: str,
    databaseName: str,
    incrementalUpdate: bool,
    compress: bool,
    numSublistFiles: int,
) -> bool:
    rlw = RepoLoadWorkflow(cachePath=tempFilePath)
    op = "pdbx_id_list_splitter"
    loadFileListPrefix = databaseName + "_ids"
    if numSublistFiles == 0:
        numSublistFiles = multiprocessing.cpu_count()
    targetFileSuffix = ".bcif"
    if compress:
        targetFileSuffix += ".gz"
    kwargs = {
        "databaseName": databaseName,
        "holdingsFilePath": holdingsFilePath,
        "loadFileListDir": loadFileListDir,
        "loadFileListPrefix": loadFileListPrefix,
        "numSublistFiles": numSublistFiles,
        "incrementalUpdate": incrementalUpdate,
        "targetFileDir": targetFileDir,
        "targetFileSuffix": targetFileSuffix,
    }
    result = rlw.splitIdList(op, **kwargs)
    return result


def splitCompList(
    holdingsFilePath, updateBase, tempFilePath, loadFileListDir, loadType, subtasks
) -> list:
    modelIds = {}
    try:
        mu = MarshalUtil(workPath=tempFilePath)
        data = mu.doImport(holdingsFilePath, fmt="json")
        for modelId in data:
            item = data[modelId]
            item["modelPath"] = item["modelPath"]
            item["datetime"] = datetime.datetime.strptime(
                item["lastModifiedDate"], "%Y-%m-%dT%H:%M:%S%z"
            )
            modelIds[modelId] = item
    except Exception as e:
        logger.exception(str(e))
        return False
    if not modelIds:
        return False
    modelList = []
    contentType = "csm"
    baseDir = os.path.join(updateBase, contentType)
    if loadType == "full":
        for modelId, data in modelIds.items():
            modelPath = data["modelPath"]
            modelList.append("%s" % modelId)
    else:
        for modelId, data in modelIds.items():
            modelPath = data["modelPath"]
            bcifModelPath = modelPath.replace(".cif.gz", ".bcif").replace(
                ".cif", ".bcif"
            )
            bcifZipPath = modelPath.replace(".cif.gz", ".bcif.gz").replace(
                ".cif", ".bcif.gz"
            )
            bcifFile = os.path.join(baseDir, bcifModelPath)
            bcifZipFile = os.path.join(baseDir, bcifZipPath)
            # check pre-existence and modification time
            # enable output of either .bcif or .bcif.gz files (determined by default at time of file write)
            # return cif model path for download rather than output bcif filepath
            if os.path.exists(bcifFile):
                t1 = os.path.getmtime(bcifFile)
                t2 = data["datetime"].timestamp()
                if t1 < t2:
                    modelList.append("%s" % modelId)
            elif os.path.exists(bcifZipFile):
                t1 = os.path.getmtime(bcifZipFile)
                t2 = data["datetime"].timestamp()
                if t1 < t2:
                    modelList.append("%s" % modelId)
            else:
                modelList.append("%s" % modelId)
    # split into multiple out files
    if subtasks == 0:
        subtasks = multiprocessing.cpu_count()
    nfiles = len(modelList)
    sublists = splitList(nfiles, subtasks, modelList)
    for index in range(0, len(sublists)):
        sublist = sublists[index]
        outfile = "pdbx_comp_model_core_ids-%d.txt" % (index + 1)
        with open(os.path.join(loadFileListDir, outfile), "w", encoding="utf-8") as w:
            for model in sublist:
                w.write(model)
                w.write("\n")
    return True


def makeTaskListFromLocal(
    localDataPath: str,
) -> bool:
    """
    requires cif files in source folder with no subdirs
    writes to target folder with no subdirs
    """
    # traverse local folder
    """
    tasklist = glob.glob(os.path.join(localDataPath, "*.cif.gz"))
    nfiles = len(tasklist)
    if nfiles == 0:
        tasklist = glob.glob(os.path.join(localDataPath, "*.cif"))
        nfiles = len(tasklist)
    logger.info("found %d cif files", nfiles)
    """
    return True


def localTaskMap(
    index: int,
    prereleaseFtpFileBasePath: str,
    csmFileRepoBasePath: str,
    structureFilePath: str,
    listFileBase: str,
    tempPath: str,
    updateBase: str,
    compress: bool,
    localInputsOrRemote: str,
    batch: int,
    maxFiles: int,
    pdbxDict: str,
    maDict: str,
    rcsbDict: str,
) -> bool:
    # read sublist
    expfilename = "pdbx_core_ids-%d.txt" % (index + 1)
    expfilepath = os.path.join(listFileBase, expfilename)
    compfilename = "pdbx_comp_model_core_ids-%d.txt" % (index + 1)
    compfilepath = os.path.join(listFileBase, compfilename)
    expfiles = []
    compfiles = []
    if not os.path.exists(expfilepath) and not os.path.exists(compfilepath):
        raise FileNotFoundError("no input files")
    if os.path.exists(expfilepath):
        for line in open(expfilepath, "r", encoding="utf-8"):
            expfiles.append(line.strip())
            if 0 < maxFiles <= len(expfiles):
                break
        if len(expfiles) < 1:
            logger.error("error - no exp files")
            return False
        logger.info("task map has %d exp files", len(expfiles))
    if os.path.exists(compfilepath):
        for line in open(compfilepath, "r", encoding="utf-8"):
            compfiles.append(line.strip())
            if 0 < maxFiles <= len(compfiles):
                break
        if len(compfiles) < 1:
            logger.error("error - no comp files")
            return False
        logger.info("task map has %d comp files", len(compfiles))

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
    logger.info("distributing %d exp files across %d sublists", len(expfiles), batch)
    logger.info("distributing %d comp files across %d sublists", len(compfiles), batch)
    procs = []
    if batch == 1:
        # process one file at a time
        for files, contentType, remotePath in zip(
            [expfiles, compfiles],
            ["pdb", "csm"],
            [prereleaseFtpFileBasePath, csmFileRepoBasePath],
        ):
            if len(files) == 0:
                continue
            for line in files:
                args = (
                    line,
                    localInputsOrRemote,
                    remotePath,
                    structureFilePath,
                    updateBase,
                    compress,
                    tempPath,
                    dictionaryApi,
                    contentType,
                )
                singleTask(*args)
    else:
        # process with file batching
        for files, contentType, remotePath in zip(
            [expfiles, compfiles],
            ["pdb", "csm"],
            [prereleaseFtpFileBasePath, csmFileRepoBasePath],
        ):
            nfiles = len(files)
            if nfiles == 0:
                continue
            tasks = splitList(nfiles, batch, files)
            for task in tasks:
                args = (
                    task,
                    localInputsOrRemote,
                    remotePath,
                    structureFilePath,
                    updateBase,
                    compress,
                    tempPath,
                    dictionaryApi,
                    contentType,
                )
                procs.append(multiprocessing.Process(target=batchTask, args=args))
            for p in procs:
                p.start()
            for p in procs:
                p.join()
            procs.clear()

    return True


def splitList(nfiles: int, subtasks: int, tasklist: List[str]) -> List[List[str]]:
    step = nfiles // subtasks
    if step < 1:
        step = 1
    steps = nfiles // step
    logger.info(
        "split list has %d files and %d steps with step %d", nfiles, steps, step
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
    localInputsOrRemote,
    remotePath,
    structureFilePath,
    updateBase,
    compress,
    tempPath,
    dictionaryApi,
    contentType,
):
    for task in tasks:
        singleTask(
            task,
            localInputsOrRemote,
            remotePath,
            structureFilePath,
            updateBase,
            compress,
            tempPath,
            dictionaryApi,
            contentType,
        )


def singleTask(
    pdbId,
    localInputsOrRemote,
    remotePath,
    structureFilePath,
    updateBase,
    compress,
    tempPath,
    dictionaryApi,
    contentType,
):
    """
    download to cifFilePath
    form output path bcifFilePath
    """
    if localInputsOrRemote == "local":
        pass
    else:
        if contentType == "pdb":
            pdbId = pdbId.lower()
            remoteFileName = "%s.cif.gz" % pdbId
            url = os.path.join(
                remotePath, structureFilePath, pdbId[1:3], remoteFileName
            )
            cifFilePath = os.path.join(tempPath, remoteFileName)
            bcifFileName = "%s.bcif" % pdbId
            if compress:
                bcifFileName += ".gz"
            bcifFilePath = os.path.join(
                updateBase, contentType, pdbId[1:3], bcifFileName
            )
        elif contentType == "csm":
            remoteFileName = "%s.cif.gz" % pdbId
            url = os.path.join(
                remotePath, pdbId[0:2], pdbId[-6:-4], pdbId[-4:-2], remoteFileName
            )
            cifFilePath = os.path.join(tempPath, remoteFileName)
            bcifFileName = "%s.bcif" % pdbId
            if compress:
                bcifFileName += ".gz"
            bcifFilePath = os.path.join(
                updateBase,
                contentType,
                pdbId[0:2],
                pdbId[-6:-4],
                pdbId[-4:-2],
                bcifFileName,
            )
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
    if os.path.exists(bcifFilePath):
        if localInputsOrRemote == "local":
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
        result = convert(cifFilePath, bcifFilePath, tempPath, dictionaryApi)
        if not result:
            raise Exception("failed to convert %s" % cifFilePath)
        shutil.chown(bcifFilePath, "root", "root")
        os.chmod(bcifFilePath, 0o777)
    except Exception as e:
        logger.exception(str(e))
    # remove input file
    finally:
        if localInputsOrRemote == "remote":
            os.unlink(cifFilePath)


def validateOutput(
    *,
    listFileBase: str,
    updateBase: str,
    compress: bool,
    missingFileBase: str,
    missingFileName: str,
    maxFiles: int,
) -> bool:
    missing = []
    for path in glob.glob(os.path.join(listFileBase, "*core_ids*.txt")):
        count = 0
        for line in open(path, "r", encoding="utf-8"):
            count += 1
            if count > maxFiles:
                break
            pdbId = line.strip().lower()
            contentType = "pdb"
            dividedPath = pdbId[1:3]
            if path.find("comp_model") >= 0:
                contentType = "csm"
                dividedPath = os.path.join(pdbId[0:2], pdbId[-6:-4], pdbId[-4:-2])
            out = os.path.join(updateBase, contentType, dividedPath, "%s.bcif" % pdbId)
            if compress:
                out = "%s.gz" % out
            if not os.path.exists(out):
                missing.append(out)
    if len(missing) > 0:
        missingFile = os.path.join(missingFileBase, missingFileName)
        with open(missingFile, "w", encoding="utf-8") as w:
            for line in missing:
                w.write(line)
                w.write("\n")
    return True


def removeRetractedEntries(
    *,
    listFileBase: str,
    updateBase: str,
    missingFileBase: str,
    removedFileName: str,
) -> bool:
    removed = []
    for outpath in pathlib.Path(updateBase).rglob("*.bcif*"):
        pdbId = (
            os.path.basename(str(outpath)).replace(".bcif.gz", "").replace(".bcif", "")
        )
        found = False
        for path in glob.glob(os.path.join(listFileBase, "*core_ids*.txt")):
            for line in open(path, "r", encoding="utf-8"):
                if line.strip() == pdbId or line.strip().lower() == pdbId:
                    found = True
                    break
            if found:
                break
        if not found:
            # obsoleted
            try:
                removed.append(str(outpath))
                os.unlink(outpath)
            except Exception as e:
                logger.exception("could not remove obsoleted file %s", outpath)
    if len(removed) > 0:
        removedFile = os.path.join(missingFileBase, removedFileName)
        with open(removedFile, "w", encoding="utf-8") as w:
            for line in removed:
                w.write(line)
                w.write("\n")
    return True


def removeTempFiles(tempPath: str, listFileBase: str) -> bool:
    if not os.path.exists(tempPath):
        return False
    try:
        for filename in os.listdir(tempPath):
            path = os.path.join(tempPath, filename)
            if os.path.isfile(path):
                os.unlink(path)
        for filename in os.listdir(listFileBase):
            path = os.path.join(listFileBase, filename)
            if os.path.isfile(path):
                os.unlink(path)
    except Exception as e:
        logger.warning(str(e))
    return True


def tasksDone() -> bool:
    logger.info("task maps completed")
    return True


def k8sBranch() -> bool:
    logger.info("using k8s tasks")
    return True


def statusComplete(listFileBase: str, statusCompleteFile: str) -> bool:
    """
    must occur after end_task
    """
    completeFile = os.path.join(listFileBase, statusCompleteFile)
    dirs = os.path.dirname(completeFile)
    if not os.path.exists(dirs):
        os.makedirs(dirs, mode=0o777)
    with open(completeFile, "w", encoding="utf-8") as w:
        w.write(
            "Binary cif run completed successfully at %s."
            % str(datetime.datetime.now())
        )
    return True


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
        logger.exception("error during bcif conversion: %s", str(e))
        return False
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
        logger.exception("error during bcif conversion: %s", str(e))
        return False
    return True
