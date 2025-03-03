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
import glob
import pathlib
import tempfile
import datetime
import logging
from typing import List
import time
import requests
from mmcif.api.DictionaryApi import DictionaryApi
from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter
from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


def statusStart(listFileBase: str, statusStartFile: str) -> bool:
    startFile = os.path.join(listFileBase, statusStartFile)
    dirs = os.path.dirname(startFile)
    if not os.path.exists(dirs):
        os.makedirs(dirs, mode=0o777)
    with open(startFile, "w", encoding="utf-8") as w:
        w.write("Binary cif run started at %s." % str(datetime.datetime.now()))
    return True


def makeDirs(
    listFileBase: str, updateBase: str, tempPath: str, outputContentType: bool
) -> bool:
    try:
        if not os.path.exists(listFileBase):
            os.mkdir(listFileBase)
            os.chmod(listFileBase, 0o777)
        if not os.path.exists(updateBase):
            os.mkdir(updateBase)
            os.chmod(updateBase, 0o777)
        if not os.path.exists(tempPath):
            os.mkdir(tempPath)
            os.chmod(tempPath, 0o777)
        if outputContentType:
            for contentType in ["pdb", "csm"]:
                path = os.path.join(updateBase, contentType)
                if not os.path.exists(path):
                    os.mkdir(path)
                    os.chmod(path, 0o777)
    except Exception as e:
        logger.error(str(e))
        return False
    return True


def splitRemoteTaskLists(
    pdbHoldingsFilePath: str,
    csmHoldingsFilePath: str,
    loadFileListDir: str,
    targetFileDir: str,
    incrementalUpdate: bool,
    outfileSuffix: str,
    numSublistFiles: int,
    configPath: str,
    outputContentType: bool,
    outputHash: bool,
) -> bool:
    holdingsFilePath = pdbHoldingsFilePath
    databaseName = "pdbx_core"
    result1 = splitRemoteTaskList(
        loadFileListDir,
        holdingsFilePath,
        targetFileDir,
        databaseName,
        incrementalUpdate,
        outfileSuffix,
        numSublistFiles,
        configPath,
        outputContentType,
        outputHash,
    )
    holdingsFilePath = csmHoldingsFilePath
    databaseName = "pdbx_comp_model_core"
    result2 = splitRemoteTaskList(
        loadFileListDir,
        holdingsFilePath,
        targetFileDir,
        databaseName,
        incrementalUpdate,
        outfileSuffix,
        numSublistFiles,
        configPath,
        outputContentType,
        outputHash,
    )
    if not result1:
        logger.error("exp list failed to load")
    if not result2:
        logger.error("comp list failed to load")
    # enable skipping one or the other
    if result1 or result2:
        return True
    return False


def splitRemoteTaskList(
    loadFileListDir: str,
    holdingsFilePath: str,
    targetFileDir: str,
    databaseName: str,
    incrementalUpdate: bool,
    outfileSuffix: str,
    numSublistFiles: int,
    configPath: str,
    outputContentType: bool,
    outputHash: bool,
) -> bool:
    op = "pdbx_id_list_splitter"
    loadFileListPrefix = databaseName + "_ids"
    if numSublistFiles == 0:
        numSublistFiles = multiprocessing.cpu_count()
    incremental = ""
    if incrementalUpdate:
        incremental = "--incremental_update"
    cmd = f"python3 -m rcsb.db.cli.RepoLoadExec --op {op} --database {databaseName} --load_file_list_dir {loadFileListDir} --holdings_file_path {holdingsFilePath} --num_sublists {numSublistFiles} {incremental} --target_file_dir {targetFileDir} --target_file_suffix {outfileSuffix} --config_path {configPath}"
    status = os.system(cmd)
    if status == 0:
        return True
    return False


def makeTaskListFromLocal(
    localDataPath: str, outputContentType: bool, outputHash: bool
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
    filepath: str,
    prereleaseFtpFileBasePath: str,
    csmFileRepoBasePath: str,
    structureFilePath: str,
    listFileBase: str,
    tempPath: str,
    updateBase: str,
    outfileSuffix: str,
    localInputsOrRemote: str,
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
    files = []
    if not os.path.exists(filepath):
        raise FileNotFoundError("no input files")
    for line in open(filepath, "r", encoding="utf-8"):
        files.append(line.strip())
        if 0 < maxFiles <= len(files):
            break
    if len(files) < 1:
        logger.error("error - no files")
        return False

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
                localInputsOrRemote,
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
                localInputsOrRemote,
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
    outfileSuffix,
    tempPath,
    dictionaryApi,
    contentType,
    outputContentType,
    outputHash,
    dtemp,
    maxTempFiles,
):
    for task in tasks:
        singleTask(
            task,
            localInputsOrRemote,
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
    localInputsOrRemote,
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
    if localInputsOrRemote == "local":
        return
    else:
        # list files have upper case for all model types
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
        if localInputsOrRemote == "remote":
            os.unlink(cifFilePath)
        # remove temp files
        if counter[0] >= maxTempFiles:
            removeTempFiles(tempPath=dtemp, listFileBase=None)
            counter[0] = 0


def validateOutput(
    *,
    listFileBase: str,
    updateBase: str,
    outfileSuffix: str,
    missingFileBase: str,
    missingFileName: str,
    maxFiles: int,
    outputContentType: bool,
    outputHash: bool,
) -> bool:
    missing = []
    for path in glob.glob(os.path.join(listFileBase, "*core_ids*.txt")):
        count = 0
        for line in open(path, "r", encoding="utf-8"):
            count += 1
            if count > maxFiles:
                break
            pdbId = line.strip()
            # list files have upper case for all model types
            # experimental models stored with lower case file name and hash
            if path.find("comp_model") < 0:
                pdbId = line.strip().lower()
            contentType = "pdb"
            dividedPath = pdbId[-3:-1]
            # csms stored with upper case file name and hash
            if path.find("comp_model") >= 0:
                contentType = "csm"
                dividedPath = os.path.join(pdbId[0:2], pdbId[-6:-4], pdbId[-4:-2])
            if outputContentType and outputHash:
                out = os.path.join(
                    updateBase,
                    contentType,
                    dividedPath,
                    "%s%s" % (pdbId, outfileSuffix),
                )
            elif outputContentType:
                out = os.path.join(
                    updateBase, contentType, "%s%s" % (pdbId, outfileSuffix)
                )
            elif outputContentType and outputHash:
                out = os.path.join(
                    updateBase, dividedPath, "%s%s" % (pdbId, outfileSuffix)
                )
            else:
                out = os.path.join(updateBase, "%s%s" % (pdbId, outfileSuffix))
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
    outputContentType: bool,
    outputHash: bool,
) -> bool:
    removed = []
    t = time.time()
    infiles = []
    for filepath in glob.glob(os.path.join(listFileBase, "*core_ids*.txt")):
        """uncomment to test
        if filepath.find("comp_model") >= 0:
            os.unlink(filepath)
            continue
        """
        with open(filepath, "r", encoding="utf-8") as r:
            infiles.extend(r.read().split("\n"))
    infiles = [file for file in infiles if file != ""]
    infiles = set(infiles)
    outfiles = {
        os.path.basename(path)
        .replace(".bcif.gz", "")
        .replace(".bcif", "")
        .upper(): str(path)
        for path in pathlib.Path(updateBase).rglob("*.bcif*")
    }
    outcodes = set(outfiles.keys())
    obsoleted = outcodes.difference(infiles)
    removed = []
    filepaths = [outfiles[key] for key in obsoleted if key in outfiles]
    for filepath in filepaths:
        try:
            if filepath.find(updateBase) >= 0:
                os.unlink(filepath)
                removed.append(filepath)
        except Exception as e:
            logger.error(str(e))
    if len(removed) > 0:
        removedFile = os.path.join(missingFileBase, removedFileName)
        with open(removedFile, "w", encoding="utf-8") as w:
            for line in removed:
                w.write(line)
                w.write("\n")
    logger.info("removed retracted entries in %.2f s", time.time() - t)
    return True


def removeTempFiles(tempPath: str, listFileBase: str) -> bool:
    try:
        # periodically
        if tempPath and os.path.exists(tempPath) and os.path.isdir(tempPath):
            for filename in os.listdir(tempPath):
                path = os.path.join(tempPath, filename)
                if os.path.isfile(path):
                    os.unlink(path)
        # once at application close
        if (
            listFileBase
            and os.path.exists(listFileBase)
            and os.path.isdir(listFileBase)
        ):
            for filename in os.listdir(listFileBase):
                path = os.path.join(listFileBase, filename)
                if os.path.isfile(path):
                    os.unlink(path)
            for path in glob.glob("/tmp/config-util*-cache"):
                try:
                    shutil.rmtree(path)
                except Exception as e:
                    logger.error(str(e))
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
